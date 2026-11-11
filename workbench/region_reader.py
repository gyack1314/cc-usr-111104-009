"""Zarr v2 二维区域采集器。

只下载给定半开索引框相交的 chunk，按 dtype 与 C/F 顺序严格解码，
缺失块（HTTP 404）按 fill_value 补，其余任何异常都让本次区域读取失败。
所有请求固定在 manifest 给出的 chunk_prefix 内，未列出的 URL 不会被请求。
"""
from __future__ import annotations

import array
import concurrent.futures
import hashlib
import json
import math
import os
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
import zlib
from dataclasses import dataclass, field
from pathlib import Path

_ALLOWED_TYPECODES = {'i2': 'h', 'u2': 'H', 'f4': 'f'}
_ALLOWED_ORDERS = ('C', 'F')
_ALLOWED_SEPARATORS = ('.', '/')


class RegionError(Exception):
    """区域读取失败；调用方不得把该结果当作空白地图使用。"""


class RegionLimitError(RegionError):
    """超出并发、峰值内存或输出大小上限，分配结果前即拒绝。"""


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Limits:
    """采集器的独立资源上限。"""
    max_concurrency: int = 4
    max_decoded_bytes: int = 64 * 1024 * 1024
    max_output_bytes: int = 64 * 1024 * 1024
    request_timeout: float = 10.0
    max_compressed_bytes: int = 64 * 1024 * 1024

    def __post_init__(self):
        if self.max_concurrency < 1:
            raise ValueError('max_concurrency 必须 >= 1')
        for name in ('max_decoded_bytes', 'max_output_bytes', 'max_compressed_bytes'):
            if getattr(self, name) < 1:
                raise ValueError(f'{name} 必须 >= 1')


@dataclass(frozen=True)
class ArrayMeta:
    """经过校验的 .zarray 元数据。"""
    shape: tuple[int, int]
    chunks: tuple[int, int]
    typecode: str          # array 模块类型码
    dtype_text: str        # 原始 dtype 字符串，如 '<i2'
    itemsize: int
    order: str
    compressor: str        # 'null' 或 'zlib'
    fill_value: float | int
    fill_is_nan: bool
    separator: str
    raw_digest: str

    @property
    def chunk_numel(self) -> int:
        return self.chunks[0] * self.chunks[1]

    @property
    def chunk_bytes(self) -> int:
        return self.chunk_numel * self.itemsize


@dataclass(frozen=True)
class ChunkSource:
    """单个来源块的核对信息。"""
    chunk_coord: tuple[int, int]
    url: str
    status: str            # 'http' | 'fill_404' | 'cache'
    http_status: int | None
    sha256: str | None     # 实际下载字节（压缩后/线上原始字节）摘要
    bytes_downloaded: int
    decoded_sha256: str    # 解码后定长字节摘要
    filled: bool


@dataclass
class RegionResult:
    generation: str
    region: tuple[int, int, int, int]
    shape: tuple[int, int]
    dtype: str
    order: str
    fill_value: float | int
    data: list[list] = field(repr=False)
    sources: list[ChunkSource]
    http_requests: int
    cache_hits: int
    filled_cells: int
    fill_value_cells: int

    def to_report(self) -> dict:
        rows = len(self.data)
        cols = len(self.data[0]) if rows else 0
        flat = [v for row in self.data for v in row]
        finite = [v for v in flat if not (isinstance(v, float) and math.isnan(v))]
        summary = {
            'rows': rows,
            'cols': cols,
            'cells': rows * cols,
            'min': min(finite) if finite else None,
            'max': max(finite) if finite else None,
            'nan_cells': len(flat) - len(finite),
        }
        fill_range = _fill_ranges(self.data, self.fill_value,
                                  any(s.filled for s in self.sources))
        return {
            'generation': self.generation,
            'region': list(self.region),
            'shape': list(self.shape),
            'dtype': self.dtype,
            'order': self.order,
            'fill_value': _json_scalar(self.fill_value),
            'output_summary': summary,
            'http_requests': self.http_requests,
            'cache_hits': self.cache_hits,
            'filled_cells': self.filled_cells,
            'fill_value_cells': self.fill_value_cells,
            'fill_value_ranges': fill_range,
            'chunks': [
                {
                    'chunk_coord': list(s.chunk_coord),
                    'url': s.url,
                    'status': s.status,
                    'http_status': s.http_status,
                    'filled': s.filled,
                    'bytes_downloaded': s.bytes_downloaded,
                    'sha256': s.sha256,
                    'decoded_sha256': s.decoded_sha256,
                }
                for s in self.sources
            ],
        }


def _json_scalar(v):
    if isinstance(v, float) and math.isnan(v):
        return 'NaN'
    return v


def _fill_ranges(data, fill_value, has_filled_chunks):
    """统计输出中等于 fill_value 的连续区间（按行展平的半开闭区间）。"""
    if not has_filled_chunks:
        return []
    flat = [v for row in data for v in row]
    nan_fill = isinstance(fill_value, float) and math.isnan(fill_value)

    def is_fill(v):
        return math.isnan(v) if nan_fill else v == fill_value

    ranges = []
    start = None
    for i, v in enumerate(flat):
        if is_fill(v) and start is None:
            start = i
        elif not is_fill(v) and start is not None:
            ranges.append([start, i])
            start = None
    if start is not None:
        ranges.append([start, len(flat)])
    return ranges


# ---------------------------------------------------------------- 元数据校验

def _parse_fill_value(raw, typecode):
    if typecode == 'f':
        if isinstance(raw, str) and raw == 'NaN':
            return float('nan'), True
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise RegionError('f4 的 fill_value 只支持数字或 "NaN"')
        return float(raw), False
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise RegionError('整数类型的 fill_value 必须是整数')
    return raw, False


def parse_metadata(raw_bytes: bytes, expected_digest: str | None = None) -> ArrayMeta:
    """严格解析 .zarray，任何不符合约定的编码都明确拒绝。"""
    digest = _sha256_hex(raw_bytes)
    if expected_digest is not None and digest != expected_digest:
        raise RegionError('.zarray 摘要与 manifest 记录不一致，拒绝继续')
    try:
        meta = json.loads(raw_bytes.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RegionError(f'.zarray 不是合法 UTF-8 JSON: {exc}') from exc
    if not isinstance(meta, dict):
        raise RegionError('.zarray 顶层必须是对象')

    if meta.get('zarr_format') != 2:
        raise RegionError('只支持 zarr_format=2')

    shape = meta.get('shape')
    chunks = meta.get('chunks')
    if not (isinstance(shape, list) and len(shape) == 2 and
            all(isinstance(v, int) and not isinstance(v, bool) and v > 0 for v in shape)):
        raise RegionError('shape 必须是两个正整数，仅支持二维')
    if not (isinstance(chunks, list) and len(chunks) == 2 and
            all(isinstance(v, int) and not isinstance(v, bool) and v > 0 for v in chunks)):
        raise RegionError('chunks 必须是两个正整数')

    dtype_text = meta.get('dtype')
    if not isinstance(dtype_text, str) or len(dtype_text) != 3:
        raise RegionError('dtype 只接受形如 <i2 / >u2 / <f4 的编码')
    endian, type_name = dtype_text[0], dtype_text[1:]
    if endian not in ('<', '>') or type_name not in _ALLOWED_TYPECODES:
        raise RegionError(f'不支持的 dtype: {dtype_text!r}，仅支持大小端 i2/u2/f4')
    typecode = _ALLOWED_TYPECODES[type_name]
    itemsize = array.array(typecode).itemsize
    if itemsize != {'i2': 2, 'u2': 2, 'f4': 4}[type_name]:
        raise RegionError('当前平台的类型宽度不符合资料约定')

    order = meta.get('order')
    if order not in _ALLOWED_ORDERS:
        raise RegionError(f'order 只接受 C 或 F，收到 {order!r}')

    compressor_raw = meta.get('compressor')
    if compressor_raw is None:
        compressor = 'null'
    elif isinstance(compressor_raw, dict) and compressor_raw.get('id') == 'zlib':
        compressor = 'zlib'
    else:
        raise RegionError(f'不支持的 compressor: {compressor_raw!r}，仅接受 null 或 zlib')

    if meta.get('filters') is not None:
        raise RegionError('filters 只接受 null，其余编码明确拒绝')

    separator = meta.get('dimension_separator', '.')
    if separator not in _ALLOWED_SEPARATORS:
        raise RegionError(f'dimension_separator 只接受 "." 或 "/"，收到 {separator!r}')

    fill_value, fill_is_nan = _parse_fill_value(meta.get('fill_value'), typecode)
    if not fill_is_nan:
        # 提前验证填充值在类型域内，避免缺失块时才失败。
        try:
            array.array(typecode, [fill_value])
        except (TypeError, OverflowError) as exc:
            raise RegionError(f'fill_value 超出 {dtype_text} 可表示范围') from exc

    return ArrayMeta(
        shape=(shape[0], shape[1]),
        chunks=(chunks[0], chunks[1]),
        typecode=typecode,
        dtype_text=dtype_text,
        itemsize=itemsize,
        order=order,
        compressor=compressor,
        fill_value=fill_value,
        fill_is_nan=fill_is_nan,
        separator=separator,
        raw_digest=digest,
    )


# ---------------------------------------------------------------- 清单与 URL

def _http_join(base_url: str, ref: str) -> str:
    """按 manifest URL 的 origin 解析清单内的绝对路径。"""
    if not ref.startswith('/'):
        raise RegionError('manifest 中的路径必须是绝对路径（以 / 开头）')
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        raise RegionError('manifest URL 必须是 http(s) 绝对地址')
    origin = f'{parsed.scheme}://{parsed.netloc}'
    return origin + ref


def load_manifest(manifest_bytes: bytes) -> dict:
    try:
        manifest = json.loads(manifest_bytes.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RegionError(f'manifest 不是合法 JSON: {exc}') from exc
    required = ('generation', 'metadata', 'metadata_sha256', 'chunk_prefix')
    if not isinstance(manifest, dict) or any(not isinstance(manifest.get(k), str) for k in required):
        raise RegionError('manifest 必须包含字符串字段 generation/metadata/metadata_sha256/chunk_prefix')
    if not manifest['chunk_prefix'].startswith('/'):
        raise RegionError('chunk_prefix 必须是绝对路径')
    if not manifest['chunk_prefix'].endswith('/'):
        raise RegionError('chunk_prefix 必须以 / 结尾，确保所有块都在该前缀内')
    for key in ('metadata', 'chunk_prefix'):
        ref = manifest[key]
        if '?' in ref or '#' in ref:
            raise RegionError(f'{key} 不允许 query 或 fragment')
    if not manifest['metadata'].startswith(manifest['chunk_prefix']):
        raise RegionError('metadata 路径必须落在 chunk_prefix 之内')
    if len(manifest['metadata_sha256']) != 64 or any(
            c not in '0123456789abcdef' for c in manifest['metadata_sha256']):
        raise RegionError('metadata_sha256 必须是 64 位十六进制摘要')
    return manifest


# ---------------------------------------------------------------- 块缓存

class ChunkCache:
    """磁盘块缓存。

    缓存键包含 generation、.zarray 摘要与块坐标。失败或不完整的块不会落盘；
    登记文件与数据文件原子替换，进程在两者之间退出后，恢复时会重新校验。
    """

    def __init__(self, root: str | os.PathLike):
        self.root = Path(root)
        self.entries_dir = self.root / 'entries'
        self.blobs_dir = self.root / 'blobs'
        self.root.mkdir(parents=True, exist_ok=True)
        self.entries_dir.mkdir(exist_ok=True)
        self.blobs_dir.mkdir(exist_ok=True)

    @staticmethod
    def key(generation: str, metadata_digest: str, coord: tuple[int, int]) -> str:
        raw = f'{generation}\x1f{metadata_digest}\x1f{coord[0]},{coord[1]}'.encode('utf-8')
        return _sha256_hex(raw)

    def _entry_path(self, key: str) -> Path:
        if not (len(key) == 64 and all(c in '0123456789abcdef' for c in key)):
            raise RegionError('缓存键格式不合法')
        return self.entries_dir / key

    def get(self, generation: str, metadata_digest: str, coord: tuple[int, int]):
        """返回 (原始线上字节, 登记信息)；任何不一致都视为未命中。"""
        key = self.key(generation, metadata_digest, coord)
        entry_path = self._entry_path(key)
        if not entry_path.exists():
            return None
        try:
            entry = json.loads(entry_path.read_text(encoding='utf-8'))
            blob = self.blobs_dir / entry['blob']
            raw = blob.read_bytes()
        except (OSError, ValueError, KeyError):
            # 登记存在但数据缺失（进程在落盘与登记之间退出）：清理后重验。
            self._discard(entry_path)
            return None
        if _sha256_hex(raw) != entry.get('raw_sha256'):
            self._discard(entry_path)
            return None
        if (entry.get('generation'), entry.get('metadata_sha256'),
                tuple(entry.get('chunk_coord', []))) != (generation, metadata_digest, coord):
            self._discard(entry_path)
            return None
        return raw, entry

    def put(self, generation: str, metadata_digest: str, coord: tuple[int, int],
            raw: bytes) -> None:
        key = self.key(generation, metadata_digest, coord)
        raw_digest = _sha256_hex(raw)
        # 内容寻址，已存在且摘要相符则直接复用。
        blob_path = self.blobs_dir / raw_digest
        if not blob_path.exists():
            fd, name = tempfile.mkstemp(prefix='.part-', dir=self.blobs_dir)
            try:
                with os.fdopen(fd, 'wb') as f:
                    f.write(raw)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(name, blob_path)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
        entry = {
            'generation': generation,
            'metadata_sha256': metadata_digest,
            'chunk_coord': [coord[0], coord[1]],
            'blob': raw_digest,
            'raw_sha256': raw_digest,
        }
        fd, name = tempfile.mkstemp(prefix='.part-', dir=self.entries_dir)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(entry, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(name, self._entry_path(key))
        finally:
            if os.path.exists(name):
                os.unlink(name)

    def _discard(self, entry_path: Path) -> None:
        try:
            entry_path.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------- 解码

def decode_chunk(raw: bytes, meta: ArrayMeta) -> array.array:
    """按 compressor/dtype/order 解码一个完整 chunk，失败即抛 RegionError。"""
    if meta.compressor == 'zlib':
        if len(raw) > meta.chunk_bytes * 64 and len(raw) > 1 << 20:
            # zlib 炸弹的早期拦截：压缩体异常大于定长解码结果。
            raise RegionError('压缩块体积异常，拒绝解压')
        try:
            dec = zlib.decompressobj()
            payload = dec.decompress(raw, meta.chunk_bytes + 1)
            payload += dec.flush()
        except zlib.error as exc:
            raise RegionError(f'zlib 解压失败: {exc}') from exc
        # eof=False 表示流被截断（缺 adler/长度尾），unused/unconsumed 表示有尾随或超长。
        if not dec.eof:
            raise RegionError('zlib 流被截断：缺少完整的流结束标记')
        if dec.unconsumed_tail or dec.unused_data:
            raise RegionError('zlib 流必须恰好一条完整流，存在尾随字节或超长数据')
    else:
        payload = raw
    if len(payload) != meta.chunk_bytes:
        raise RegionError(
            f'块长度不符: 期望 {meta.chunk_bytes} 字节，实际 {len(payload)} 字节')
    values = array.array(meta.typecode)
    try:
        values.frombytes(payload)
    except ValueError as exc:
        raise RegionError(f'块字节无法按 {meta.dtype_text} 解释: {exc}') from exc
    if len(values) != meta.chunk_numel:
        raise RegionError('解码元素数量与 chunk 形状不符')
    if meta.dtype_text[0] == '>':
        values.byteswap()
    return values


def _filled_array(meta: ArrayMeta) -> array.array:
    if meta.fill_is_nan:
        return array.array(meta.typecode, [float('nan')]) * meta.chunk_numel
    return array.array(meta.typecode, [meta.fill_value]) * meta.chunk_numel


# ---------------------------------------------------------------- 采集器

class RegionCollector:
    """绑定一个不可变 manifest 的区域采集器。"""

    def __init__(self, manifest_url: str, limits: Limits | None = None,
                 cache: ChunkCache | None = None, opener=None):
        self.manifest_url = manifest_url
        self.limits = limits or Limits()
        self.cache = cache
        self._opener = opener or (lambda req, timeout: urllib.request.urlopen(req, timeout=timeout))
        self._manifest = None
        self._meta: ArrayMeta | None = None
        self._chunk_prefix_url: str | None = None
        self._request_count = 0
        self._count_lock = threading.Lock()

    # -- HTTP -------------------------------------------------------------

    def _http_get(self, url: str):
        """返回 (状态码, 字节)。404 由调用方决定语义，其他错误直接失败。"""
        with self._count_lock:
            self._request_count += 1
        req = urllib.request.Request(url, method='GET')
        try:
            with self._opener(req, self.limits.request_timeout) as resp:
                status = getattr(resp, 'status', resp.getcode())
                data = resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return 404, b''
            raise RegionError(f'HTTP {exc.code} 读取 {url} 失败，本次区域结果作废') from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RegionError(f'网络读取 {url} 失败: {exc}') from exc
        if status != 200:
            if status == 404:
                return 404, b''
            raise RegionError(f'HTTP {status} 读取 {url} 失败，本次区域结果作废')
        return 200, data

    def _ensure_manifest(self):
        if self._manifest is not None:
            return
        _, raw = self._http_get(self.manifest_url)
        manifest = load_manifest(raw)
        metadata_url = _http_join(self.manifest_url, manifest['metadata'])
        _, meta_raw = self._http_get(metadata_url)
        meta = parse_metadata(meta_raw, manifest['metadata_sha256'])
        prefix_url = _http_join(self.manifest_url, manifest['chunk_prefix'])
        self._manifest = manifest
        self._meta = meta
        self._chunk_prefix_url = prefix_url

    # -- 块覆盖 -----------------------------------------------------------

    def chunk_coverage(self, r0: int, c0: int, r1: int, c1: int):
        """计算与半开区域相交的块坐标，以及区域裁到数组 shape 后的范围。"""
        self._ensure_manifest()
        meta = self._meta
        rows, cols = meta.shape
        cr, cc = meta.chunks
        for name, v in (('r0', r0), ('c0', c0), ('r1', r1), ('c1', c1)):
            if not isinstance(v, int) or isinstance(v, bool):
                raise RegionError(f'{name} 必须是整数索引')
        if r0 < 0 or c0 < 0 or r1 > rows or c1 > cols:
            raise RegionError('区域越界：索引必须落在数组 shape 内')
        if r1 <= r0 or c1 <= c0:
            raise RegionError('区域必须是非空半开矩形 (r1>r0 且 c1>c0)')
        i0, i1 = r0 // cr, (r1 - 1) // cr
        j0, j1 = c0 // cc, (c1 - 1) // cc
        coords = [(i, j) for i in range(i0, i1 + 1) for j in range(j0, j1 + 1)]
        return coords, (r0, c0, r1, c1)

    def _chunk_url(self, coord: tuple[int, int]) -> str:
        meta = self._meta
        name = f'{coord[0]}{meta.separator}{coord[1]}'
        return self._chunk_prefix_url + name

    # -- 单块获取 ---------------------------------------------------------

    def _fetch_chunk(self, coord: tuple[int, int]):
        meta = self._meta
        generation = self._manifest['generation']
        digest = meta.raw_digest

        if self.cache is not None:
            hit = self.cache.get(generation, digest, coord)
            if hit is not None:
                raw, _ = hit
                values = decode_chunk(raw, meta)
                source = ChunkSource(
                    chunk_coord=coord, url=self._chunk_url(coord),
                    status='cache', http_status=None, sha256=_sha256_hex(raw),
                    bytes_downloaded=0, decoded_sha256=_sha256_hex(values.tobytes()),
                    filled=False)
                return values, source, True

        url = self._chunk_url(coord)
        status, raw = self._http_get(url)
        if status == 404:
            values = _filled_array(meta)
            source = ChunkSource(
                chunk_coord=coord, url=url, status='fill_404', http_status=404,
                sha256=None, bytes_downloaded=0,
                decoded_sha256=_sha256_hex(values.tobytes()), filled=True)
            # 404 是来源的稳定语义，但不缓存填充结果：版本/缓存键已隔离，
            # 缓存只存放经过完整校验的真实块字节。
            return values, source, False

        if len(raw) > self.limits.max_compressed_bytes:
            raise RegionLimitError(
                f'块 {coord} 线上字节 {len(raw)} 超过 max_compressed_bytes')
        values = decode_chunk(raw, meta)  # 失败则该块不进缓存
        if self.cache is not None:
            self.cache.put(generation, digest, coord, raw)
        source = ChunkSource(
            chunk_coord=coord, url=url, status='http', http_status=200,
            sha256=_sha256_hex(raw), bytes_downloaded=len(raw),
            decoded_sha256=_sha256_hex(values.tobytes()), filled=False)
        return values, source, False

    # -- 区域读取 ---------------------------------------------------------

    def read_region(self, r0: int, c0: int, r1: int, c1: int) -> RegionResult:
        coords, clipped = self.chunk_coverage(r0, c0, r1, c1)
        meta = self._meta
        limits = self.limits
        out_rows, out_cols = r1 - r0, c1 - c0
        output_bytes = out_rows * out_cols * meta.itemsize
        if output_bytes > limits.max_output_bytes:
            raise RegionLimitError(
                f'输出 {output_bytes} 字节超过 max_output_bytes={limits.max_output_bytes}，'
                '尚未分配结果缓冲')
        # 解码峰值：最多并发数个完整 chunk 同时驻留。
        peak = min(len(coords), limits.max_concurrency) * meta.chunk_bytes
        if peak > limits.max_decoded_bytes:
            raise RegionLimitError(
                f'解码峰值约 {peak} 字节超过 max_decoded_bytes={limits.max_decoded_bytes}')

        results = {}
        sources = []
        cache_hits = 0
        # 有界线程池：并发请求数量独立受限。
        workers = min(limits.max_concurrency, len(coords))
        if workers == 1:
            for coord in coords:
                values, source, was_cache = self._fetch_chunk(coord)
                results[coord] = values
                sources.append(source)
                cache_hits += int(was_cache)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                future_map = {pool.submit(self._fetch_chunk, c): c for c in coords}
                for future in concurrent.futures.as_completed(future_map):
                    coord = future_map[future]
                    values, source, was_cache = future.result()
                    results[coord] = values
                    sources.append(source)
                    cache_hits += int(was_cache)
        total_requests = self._request_count

        data = self._assemble(coords, results, clipped)
        sources.sort(key=lambda s: s.chunk_coord)

        filled_cells = 0
        fill_value_cells = 0
        nan_fill = meta.fill_is_nan
        for row in data:
            for v in row:
                if nan_fill:
                    if math.isnan(v):
                        filled_cells += 1
                        fill_value_cells += 1
                elif v == meta.fill_value:
                    fill_value_cells += 1
        # filled_cells 只统计来自 404 填充块、且落在输出内的单元。
        filled_sources = {s.chunk_coord for s in sources if s.filled}
        if filled_sources:
            cr, cc = meta.chunks
            for (i, j) in coords:
                if (i, j) not in filled_sources:
                    continue
                br0, br1 = max(r0, i * cr), min(r1, (i + 1) * cr)
                bc0, bc1 = max(c0, j * cc), min(c1, (j + 1) * cc)
                filled_cells += (br1 - br0) * (bc1 - bc0)

        return RegionResult(
            generation=self._manifest['generation'],
            region=clipped,
            shape=(out_rows, out_cols),
            dtype=meta.dtype_text,
            order=meta.order,
            fill_value=meta.fill_value,
            data=data,
            sources=sources,
            http_requests=total_requests,
            cache_hits=cache_hits,
            filled_cells=filled_cells,
            fill_value_cells=fill_value_cells,
        )

    def _assemble(self, coords, chunk_values, clipped):
        """把完整 chunk 按 dtype/order 取到输出，边界裁在 shape 与查询区域内。"""
        meta = self._meta
        r0, c0, r1, c1 = clipped
        cr, cc = meta.chunks
        nr, nc = meta.chunks
        # 输出用 list[list]，逐格拷贝，避免持有整块内存的额外视图。
        data = [[None] * (c1 - c0) for _ in range(r1 - r0)]
        for (i, j) in coords:
            values = chunk_values[(i, j)]
            base_r, base_c = i * cr, j * cc
            br0, br1 = max(r0, base_r), min(r1, base_r + cr)
            bc0, bc1 = max(c0, base_c), min(c1, base_c + cc)
            for r in range(br0, br1):
                lr = r - base_r
                out_r = r - r0
                for c in range(bc0, bc1):
                    lc = c - base_c
                    if meta.order == 'C':
                        flat = lr * nc + lc
                    else:  # F：列优先，绝不能按转置后的 C 顺序解码
                        flat = lc * nr + lr
                    data[out_r][c - c0] = values[flat]
        return data


# ---------------------------------------------------------------- 便捷 API

def read_region(manifest_url: str, r0: int, c0: int, r1: int, c1: int,
                limits: Limits | None = None,
                cache: ChunkCache | None = None,
                opener=None) -> RegionResult:
    """区域读取 API：返回可核对数值与逐块来源。"""
    collector = RegionCollector(manifest_url, limits=limits, cache=cache, opener=opener)
    return collector.read_region(r0, c0, r1, c1)


def write_result_atomic(result: RegionResult, out_path: str | os.PathLike) -> None:
    """原子写出正式结果；失败不会留下看似成功的文件。"""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(result.to_report(), ensure_ascii=False, indent=2,
                         allow_nan=False).encode('utf-8')
    fd, name = tempfile.mkstemp(prefix='.part-', dir=out_path.parent)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, out_path)
    finally:
        if os.path.exists(name):
            os.unlink(name)
