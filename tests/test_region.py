"""Zarr v2 区域采集器的端到端核对。

本地对象站覆盖：C/F 顺序、大小端、i2/u2/f4、点号/斜线分隔、null/zlib 压缩、
部分越过边界、404 填充、403/损坏/截断/尾随字节失败、缓存隔离与恢复、
版本切换、上限拒绝与未相交块不读取。
"""
from __future__ import annotations

import array
import hashlib
import json
import math
import subprocess
import sys
import tempfile
import unittest
import zlib
from pathlib import Path

from source_lab.http import start
from workbench.region_reader import (
    ChunkCache,
    Limits,
    RegionError,
    RegionLimitError,
    parse_metadata,
    read_region,
)

ROOT = Path(__file__).resolve().parents[1]


# ----------------------------------------------------------- 合成来源构建

def encode_chunk(grid, dtype, order, compressor):
    """grid: 完整 chunk 形状的二维 list，按 dtype/order 编码为线上字节。"""
    nr, nc = len(grid), len(grid[0])
    endian, tname = dtype[0], dtype[1:]
    tc = {'i2': 'h', 'u2': 'H', 'f4': 'f'}[tname]
    flat = array.array(tc)
    if order == 'C':
        for r in range(nr):
            flat.extend(grid[r])
    else:
        for c in range(nc):
            for r in range(nr):
                flat.append(grid[r][c])
    if endian == '>':
        flat.byteswap()
    raw = flat.tobytes()
    if compressor == 'zlib':
        raw = zlib.compress(raw)
    return raw


def build_source(root: Path, gen: str, *, shape, chunks, dtype, order,
                 compressor, fill_value, separator, logical, missing=(),
                 corrupt_overrides=None):
    """在 root/gen 下生成 zarr v2 对象树，返回 manifest dict。"""
    gdir = root / gen
    gdir.mkdir(parents=True)
    cr, cc = chunks
    nr = (shape[0] + cr - 1) // cr
    nc = (shape[1] + cc - 1) // cc
    meta = {
        'zarr_format': 2, 'shape': list(shape), 'chunks': list(chunks),
        'dtype': dtype,
        'compressor': None if compressor is None else {'id': compressor, 'level': 6},
        'fill_value': fill_value, 'order': order, 'filters': None,
        'dimension_separator': separator,
    }
    meta_bytes = json.dumps(meta, separators=(',', ':')).encode('utf-8')
    (gdir / '.zarray').write_bytes(meta_bytes)
    corrupt_overrides = corrupt_overrides or {}
    for i in range(nr):
        for j in range(nc):
            grid = []
            for r in range(cr):
                row = []
                for c in range(cc):
                    gr, gc = i * cr + r, j * cc + c
                    if gr < shape[0] and gc < shape[1]:
                        row.append(logical[gr][gc])
                    else:
                        row.append(fill_value if not (
                            isinstance(fill_value, str) and fill_value == 'NaN') else float('nan'))
                grid.append(row)
            rel = f'{i}{separator}{j}'
            path = gdir / rel
            if (i, j) in missing:
                continue  # 该块整体缺失，靠 404 补
            if (i, j) in corrupt_overrides:
                path.write_bytes(corrupt_overrides[(i, j)])
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(encode_chunk(grid, dtype, order, compressor))
    prefix = f'/{gen}/'
    manifest = {
        'generation': gen,
        'metadata': f'/{gen}/.zarray',
        'metadata_sha256': hashlib.sha256(meta_bytes).hexdigest(),
        'chunk_prefix': prefix,
    }
    (root / f'{gen}-manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False), encoding='utf-8')
    return manifest


def serve(root: Path, routes):
    """routes: 路径到 (status, bytes|str) 的映射，缺失路径返回 404。

    二进制负载写入 scenario 同目录的响应文件，避免经 JSON 文本转码。
    """
    payloads = root / 'payloads'
    payloads.mkdir(exist_ok=True)
    cfg = {'routes': []}
    for path, (status, payload) in routes.items():
        resp = {'status': status}
        if isinstance(payload, bytes):
            # 内容寻址命名：同一 tmp 目录被多套 routes 复用时不会互相覆盖。
            name = hashlib.sha256(payload).hexdigest() + '.bin'
            target = payloads / name
            if not target.exists():
                target.write_bytes(payload)
            resp['file'] = name
        else:
            resp['text'] = payload
        cfg['routes'].append({'path': path, 'responses': [resp]})
    # scenario 与响应文件同目录，http.py 按 scenario 所在目录解析 file。
    scenario = payloads / 'scenario.json'
    scenario.write_text(json.dumps(cfg), encoding='utf-8')
    return start(scenario)


def static_routes(root: Path, gen: str, manifest: dict):
    """从对象树直接生成全量 200 路由（manifest 也在内）。"""
    routes = {
        f'/{gen}/manifest.json': (200, (root / f'{gen}-manifest.json').read_bytes()),
    }
    for p in (root / gen).rglob('*'):
        if p.is_file():
            rel = f'/{gen}/' + str(p.relative_to(root / gen)).replace('\\', '/')
            routes.setdefault(rel, (200, p.read_bytes()))
    return routes


class RegionReaderTests(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.logical = [
            [0, 1, 2, 3, 4],
            [10, 11, 12, 13, 14],
            [20, 21, 22, 23, 24],
        ]

    def tearDown(self):
        self._tmp.cleanup()

    def _base(self, gen='g1', **kw):
        root = self.tmp / 'src'
        root.mkdir(exist_ok=True)
        manifest = build_source(root, gen, logical=self.logical, **kw)
        return root, manifest

    def _run(self, server, gen, region, manifest_path=None, **kw):
        base = f'http://127.0.0.1:{server.server_port}'
        url = base + (manifest_path or f'/{gen}/manifest.json')
        return read_region(url, *region, **kw)

    # ------------------------------------------------ C 顺序小端 i2 / 点号

    def test_c_order_little_i2_dot_full_and_partial(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        routes = static_routes(root, 'g1', manifest)
        server = serve(root, routes)
        try:
            # 整个数组：小样本逐元素比对
            result = self._run(server, 'g1', (0, 0, 3, 5))
            self.assertEqual(result.data, self.logical)
            self.assertEqual(result.shape, (3, 5))
            self.assertEqual(result.dtype, '<i2')
            self.assertEqual(result.order, 'C')
            # 2x2=4 个块全读 + manifest + .zarray = 6 个请求
            self.assertEqual(result.http_requests, 6)
            self.assertEqual([s.chunk_coord for s in result.sources],
                             [(0, 0), (0, 1), (1, 0), (1, 1)])
            self.assertTrue(all(s.status == 'http' for s in result.sources))
            self.assertTrue(all(s.sha256 and len(s.sha256) == 64
                                for s in result.sources))

            # 越过右/下边界的区域，输出裁在 shape 内（独立服务以精确核对请求数）
            server2 = serve(root, static_routes(root, 'g1', manifest))
            try:
                # 行 2-2（仅块行1）、列 2-4（块列0和1）：相交 (1,0)(1,1) 两块
                partial = self._run(server2, 'g1', (2, 2, 3, 5))
                self.assertEqual(partial.data, [row[2:5] for row in self.logical[2:3]])
                self.assertEqual(partial.shape, (1, 3))
                # 2 个块 + manifest + .zarray = 4 个请求
                self.assertEqual(partial.http_requests, 4)
                self.assertEqual({s.chunk_coord for s in partial.sources},
                                 {(1, 0), (1, 1)})
                # 下边缘块按完整 chunk 编码，输出只取 shape 内的一行
                bottom = next(s for s in partial.sources if s.chunk_coord == (1, 0))
                self.assertEqual(bottom.status, 'http')
            finally:
                server2.shutdown(); server2.server_close()
        finally:
            server.shutdown(); server.server_close()

    def test_unintersected_chunks_never_requested(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        routes = static_routes(root, 'g1', manifest)
        server = serve(root, routes)
        try:
            # 只落在块 (0,0) 内
            result = self._run(server, 'g1', (0, 0, 1, 2))
            self.assertEqual(result.data, [row[0:2] for row in self.logical[0:1]])
            self.assertEqual([s.chunk_coord for s in result.sources], [(0, 0)])
            paths = [e['path'] for e in server.events]
            self.assertIn('/g1/0.0', paths)
            for forbidden in ('/g1/0.1', '/g1/1.0', '/g1/1.1'):
                self.assertNotIn(forbidden, paths)
            # 所有请求都在 chunk_prefix 同来源内
            for p in paths:
                self.assertTrue(p.startswith('/g1/'))
        finally:
            server.shutdown(); server.server_close()

    # ------------------------------------------------ F 顺序大端 u2 / 斜线 / zlib

    def test_f_order_big_u2_slash_zlib_matches_logical(self):
        root, manifest = self._base(
            'g2', shape=[3, 5], chunks=[2, 3], dtype='>u2', order='F',
            compressor='zlib', fill_value=0, separator='/')
        routes = static_routes(root, 'g2', manifest)
        server = serve(root, routes)
        try:
            result = self._run(server, 'g2', (0, 0, 3, 5))
            self.assertEqual(result.data, self.logical)  # 与 C 顺序逻辑数组相同
            # 确认不是把 F 当转置 C 蒙混：取一个跨行列的区域
            sub = self._run(server, 'g2', (1, 1, 3, 4))
            expect = [row[1:4] for row in self.logical[1:3]]
            self.assertEqual(sub.data, expect)
            for s in sub.sources:
                self.assertTrue(s.url.startswith(
                    f'http://127.0.0.1:{server.server_port}/g2/'))
                # 斜线分隔的路径形如 /g2/0/1
                tail = s.url.split('/g2/')[1]
                self.assertIn('/', tail)
                self.assertNotIn('.', tail)
        finally:
            server.shutdown(); server.server_close()

    def test_f_order_is_not_transposed_c(self):
        """构造非对称数组，证明 F 顺序按列优先解码而非转置。"""
        root = self.tmp / 'src2'; root.mkdir()
        logical = [[1, 2, 3, 4], [5, 6, 7, 8]]  # 2x4，单块
        manifest = build_source(
            root, 'gf', shape=[2, 4], chunks=[2, 4], dtype='<i2', order='F',
            compressor=None, fill_value=0, separator='.', logical=logical)
        server = serve(root, static_routes(root, 'gf', manifest))
        try:
            result = self._run(server, 'gf', (0, 0, 2, 4))
            self.assertEqual(result.data, logical)
            self.assertNotEqual(result.data, [list(x) for x in zip(*logical)])
        finally:
            server.shutdown(); server.server_close()

    # ------------------------------------------------ 404 填充

    def test_missing_chunk_404_uses_fill_value(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.', missing={(0, 0)})
        routes = static_routes(root, 'g1', manifest)  # 缺块没有路由 -> 服务端 404
        server = serve(root, routes)
        try:
            result = self._run(server, 'g1', (0, 0, 3, 5))
            expect = [row[:] for row in self.logical]
            # 块 (0,0) 覆盖行 0-1、列 0-2；其中落在 shape 内的单元被填 0
            for r in range(2):
                for c in range(3):
                    expect[r][c] = 0
            self.assertEqual(result.data, expect)
            src = next(s for s in result.sources if s.chunk_coord == (0, 0))
            self.assertEqual(src.status, 'fill_404')
            self.assertEqual(src.http_status, 404)
            self.assertTrue(src.filled)
            self.assertEqual(result.filled_cells, 6)  # 2 行 x 3 列落在 shape 内
            report = result.to_report()
            # filled_cells 只计 404 块；fill_value_cells 计输出中全部等于 0 的单元。
            # 填充块恰好覆盖了原数组唯一的 0（[0][0]），故两者在此场景都是 6。
            self.assertEqual(report['filled_cells'], 6)
            self.assertEqual(report['fill_value_cells'], 6)
            # 填充值范围（按行展平的半开闭区间）
            self.assertIn([0, 3], report['fill_value_ranges'])
        finally:
            server.shutdown(); server.server_close()

    def test_filled_vs_fill_value_cells_are_distinct(self):
        """填充值非零且真实数据含 0 时，两种计数必须可区分。"""
        root = self.tmp / 'srcdist'; root.mkdir()
        logical = [[0, 7], [7, 0]]  # 数据本身含两个 0
        manifest = build_source(
            root, 'gd', shape=[2, 2], chunks=[2, 2], dtype='<i2', order='C',
            compressor=None, fill_value=9, separator='.', logical=logical,
            missing={(0, 0)})
        server = serve(root, static_routes(root, 'gd', manifest))
        try:
            result = self._run(server, 'gd', (0, 0, 2, 2))
            # 整块 4 个单元都来自 404 填充
            self.assertEqual(result.filled_cells, 4)
            self.assertEqual(result.fill_value_cells, 4)  # 输出里等于 9 的
            # 真实数据的 0 已被填充覆盖，输出中不存在 0
            self.assertEqual([v for row in result.data for v in row], [9, 9, 9, 9])
        finally:
            server.shutdown(); server.server_close()

    def test_nan_fill_f4(self):
        root = self.tmp / 'srcf'; root.mkdir()
        logical = [[1.5, 2.5], [3.5, 4.5]]
        manifest = build_source(
            root, 'gf', shape=[2, 2], chunks=[2, 2], dtype='<f4', order='C',
            compressor='zlib', fill_value='NaN', separator='.', logical=logical,
            missing={(0, 0)})
        server = serve(root, static_routes(root, 'gf', manifest))
        try:
            result = self._run(server, 'gf', (0, 0, 2, 2))
            self.assertTrue(all(math.isnan(v) for v in result.data[0] + result.data[1]))
            report = result.to_report()
            self.assertEqual(report['fill_value'], 'NaN')
            self.assertEqual(report['output_summary']['nan_cells'], 4)
            self.assertEqual(report['fill_value_ranges'], [[0, 4]])
        finally:
            server.shutdown(); server.server_close()

    # ------------------------------------------------ 失败语义：绝不伪装空白

    def test_403_fails_region(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        routes = static_routes(root, 'g1', manifest)
        routes['/g1/0.0'] = (403, b'forbidden')
        server = serve(root, routes)
        try:
            with self.assertRaises(RegionError) as ctx:
                self._run(server, 'g1', (0, 0, 3, 5))
            self.assertIn('403', str(ctx.exception))
        finally:
            server.shutdown(); server.server_close()

    def test_500_fails_region(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        routes = static_routes(root, 'g1', manifest)
        routes['/g1/1.1'] = (500, b'boom')
        server = serve(root, routes)
        try:
            with self.assertRaises(RegionError):
                self._run(server, 'g1', (0, 0, 3, 5))
        finally:
            server.shutdown(); server.server_close()

    def test_corrupt_length_mismatch_fails(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.',
            corrupt_overrides={(1, 0): b'short'})
        server = serve(root, static_routes(root, 'g1', manifest))
        try:
            with self.assertRaises(RegionError) as ctx:
                self._run(server, 'g1', (0, 0, 3, 5))
            self.assertIn('长度不符', str(ctx.exception))
        finally:
            server.shutdown(); server.server_close()

    def test_truncated_zlib_fails(self):
        root, manifest = self._base(
            'g2', shape=[3, 5], chunks=[2, 3], dtype='>u2', order='F',
            compressor='zlib', fill_value=0, separator='/')
        routes = static_routes(root, 'g2', manifest)
        good = routes['/g2/0/0'][1]
        routes['/g2/0/0'] = (200, good[:-2])  # 截断的 zlib 流
        server = serve(root, routes)
        try:
            with self.assertRaises(RegionError):
                self._run(server, 'g2', (0, 0, 3, 5))
        finally:
            server.shutdown(); server.server_close()

    def test_zlib_trailing_bytes_fails(self):
        root, manifest = self._base(
            'g2', shape=[3, 5], chunks=[2, 3], dtype='>u2', order='F',
            compressor='zlib', fill_value=0, separator='/')
        routes = static_routes(root, 'g2', manifest)
        good = routes['/g2/1/0'][1]
        routes['/g2/1/0'] = (200, good + b'\x00\x00')  # 尾随字节
        server = serve(root, routes)
        try:
            with self.assertRaises(RegionError) as ctx:
                self._run(server, 'g2', (0, 0, 3, 5))
            self.assertIn('尾随字节', str(ctx.exception))
        finally:
            server.shutdown(); server.server_close()

    def test_failed_chunk_never_cached(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.',
            corrupt_overrides={(0, 0): b'bad-bytes'})
        cache_dir = self.tmp / 'cache'
        cache = ChunkCache(cache_dir)
        server = serve(root, static_routes(root, 'g1', manifest))
        try:
            with self.assertRaises(RegionError):
                self._run(server, 'g1', (0, 0, 2, 2), cache=cache)
            # 缓存目录里没有任何登记项
            self.assertEqual(list((cache_dir / 'entries').iterdir()), [])
        finally:
            server.shutdown(); server.server_close()

    # ------------------------------------------------ 缓存：共享、隔离、恢复

    def test_cache_shared_across_regions_and_isolated_by_generation(self):
        root = self.tmp / 'src'; root.mkdir(exist_ok=True)
        m1 = build_source(root, 'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2',
                          order='C', compressor=None, fill_value=0,
                          separator='.', logical=self.logical)
        # g2 同形状但内容不同（+100），模拟版本切换
        logical2 = [[v + 100 for v in row] for row in self.logical]
        m2 = build_source(root, 'g2', shape=[3, 5], chunks=[2, 3], dtype='<i2',
                          order='C', compressor=None, fill_value=0,
                          separator='.', logical=logical2)
        routes = {}
        routes.update(static_routes(root, 'g1', m1))
        routes.update(static_routes(root, 'g2', m2))
        server = serve(root, routes)
        cache = ChunkCache(self.tmp / 'cache')
        try:
            base = f'http://127.0.0.1:{server.server_port}'
            r1 = read_region(base + '/g1/manifest.json', 0, 0, 2, 2, cache=cache)
            self.assertEqual(r1.http_requests, 3)  # manifest + zarray + 1 块
            r1b = read_region(base + '/g1/manifest.json', 0, 0, 2, 2, cache=cache)
            self.assertEqual(r1b.http_requests, 2)  # 块命中缓存，仅元请求
            self.assertEqual(r1b.cache_hits, 1)
            self.assertEqual(r1b.data, r1.data)
            self.assertEqual(r1b.sources[0].status, 'cache')

            # 另一区域复用已校验的 (0,0)
            r1c = read_region(base + '/g1/manifest.json', 0, 0, 3, 5, cache=cache)
            self.assertEqual(r1c.cache_hits, 1)
            self.assertEqual(r1c.data, self.logical)

            # 版本切换：g2 不得读到 g1 的旧块
            r2 = read_region(base + '/g2/manifest.json', 0, 0, 3, 5, cache=cache)
            self.assertEqual(r2.data, logical2)
            self.assertEqual(r2.generation, 'g2')
            self.assertTrue(all(s.status == 'http' for s in r2.sources))
        finally:
            server.shutdown(); server.server_close()

    def test_cache_recovery_revalidates_after_crash_between_blob_and_entry(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        cache = ChunkCache(self.tmp / 'cache')
        server = serve(root, static_routes(root, 'g1', manifest))
        try:
            r1 = self._run(server, 'g1', (0, 0, 2, 2), cache=cache)
            self.assertEqual(r1.cache_hits, 0)
            # 模拟崩溃：删除登记文件，保留 blob（落盘与登记之间退出）
            entries = list((self.tmp / 'cache' / 'entries').iterdir())
            self.assertEqual(len(entries), 1)
            entries[0].unlink()
            r2 = self._run(server, 'g1', (0, 0, 2, 2), cache=cache)
            self.assertEqual(r2.cache_hits, 0)       # 不采信无登记的 blob
            self.assertEqual(r2.data, r1.data)       # 重新 HTTP 校验后可用
            self.assertEqual(len(list((self.tmp / 'cache' / 'entries').iterdir())), 1)

            # 篡改 blob 内容后，登记仍在但摘要不符 -> 重验
            entry = next((self.tmp / 'cache' / 'entries').iterdir())
            info = json.loads(entry.read_text())
            blob = self.tmp / 'cache' / 'blobs' / info['blob']
            blob.write_bytes(b'x' * blob.stat().st_size)
            r3 = self._run(server, 'g1', (0, 0, 2, 2), cache=cache)
            self.assertEqual(r3.cache_hits, 0)
            self.assertEqual(r3.data, r1.data)
        finally:
            server.shutdown(); server.server_close()

    def test_cache_key_includes_metadata_digest(self):
        """同 generation 但 .zarray 摘要变化（块布局变更）时缓存不得串用。"""
        root = self.tmp / 'src'; root.mkdir(exist_ok=True)
        m1 = build_source(root, 'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2',
                          order='C', compressor=None, fill_value=0,
                          separator='.', logical=self.logical)
        cache = ChunkCache(self.tmp / 'cache')
        server = serve(root, static_routes(root, 'g1', m1))
        try:
            self._run(server, 'g1', (0, 0, 2, 2), cache=cache)
            # 用不同摘要直接查缓存键，确认键空间分离
            k1 = ChunkCache.key('g1', m1['metadata_sha256'], (0, 0))
            k2 = ChunkCache.key('g1', 'f' * 64, (0, 0))
            self.assertNotEqual(k1, k2)
            self.assertIsNotNone(cache.get('g1', m1['metadata_sha256'], (0, 0)))
            self.assertIsNone(cache.get('g1', 'f' * 64, (0, 0)))
        finally:
            server.shutdown(); server.server_close()

    # ------------------------------------------------ 上限

    def test_output_limit_rejected_before_allocation(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        server = serve(root, static_routes(root, 'g1', manifest))
        try:
            limits = Limits(max_output_bytes=10)  # 输出需 30 字节
            with self.assertRaises(RegionLimitError):
                self._run(server, 'g1', (0, 0, 3, 5), limits=limits)
            # 上限拒绝发生在请求块之前：只有 manifest + zarray 两个请求
            self.assertEqual(
                [e['path'] for e in server.events].count('/g1/0.0'), 0)
        finally:
            server.shutdown(); server.server_close()

    def test_decoded_peak_limit(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        server = serve(root, static_routes(root, 'g1', manifest))
        try:
            limits = Limits(max_concurrency=4, max_decoded_bytes=11)  # 单块 12 字节
            with self.assertRaises(RegionLimitError):
                self._run(server, 'g1', (0, 0, 3, 5), limits=limits)
        finally:
            server.shutdown(); server.server_close()

    def test_concurrency_is_bounded(self):
        root = self.tmp / 'src4'; root.mkdir()
        logical = [[r * 10 + c for c in range(6)] for r in range(4)]
        manifest = build_source(
            root, 'g1', shape=[4, 6], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.', logical=logical)
        server = serve(root, static_routes(root, 'g1', manifest))
        try:
            # max_concurrency=1 仍要得到正确结果
            result = self._run(server, 'g1', (0, 0, 4, 6),
                               limits=Limits(max_concurrency=1))
            self.assertEqual(result.data, logical)
            self.assertEqual(len(result.sources), 4)
            # 真实请求 = manifest + .zarray + 4 块
            self.assertEqual(result.http_requests, 6)
        finally:
            server.shutdown(); server.server_close()

    # ------------------------------------------------ 元数据/清单校验

    def test_metadata_digest_mismatch_rejected_before_coverage(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        manifest['metadata_sha256'] = '0' * 64
        (root / 'g1-manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
        server = serve(root, static_routes(root, 'g1', manifest))
        try:
            with self.assertRaises(RegionError) as ctx:
                self._run(server, 'g1', (0, 0, 1, 1))
            self.assertIn('摘要', str(ctx.exception))
            # 摘要不符：任何 chunk 都不得请求
            paths = [e['path'] for e in server.events]
            self.assertNotIn('/g1/0.0', paths)
        finally:
            server.shutdown(); server.server_close()

    def test_rejected_encodings(self):
        base = {
            'zarr_format': 2, 'shape': [2, 2], 'chunks': [2, 2],
            'dtype': '<i2', 'compressor': None, 'fill_value': 0,
            'order': 'C', 'filters': None, 'dimension_separator': '.',
        }
        bad_variants = [
            {'zarr_format': 3},
            {'dtype': '<i4'},
            {'dtype': '|i2'},
            {'order': 'S'},
            {'compressor': {'id': 'blosc'}},
            {'filters': [{'id': 'shuffle'}]},
            {'dimension_separator': '_'},
            {'shape': [2, 2, 2]},
            {'fill_value': 1.5},          # i2 不接受浮点填充
        ]
        for override in bad_variants:
            meta = dict(base)
            meta.update(override)
            raw = json.dumps(meta).encode()
            with self.assertRaises(RegionError, msg=f'应拒绝 {override}'):
                parse_metadata(raw, hashlib.sha256(raw).hexdigest())

    def test_manifest_prefix_guard(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        # chunk_prefix 带 query 必须被拒
        bad = dict(manifest, chunk_prefix='/g1/?token=x')
        from workbench.region_reader import load_manifest
        with self.assertRaises(RegionError):
            load_manifest(json.dumps(bad).encode())
        bad2 = dict(manifest, metadata='/other/.zarray')
        with self.assertRaises(RegionError):
            load_manifest(json.dumps(bad2).encode())

    def test_big_endian_f4_roundtrip(self):
        root = self.tmp / 'srcbe'; root.mkdir()
        logical = [[-1.25, 0.0], [3.5, float('inf')]]
        manifest = build_source(
            root, 'be', shape=[2, 2], chunks=[2, 2], dtype='>f4', order='F',
            compressor=None, fill_value=0, separator='/', logical=logical)
        server = serve(root, static_routes(root, 'be', manifest))
        try:
            result = self._run(server, 'be', (0, 0, 2, 2))
            for r in range(2):
                for c in range(2):
                    got, want = result.data[r][c], logical[r][c]
                    if math.isinf(want):
                        self.assertTrue(math.isinf(got) and got > 0)
                    else:
                        self.assertAlmostEqual(got, want, places=6)
        finally:
            server.shutdown(); server.server_close()

    # ------------------------------------------------ CLI

    def test_cli_collect_and_atomic_outputs(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.')
        server = serve(root, static_routes(root, 'g1', manifest))
        out_dir = self.tmp / 'out'
        try:
            url = f'http://127.0.0.1:{server.server_port}/g1/manifest.json'
            runner = (
                'import sys; sys.path.insert(0, %r); '
                'from workbench.cli import main; sys.argv=["prog","collect"]+sys.argv[1:]; main()'
                % str(ROOT))
            proc = subprocess.run(
                [sys.executable, '-I', '-c', runner, url,
                 '0', '0', '3', '5',
                 '--cache-dir', str(self.tmp / 'cache'),
                 '--out', str(out_dir / 'report.json'),
                 '--data-out', str(out_dir / 'data.json')],
                cwd=self.tmp, capture_output=True, text=True, timeout=30)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            report = json.loads(proc.stdout)
            self.assertTrue(report['ok'])
            self.assertEqual(report['http_requests'], 6)
            self.assertEqual(report['output_summary'],
                             {'rows': 3, 'cols': 5, 'cells': 15,
                              'min': 0, 'max': 24, 'nan_cells': 0})
            on_disk = json.loads((out_dir / 'report.json').read_text())
            self.assertEqual(len(on_disk['chunks']), 4)
            data = json.loads((out_dir / 'data.json').read_text())
            self.assertEqual(data['data'], self.logical)
            # 无 .part 临时文件残留
            self.assertEqual([p.name for p in out_dir.iterdir()
                              if p.name.startswith('.part-')], [])

            # 第二次运行命中缓存
            proc2 = subprocess.run(
                [sys.executable, '-I', '-c', runner, url,
                 '0', '0', '2', '2',
                 '--cache-dir', str(self.tmp / 'cache')],
                cwd=self.tmp, capture_output=True, text=True, timeout=30)
            report2 = json.loads(proc2.stdout)
            self.assertEqual(report2['cache_hits'], 1)
            self.assertEqual(report2['http_requests'], 2)
        finally:
            server.shutdown(); server.server_close()

    def test_cli_failure_exit_code_and_no_official_result(self):
        root, manifest = self._base(
            'g1', shape=[3, 5], chunks=[2, 3], dtype='<i2', order='C',
            compressor=None, fill_value=0, separator='.',
            corrupt_overrides={(0, 0): b'bad'})
        server = serve(root, static_routes(root, 'g1', manifest))
        out = self.tmp / 'out' / 'report.json'
        try:
            url = f'http://127.0.0.1:{server.server_port}/g1/manifest.json'
            runner = (
                'import sys; sys.path.insert(0, %r); '
                'from workbench.cli import main; sys.argv=["prog","collect"]+sys.argv[1:]; main()'
                % str(ROOT))
            proc = subprocess.run(
                [sys.executable, '-I', '-c', runner, url,
                 '0', '0', '2', '2', '--out', str(out)],
                cwd=self.tmp, capture_output=True, text=True, timeout=30)
            self.assertEqual(proc.returncode, 2)
            payload = json.loads(proc.stdout)
            self.assertFalse(payload['ok'])
            self.assertFalse(out.exists())  # 失败不留正式结果
        finally:
            server.shutdown(); server.server_close()


if __name__ == '__main__':
    unittest.main()
