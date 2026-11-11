"""工作台账命令行：字节指纹查看与 Zarr 区域采集。"""
import argparse
import hashlib
import json
from pathlib import Path

from workbench.region_reader import (
    ChunkCache,
    Limits,
    RegionError,
    RegionLimitError,
    read_region,
    write_result_atomic,
)


def _inspect(directory: str) -> int:
    root = Path(directory)
    if not root.is_dir():
        raise SystemExit('资料目录不存在')
    rows = []
    for path in sorted(root.rglob('*')):
        if path.is_file():
            data = path.read_bytes()
            rows.append({'path': path.relative_to(root).as_posix(),
                         'size': len(data),
                         'sha256': hashlib.sha256(data).hexdigest()})
    print(json.dumps(rows, ensure_ascii=False, indent=2))
    return 0


def _json_safe_grid(data):
    """NaN 在正式 JSON 中写成 'NaN' 字符串，API 返回值仍是 float nan。"""
    safe = []
    for row in data:
        safe.append(['NaN' if isinstance(v, float) and v != v else v for v in row])
    return safe


def _collect(args) -> int:
    limits = Limits(
        max_concurrency=args.max_concurrency,
        max_decoded_bytes=args.max_decoded_bytes,
        max_output_bytes=args.max_output_bytes,
        request_timeout=args.timeout,
    )
    cache = ChunkCache(args.cache_dir) if args.cache_dir else None
    try:
        result = read_region(args.manifest_url, args.r0, args.c0, args.r1, args.c1,
                             limits=limits, cache=cache)
    except RegionLimitError as exc:
        print(json.dumps({'ok': False, 'reason': 'limit', 'error': str(exc)},
                         ensure_ascii=False))
        return 3
    except RegionError as exc:
        print(json.dumps({'ok': False, 'reason': 'region_failed', 'error': str(exc)},
                         ensure_ascii=False))
        return 2

    report = result.to_report()
    report['ok'] = True
    if args.data_out:
        # 数据与来源清单分开落盘，均为原子替换。
        data_payload = {
            'generation': result.generation,
            'region': list(result.region),
            'shape': list(result.shape),
            'dtype': result.dtype,
            'order': result.order,
            'data': _json_safe_grid(result.data),
        }
        _atomic_json(args.data_out, data_payload)
        report['data_out'] = str(Path(args.data_out).resolve())
    if args.out:
        write_result_atomic(result, args.out)
        report['report_out'] = str(Path(args.out).resolve())
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
    return 0


def _atomic_json(path, payload):
    import os
    import tempfile
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.part-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(json.dumps(payload, ensure_ascii=False, indent=2,
                               allow_nan=False).encode('utf-8'))
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def main():
    p = argparse.ArgumentParser(prog='workbench.cli')
    sub = p.add_subparsers(dest='command', required=True)

    p_inspect = sub.add_parser('inspect', help='查看目录内文件的字节指纹')
    p_inspect.add_argument('directory')

    p_collect = sub.add_parser('collect', help='按半开索引框采集 Zarr v2 区域')
    p_collect.add_argument('manifest_url', help='manifest 的完整 URL，所有读取固定在其前缀内')
    p_collect.add_argument('r0', type=int, help='起始行（含）')
    p_collect.add_argument('c0', type=int, help='起始列（含）')
    p_collect.add_argument('r1', type=int, help='结束行（不含）')
    p_collect.add_argument('c1', type=int, help='结束列（不含）')
    p_collect.add_argument('--cache-dir', help='块缓存目录，缓存键含 generation/摘要/坐标')
    p_collect.add_argument('--out', help='来源清单与输出摘要的原子落盘路径')
    p_collect.add_argument('--data-out', help='区域数值网格的原子落盘路径')
    p_collect.add_argument('--max-concurrency', type=int, default=4)
    p_collect.add_argument('--max-decoded-bytes', type=int, default=64 * 1024 * 1024)
    p_collect.add_argument('--max-output-bytes', type=int, default=64 * 1024 * 1024)
    p_collect.add_argument('--timeout', type=float, default=10.0)
    p_collect.set_defaults(func=_collect)

    args = p.parse_args()
    if args.command == 'inspect':
        raise SystemExit(_inspect(args.directory))
    raise SystemExit(args.func(args))


if __name__ == '__main__':
    main()
