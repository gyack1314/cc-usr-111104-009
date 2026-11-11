# Zarr v2 区域采集器

`workbench.region_reader` 只下载与二维半开索引框 `[r0,r1) × [c0,c1)` 相交的 chunk，
按 `.zarray` 声明的 dtype 与 C/F 顺序严格解码，返回可核对的数值与逐块来源。
随附地图仅为合成数字数组，不涉及真实地理坐标或设备连接。

## 约定与校验顺序

1. 拉取入口 manifest（不可变 `generation`、`.zarray` 的 sha256 摘要、`chunk_prefix`）。
2. 按 manifest URL 的 origin 解析绝对路径，拉取 `.zarray`，**先核对摘要**，摘要不符立即失败，
   不计算块覆盖、不请求任何 chunk。
3. 严格校验元数据，以下编码明确拒绝（抛 `RegionError`）：
   - 非 `zarr_format=2`、非二维、非正整数 shape/chunks；
   - dtype 仅限大小端 `i2`/`u2`/`f4`（`<i2`、`>u2`、`<f4` 等）；
   - order 仅 `C`/`F`；compressor 仅 `null` 或 `{"id":"zlib"}`；
   - `filters` 仅接受 `null`；dimension_separator 仅 `.` 或 `/`；
   - fill_value：整数类型须为整数，f4 可为数字或字符串 `"NaN"`，且须在类型域内。
4. 校验通过后才计算与区域相交的块坐标。右/下边缘块仍按完整 chunk 形状解码，
   输出再裁到数组 shape 与查询区域的交集。

所有请求 URL 都落在 manifest 的 `chunk_prefix` 同来源内；带 query/fragment 的前缀、
metadata 不在前缀内、非 http(s) 的 manifest 地址都会被拒绝，未列出的 URL 不会被请求。

## 解码语义

- C 顺序：块内偏移 `lr * ncols + lc`；F 顺序：`lc * nrows + lr`。
  F 顺序按列优先直接解码，不会被当作“转置后的 C 顺序”处理。
- 大端 dtype 解码后做 `byteswap`；`i2/u2` 宽度 2、`f4` 宽度 4。
- null 压缩直接按定长解释；zlib 要求**恰好一条完整流**：
  解压对象 `eof` 必须为真（截断即失败），且 `unused_data`/`unconsumed_tail` 为空（尾随字节即失败）。
- 解码后字节长度必须等于 `chunks 元素数 × itemsize`，长度不符即失败，坏块不进缓存。

## 缺失与错误

| 情况 | 行为 |
| --- | --- |
| chunk 返回 404 | 按 fill_value 补整块，来源标记 `status="fill_404"`、`filled=true`，记录填充范围 |
| 403 / 500 / 其他非 404 状态 | 本次区域结果失败（`RegionError`），不补空白 |
| 连接失败、超时、截断 | 失败 |
| zlib 解压超限、尾随字节、长度不符 | 失败 |
| f4 的 NaN 填充 | 按 NaN 语义比较，正式 JSON 中写为字符串 `"NaN"` |

## 缓存

`ChunkCache` 为磁盘缓存，缓存键 = sha256(`generation` + `.zarray` 摘要 + 块坐标)，
因此不同 generation、不同元数据版本、不同块坐标的键空间彼此隔离，版本切换不会读到旧块。

- 只有通过完整解码校验的真实块字节才落盘；失败块、404 填充结果都不进缓存。
- blob 内容寻址，entry 与 blob 均用临时文件 + 原子替换 + fsync。
- 进程在 blob 落盘与 entry 登记之间退出：恢复时因缺少 entry 而视为未命中，重新 HTTP 校验；
  blob 被篡改导致摘要不符同样丢弃重验。
- 不同区域可复用已校验块（命中时 `status="cache"`、`bytes_downloaded=0`）。

## 资源上限（`Limits`，各自独立）

- `max_concurrency`：并发 HTTP 请求上限，有界线程池。
- `max_decoded_bytes`：同时驻留的解码块峰值上限（按并发数 × 单块字节估算）。
- `max_output_bytes`：输出字节上限，**在分配结果缓冲前**检查，超限抛 `RegionLimitError`。
- `max_compressed_bytes`：单个线上块体上限，拦截异常压缩体。

区域读取可逐块装配；任何块失败都抛异常，正式结果文件通过临时文件 + 原子替换写出，
失败路径不会留下看似成功的文件，也无 `.part-` 残留。

## API

```python
from workbench.region_reader import read_region, ChunkCache, Limits, RegionError

result = read_region(
    "http://127.0.0.1:8801/g1/manifest.json",
    0, 1, 3, 4,                       # r0, c0, r1, c1（半开）
    limits=Limits(max_concurrency=4),
    cache=ChunkCache("./cache"),
)
result.data                 # list[list] 数值网格，已裁到 shape 与区域
result.sources              # 逐块 ChunkSource：坐标、URL、状态、下载/解码摘要
result.http_requests        # 真实 HTTP 请求总数（含 manifest 与 .zarray）
result.cache_hits           # 缓存命中块数
result.filled_cells         # 来自 404 填充块、落在输出内的单元数
result.fill_value_cells     # 输出中等于 fill_value 的单元总数
result.to_report()          # 可 JSON 化的来源清单与输出摘要
```

`RegionResult.to_report()` 包含：generation、region、shape、dtype、order、fill_value、
`output_summary`（rows/cols/cells/min/max/nan_cells）、`http_requests`、`cache_hits`、
`filled_cells`、`fill_value_ranges`（按行展平的半开填充区间），以及每个 chunk 的
`chunk_coord`、`url`、`status`（http/fill_404/cache）、`http_status`、`filled`、
`bytes_downloaded`、线上字节 `sha256` 与 `decoded_sha256`。

## 采集命令

```sh
python -m workbench.cli collect <manifest_url> <r0> <c0> <r1> <c1> \
    [--cache-dir DIR] [--out report.json] [--data-out data.json] \
    [--max-concurrency N] [--max-decoded-bytes N] [--max-output-bytes N] [--timeout S]
```

- 成功退出码 0，stdout 打印来源清单与输出摘要；`--out`/`--data-out` 原子落盘。
- 区域失败退出码 2，超上限退出码 3；两种失败都不产生正式结果文件。
