# 阶段 9：自动化、性能和运维

阶段 9 为本地同步任务补充结构化日志、运行指标、健康检查、完整性检查、备份、
Parquet compact 和外部调度计划。调度器只生成可审查的 cron/launchd 参数，不在
MCP 进程中启动常驻线程。

## 结构化日志与指标

同步命令默认把 JSON 日志写到 stderr。使用 `--log-file` 可以同时写入 JSONL 文件：

```bash
uv run arxiv-papers sync incremental \
  --data-dir data-cs-2023 \
  --log-file data-cs-2023/logs/incremental.jsonl
```

OAI 客户端记录请求数、重试数、响应字节数、传输错误和 HTTP 错误。同步结果中的
`stats` 还包括页面数、记录数、解析数、过滤数、删除数、解析错误数、token 恢复数
和耗时。日志和最终 JSON 结果都不包含论文全文或响应 XML 内容。

## 健康与完整性检查

```bash
uv run arxiv-papers ops health \
  --data-dir data-cs-2023 \
  --sync-name incremental

uv run arxiv-papers ops integrity --data-dir data-cs-2023
```

`health` 只读检查同步状态、`current.json` 和当前快照；同步状态为 `running` 或
`failed`、快照缺失、指针损坏都会报告 `ok: false`。`integrity` 会读取全部 Parquet
文件 metadata，检查当前快照所需表和快照计数，并报告每个 Parquet 表的文件数、可读
文件数和行数。

## 每日增量同步

推荐使用系统级调度器调用一次性 CLI。先生成计划：

```bash
uv run arxiv-papers ops schedule \
  --data-dir data-cs-2023 \
  --hour 2 \
  --minute 0
```

输出包含适用于当前项目目录的 cron 字符串，以及可直接用于 macOS `launchd`
`ProgramArguments` 的 JSON 数组。cron 示例：

```cron
0 2 * * * cd "/path/to/arxiv papers" && uv run arxiv-papers sync incremental --data-dir "/path/to/arxiv papers/data-cs-2023" --no-progress
```

任务失败时同步状态会保留为 `failed`，下一次相同 scope 的运行会按 checkpoint 恢复。
可由外部监控定期检查 `ops health` 的退出输出和 JSON 中的 `ok`、`issues` 字段。

## 备份

备份使用临时目录构建，完成后原子重命名；默认包含 `parquet`、`duckdb` 和 `state`，
不包含 raw XML：

```bash
uv run arxiv-papers ops backup \
  --data-dir data-cs-2023 \
  --output-dir backups
```

需要保留原始 OAI 响应时加入 `--include-raw`：

```bash
uv run arxiv-papers ops backup \
  --data-dir data-cs-2023 \
  --output-dir backups \
  --include-raw
```

备份目录类似：

```text
backups/
  arxiv-papers-20260920T020000Z/
    parquet/
    duckdb/
    state/
    raw/                 # 仅 include_raw=true 时存在
    manifest.json        # 每个文件的大小和 SHA-256
```

备份目标不能位于数据目录内，避免备份扫描自身或改变同步数据目录。

## Parquet compact

同步会为每个 OAI 页面写入 Parquet part。可以对一个已经完成的 run 合并成每个表一
个压缩文件：

```bash
uv run arxiv-papers ops compact \
  --source data-cs-2023/parquet/full/<run-id> \
  --output data-cs-2023/parquet/full/<run-id>-compact
```

compact 输出到新目录，不覆盖源数据，也不会自动替换当前 DuckDB 快照。替换快照前
应重新构建并通过现有 snapshot 校验流程。

## 查询 benchmark

可以对当前本地快照运行只读 benchmark：

```bash
uv run arxiv-papers ops benchmark \
  --data-dir data-cs-2023 \
  --iterations 5 \
  --limit 20
```

benchmark 自动从快照中选择代表性的关键词、作者、分类和 arXiv ID，输出每类查询的
最小、median、p95 延迟，以及结果数和是否存在下一页。它不访问网络、不修改快照，也
不设置依赖机器性能的硬阈值。

## 性能边界

第一版继续使用 DuckDB 的结构化索引布局和全文搜索表，查询只读本地快照。阶段 9
没有同时引入 Tantivy，避免同步维护两套索引。应使用真实数据定期测量关键词、作者、
分类和 ID 查询的延迟；只有当 DuckDB FTS 在目标规模下无法满足延迟或并发要求时，
才单独评估 Tantivy。
