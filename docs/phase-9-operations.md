# 阶段 9：自动化、性能和运维

阶段 9 为本地同步任务补充结构化日志、运行指标、健康检查、完整性检查、备份、
Parquet compact 和外部调度计划。调度器只生成可审查的 cron/launchd 参数，不在
MCP 进程中启动常驻线程。

## 本地查询运行参数

MCP 查询连接默认使用 2 个 DuckDB worker、最多 8 个连接，每连接内存上限为
`1536MB`，临时文件写入 `/tmp/arxiv-papers-duckdb`。可通过环境变量调整：

- `ARXIV_DUCKDB_QUERY_THREADS` / `ARXIV_DUCKDB_BUILD_THREADS`
- `ARXIV_DUCKDB_QUERY_MEMORY` / `ARXIV_DUCKDB_BUILD_MEMORY`
- `ARXIV_DUCKDB_TEMP_DIR`
- `ARXIV_MCP_MAX_CONNECTIONS` / `ARXIV_MCP_POOL_TIMEOUT`
- `ARXIV_MCP_PAPER_CACHE_BYTES` / `ARXIV_MCP_SEARCH_CACHE_SIZE`
- `ARXIV_SYNC_CHECKPOINT_PAGES` / `ARXIV_SYNC_CHECKPOINT_RECORDS`

查询快照必须包含 DuckDB FTS 扩展。构建快照时会自动加载或安装匹配版本的
扩展；安装失败会终止构建，不再静默降级到 `ILIKE` 全表搜索。查询进程只加载
扩展，不会在请求期间安装扩展或访问网络。

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

## 历史数据修复

旧版本曾在单条记录解析失败后继续提交页面，也曾把同一论文版本的新旧作者、分类
关联一起保留。升级后的同步会阻止新的缺失和旧关联继续累积，但不会自动修复已经
发布的快照。最稳妥的方式是在新目录重新完整同步：

```bash
uv run arxiv-papers sync full --data-dir data-cs-2023-rebuilt --no-progress
uv run arxiv-papers sync verify --data-dir data-cs-2023-rebuilt --sync-name full
uv run arxiv-papers ops health --data-dir data-cs-2023-rebuilt --sync-name full
```

命令沿用默认的 `cs` OAI set、`arXivRaw` 元数据和 2023-01-01 起的本地创建日期
过滤。确认新目录的 `verify` 和 `health` 均为 `ok: true` 后，把 MCP 服务及每日增量
同步的 `--data-dir` 改为 `data-cs-2023-rebuilt`。旧目录保留作核对，不要在同步中
切换目录。

新版快照 schema 为 `6`。旧快照仍可只读查询，但新版增量同步会拒绝旧 schema
基线；不要直接对旧数据目录运行增量命令。新目录完成全量同步后，增量默认扫描
全局 OAI 变更并应用本地 `cs.` 分类过滤，下载量会高于旧的 `set=cs` 增量扫描。

若只修复少量已知论文，解析失败的 ID 和 `raw_xml_path` 可从
`state/sync.duckdb` 的 `dead_letters` 表取得；旧关联污染则需要先确定受影响的
论文 ID。用 `RawXMLArchive.read_response()` 解压归档页，再通过
`OAIClient._parse_page()` 和 `parse_oai_record()` 解析。必须选出该论文最新的 OAI
记录，重新生成其 Parquet 关系行，用 `DuckDBSnapshotBuilder` 基于当前快照构建新
快照，并在验证后更新快照指针和同步状态。当前 CLI 没有归档重放命令；只手工改
DuckDB 的作者或分类表会遗漏 `paper_details` 和搜索索引，因此不建议直接修改现有
快照。

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

## 性能边界

第一版继续使用 DuckDB 的结构化索引布局和全文搜索表，查询只读本地快照。阶段 9
没有同时引入 Tantivy，避免同步维护两套索引。应使用真实数据定期测量关键词、作者、
分类和 ID 查询的延迟；只有当 DuckDB FTS 在目标规模下无法满足延迟或并发要求时，
才单独评估 Tantivy。
