# 当前同步范围与数据语义

## 默认范围

同步 CLI 默认使用以下参数：

```text
metadataPrefix = arXivRaw
set            = cs
created_from   = 2023-01-01
category       = cs.
```

`created_from` 是本地过滤条件，使用论文 `v1` 的提交时间判断，包含
`2023-01-01`。OAI-PMH 的 `datestamp` 表示记录最近修改时间，不能替代论文首次
提交时间，因此不能只依赖 OAI 的 `from` 参数完成这个过滤。

分类检查全部当前分类，只要存在一个 `cs.*` 分类就保留；不会只检查 primary
category。`set=cs` 用于缩小 OAI-PMH 数据源范围，最终范围仍由本地过滤器决定。

## arXivRaw 与版本历史

`arXivRaw` 提供当前论文 metadata 和版本历史。版本历史可靠包含：

- version；
- v1/vN 提交时间；
- 源码大小；
- source type。

当前论文的标题、摘要、作者、分类、DOI、journal reference 等保存在 `papers`。
版本历史保存在 `paper_versions`，每个版本一行。由于官方 `arXivRaw` 不提供每个
历史版本独立的标题、摘要、作者和分类，历史版本行不会复制当前字段或伪造旧版本
metadata；这些字段只在 `is_current = true` 的当前版本行提供。

## 存储语义

```text
data/parquet/full/<run-id>/paper_versions/
data/parquet/incremental/<run-id>/paper_versions/
data/duckdb/library-<run-id>.duckdb
data/duckdb/current.json
data/state/sync.duckdb
```

`papers` 是去重、软删除后的当前论文视图；`paper_versions` 保留所有版本历史。
增量同步会继承最近一次完成同步的 metadata prefix、set、创建时间范围和分类范围，
并将这些参数写入 checkpoint，避免恢复时混用不同 scope。

`duckdb/current.json` 是已完成查询校验的当前快照指针，由临时文件加原子替换发布。
MCP 默认在每个请求边界检查该指针并切换 generation；显式指定 `--snapshot` 时固定使用
指定文件。

状态数据库的表结构只在同步写入路径中初始化或迁移；`sync status`、`sync verify` 和
运维检查使用只读连接，不会与正在进行的同步任务争抢写锁。

## 常用命令

```bash
uv run arxiv-papers sync full --data-dir data-cs-2023
uv run arxiv-papers sync incremental --data-dir data-cs-2023
uv run arxiv-papers sync status --data-dir data-cs-2023
uv run arxiv-papers sync verify --data-dir data-cs-2023
```

阶段 9 运维命令：

```bash
uv run arxiv-papers ops health --data-dir data-cs-2023 --sync-name incremental
uv run arxiv-papers ops integrity --data-dir data-cs-2023
uv run arxiv-papers ops backup --data-dir data-cs-2023 --output-dir backups
uv run arxiv-papers ops compact \
  --source data-cs-2023/parquet/full/<run-id>
uv run arxiv-papers ops schedule --data-dir data-cs-2023 --hour 2 --minute 0
```

同步支持 `--log-file` 输出 JSONL 结构化日志；同步结果中的 `stats` 包含请求、重试、
响应字节数、错误数、记录计数和耗时。健康检查和完整性检查只读本地状态、Parquet
metadata 与当前 DuckDB 快照，不会请求 arXiv。详细运维策略见
`docs/phase-9-operations.md`。

查询列表使用 keyset cursor，不返回精确 total；snapshot generation 变化后旧 cursor
会失效。`get_paper` 支持查询指定历史版本，并在 `selected_version` 中返回版本事实。

如果终端使用的是本机代理但代理没有通过环境变量传给 Python，可以显式指定：

```bash
uv run arxiv-papers sync full \
  --data-dir data-cs-2023 \
  --proxy http://127.0.0.1:7890
```

同步开始请求每个 OAI 页面前会立即显示 `requesting OAI page ...`；页面完整下载、
解析并写入 checkpoint 后，才会更新记录百分比、已下载大小、速度和剩余大小估算。

修改创建时间或分类范围后，应使用新的 data 目录重新执行全量同步。旧的
`arXiv` metadata 数据库不包含 `arXivRaw` 的版本历史，不能直接补齐历史版本。
