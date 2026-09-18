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
data/state/sync.duckdb
```

`papers` 是去重、软删除后的当前论文视图；`paper_versions` 保留所有版本历史。
增量同步会继承最近一次完成同步的 metadata prefix、set、创建时间范围和分类范围，
并将这些参数写入 checkpoint，避免恢复时混用不同 scope。

## 常用命令

```bash
uv run arxiv-papers sync full --data-dir data-cs-2023
uv run arxiv-papers sync incremental --data-dir data-cs-2023
uv run arxiv-papers sync status --data-dir data-cs-2023
uv run arxiv-papers sync verify --data-dir data-cs-2023
```

修改创建时间或分类范围后，应使用新的 data 目录重新执行全量同步。旧的
`arXiv` metadata 数据库不包含 `arXivRaw` 的版本历史，不能直接补齐历史版本。
