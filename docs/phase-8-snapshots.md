# 阶段 8：查询快照与并发安全

阶段 8 已实现查询快照发布和 MCP generation 切换。

## 快照发布

同步器先在同一 `duckdb/` 目录生成临时 DuckDB 文件，完成表、索引和 smoke test 后原子替换为 `library-<run-id>.duckdb`。随后 `SnapshotCatalog` 对快照执行只读校验，并通过临时 JSON 文件、`fsync` 和 `os.replace` 原子更新：

```text
data-cs-2023/duckdb/current.json
```

指针保存 generation、快照文件名、发布时间和记录计数。指针只引用同目录文件名，读取时不会接受绝对路径或目录穿越路径。

## MCP 并发行为

默认启动方式使用 `current.json`：

```bash
uv run arxiv-papers mcp --data-dir data-cs-2023
```

每个 MCP 工具请求都会在查询锁内检查当前 generation。如果同步器已经发布新快照，MCP 会先只读打开新快照，打开成功后再替换旧连接；打开失败则继续使用旧连接。已经开始的查询不会读到半构建文件。

显式传入 `--snapshot` 时，MCP 固定查询指定文件，不跟随 generation 切换。

## 保留和回滚

同步器默认保留最近 3 个 DuckDB 快照，并始终保护当前指针引用的快照。旧快照删除失败不会影响新快照发布，因此在支持文件句柄延迟删除的平台上仍可继续保留旧文件。新快照构建、校验或指针发布失败时，旧 `current.json` 不会被覆盖，MCP 继续使用旧 generation。
