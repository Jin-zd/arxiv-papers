# 阶段 7：本地 MCP Server

阶段 7 已实现 STDIO MCP Server。服务只读打开一个 DuckDB 快照，不访问 arXiv 网络，也不提供任意 SQL 执行工具。

## 启动

默认从数据目录的同步状态中选择最近完成的快照：

```bash
uv run arxiv-papers mcp --data-dir data-cs-2023
```

也可以显式指定快照：

```bash
uv run arxiv-papers mcp \
  --snapshot data-cs-2023/duckdb/library-<run-id>.duckdb
```

## MCP 工具

- `search_papers`：关键词、作者、分类和创建/更新时间组合检索；
- `get_paper`：按 arXiv ID 获取当前 metadata、作者、分类和版本事实；
- `search_by_author`：作者检索；
- `search_by_category`：分类精确或前缀检索；
- `library_status`：返回快照 schema、论文数量、版本数量和 FTS 状态。

列表工具使用不透明 `cursor` 分页。cursor 包含查询条件指纹，不能跨查询条件复用；服务端仍通过阶段 6 的稳定排序保证翻页结果一致。

工具返回 JSON 结构，日期和时间统一转换为 ISO 8601 字符串。无结果返回空 `items` 或 `paper: null`，参数错误会以 MCP tool error 返回。
