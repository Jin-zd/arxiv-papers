# arXiv Papers

一个本地 arXiv 元数据同步与检索工具。

项目通过 arXiv OAI-PMH `arXivRaw` 接口下载元数据，保存论文当前信息、版本历史、作者、分类和 DOI，并构建 DuckDB 查询快照。快照可以通过命令行、Python API 或 MCP 提供给本地工具使用。

项目只同步元数据，不下载论文 PDF。

## 功能

- 全量同步 arXiv 元数据，支持断点续传
- 增量同步最近发生变化的记录
- 保存论文版本历史、提交时间、源码大小和 source type
- 支持标题、摘要、comments、journal reference 全文检索
- 支持作者、分类、创建日期和更新时间过滤
- 支持按 arXiv ID、DOI 和历史版本查询
- 生成不可变 DuckDB 快照，支持 MCP 只读查询
- 保存原始 OAI XML，便于故障排查和重新解析

## 环境要求

- Python 3.12 或更高版本
- [`uv`](https://docs.astral.sh/uv/)
- 能够访问 `https://oaipmh.arxiv.org/oai`

安装依赖：

```bash
uv sync
```

也可以直接使用 `uv run`，它会自动使用项目环境：

```bash
uv run arxiv-papers --help
```

## 默认同步范围

默认全量同步使用以下范围：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `metadataPrefix` | `arXivRaw` | 使用包含版本历史的元数据格式 |
| `set` | `cs` | 全量请求优先使用 arXiv CS 集合 |
| `created_from` | `2023-01-01` | 本地保留 v1 提交日期不早于该日期的论文 |
| 分类前缀 | `cs.` | 保留当前分类中至少包含一个 `cs.*` 的论文 |

`created_from` 和分类是本地过滤条件。OAI `datestamp` 表示记录修改时间，不能代替论文首次提交时间。

## 首次全量下载

建议使用新的数据目录执行首次同步：

```bash
uv run arxiv-papers sync full \
  --data-dir data-cs-2023
```

默认会显示 `tqdm` 进度条。同步完成后会输出 JSON 结果，并生成当前查询快照。

如果网络需要代理：

```bash
uv run arxiv-papers sync full \
  --data-dir data-cs-2023 \
  --proxy http://127.0.0.1:7890
```

如果同步中断或失败，重新运行**相同的命令和参数**会从已保存的 checkpoint 继续。同步正常完成后再次运行 `sync full` 会创建新的全量任务并重新下载，这是全量命令的预期行为。

因此，全量同步完成后不要重复运行 `sync full`，日常更新应使用增量同步。

关闭终端进度条：

```bash
uv run arxiv-papers sync full \
  --data-dir data-cs-2023 \
  --no-progress
```

保存完整 JSONL 日志：

```bash
uv run arxiv-papers sync full \
  --data-dir data-cs-2023 \
  --log-file data-cs-2023/logs/full.jsonl
```

## 增量同步

全量同步成功后，使用以下命令获取后续变化：

```bash
uv run arxiv-papers sync incremental \
  --data-dir data-cs-2023
```

增量同步会根据上一次成功扫描的时间点计算起始日期，并保留少量重叠时间，避免遗漏边界记录。默认不限制 OAI set，以便发现论文退出本地 `cs.` 范围的变化；本地创建日期和分类过滤仍然生效。

增量同步中断后，重新执行相同命令即可恢复：

```bash
uv run arxiv-papers sync incremental \
  --data-dir data-cs-2023
```

常用参数：

```bash
# 覆盖同步起始日期
uv run arxiv-papers sync incremental \
  --data-dir data-cs-2023 \
  --from 2026-09-01

# 覆盖本地分类过滤
uv run arxiv-papers sync incremental \
  --data-dir data-cs-2023 \
  --category-prefix cs.AI

# 指定 OAI set。缩小 set 可能看不到退出该 set 的论文变化
uv run arxiv-papers sync incremental \
  --data-dir data-cs-2023 \
  --set cs
```

## 查看状态与验证

```bash
# 查看同步状态、checkpoint 和批次
uv run arxiv-papers sync status --data-dir data-cs-2023

# 验证全量同步
uv run arxiv-papers sync verify \
  --data-dir data-cs-2023 \
  --sync-name full

# 检查同步和当前快照
uv run arxiv-papers ops health \
  --data-dir data-cs-2023 \
  --sync-name full

# 检查 Parquet 文件和快照完整性
uv run arxiv-papers ops integrity --data-dir data-cs-2023
```

## MCP 配置

项目提供一个基于 STDIO 的只读 MCP 服务。MCP 服务不会访问 arXiv，也不会执行任意 SQL，只查询本地 DuckDB 快照。

### 直接启动

```bash
uv run arxiv-papers mcp \
  --data-dir data-cs-2023
```

默认读取数据目录中的当前快照指针。也可以固定到一个具体快照：

```bash
uv run arxiv-papers mcp \
  --snapshot data-cs-2023/duckdb/library-full-<run-id>.duckdb
```

### Codex Desktop 配置
在 config.toml 中加入：
```toml
[mcp_servers.arxiv_local]
command = "绝对路径/uv"
args = [
  "run",
  "arxiv-papers",
  "mcp",
  "--data-dir",
  "绝对路径/arxiv papers/data-cs-2023",
]
cwd = "绝对路径/arxiv papers"
startup_timeout_sec = 30
tool_timeout_sec = 120
enabled = true

enabled_tools = [
  "search_papers",
  "get_paper",
  "search_by_author",
  "search_by_category",
  "library_status",
]
```

如果不使用 uv，可以更改 `command` 为 Python 解释器路径，并在 `args` 中加入 `-m arxiv_papers mcp --data-dir ...`。


### MCP 提供的工具

- `search_papers`：关键词、作者、分类和日期过滤
- `search_by_author`：按作者名检索
- `search_by_category`：按分类检索
- `get_paper`：查询一篇论文及其版本事实
- `get_papers`：批量查询最多 100 篇论文
- `library_status`：查看本地快照计数和搜索状态

快照发布后，MCP 会在后续请求中自动切换到新的 generation。正在执行的查询会继续使用旧快照，避免读到半成品文件。

## 数据目录结构

```text
data-cs-2023/
├── duckdb/
│   ├── current.json                 # 当前快照指针
│   └── library-<run-id>.duckdb      # 不可变查询快照
├── parquet/
│   ├── full/<run-id>/               # 全量同步中间数据
│   └── incremental/<run-id>/        # 增量同步中间数据
├── raw/                             # 压缩保存的 OAI XML 响应
└── state/
    └── sync.duckdb                  # checkpoint 和运行状态
```

元数据快照包含 `papers`、`paper_versions`、`authors`、`paper_authors`、`categories`、`deleted_records` 和 `excluded_records` 等表。作者原始文本保存在 `authors_raw`；从非结构化作者字符串拆出的姓名会标记为启发式结果。

## Python 查询 API

```python
from arxiv_papers.query import DuckDBQueryService

snapshot = "data-cs-2023/duckdb/library-<run-id>.duckdb"

with DuckDBQueryService(snapshot) as query:
    page = query.search_papers(
        query="retrieval",
        category="cs.AI",
        created_from="2023-01-01",
        limit=20,
    )
    paper = query.get_paper("2401.00001")
    by_doi = query.get_paper_by_doi("10.1234/example")
```

查询只读取本地快照，不创建网络连接。

## 备份与定时任务

创建备份：

```bash
uv run arxiv-papers ops backup \
  --data-dir data-cs-2023 \
  --output-dir backups
```

备份原始 OAI XML：

```bash
uv run arxiv-papers ops backup \
  --data-dir data-cs-2023 \
  --output-dir backups \
  --include-raw
```

生成每日增量同步的 cron/launchd 计划：

```bash
uv run arxiv-papers ops schedule \
  --data-dir data-cs-2023 \
  --hour 2 \
  --minute 0
```

## 开发与测试

```bash
uv run pytest -q
uv run ruff check src tests
uv run pyright --pythonpath .venv/bin/python
```

## 许可证与数据来源

本项目通过 arXiv OAI-PMH 获取元数据。论文元数据的使用应遵守 arXiv 的服务条款和各条记录提供的许可信息。项目代码的许可证以仓库后续提供的许可证文件为准。