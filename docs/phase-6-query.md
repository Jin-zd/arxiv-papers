# 阶段 6：DuckDB 查询层

阶段 6 已实现本地 DuckDB 查询服务，查询过程只打开本地快照，不创建 HTTP 客户端，也不会请求 arXiv。

## 查询入口

```python
from arxiv_papers.query import DuckDBQueryService

with DuckDBQueryService("data-cs-2023/duckdb/library-<run-id>.duckdb") as query:
    page = query.search_papers(
        query="retrieval",
        category="cs.AI",
        created_from="2023-01-01",
        limit=20,
    )
    paper = query.get_paper("2401.00001")
    by_doi = query.get_paper_by_doi("10.1234/example")
```

已提供的能力：

- `search_papers`：标题、摘要、comments、journal reference 关键词检索，可组合作者、分类和创建/更新时间过滤；
- `search_by_author`：按标准化作者显示名检索；
- `search_by_category`：精确分类检索，分类以 `.` 结尾时按前缀检索；
- `get_paper`：按 arXiv ID 返回当前 metadata、作者、分类和版本事实；
- `get_paper_by_doi`：大小写不敏感的精确 DOI 查询；
- 所有列表查询默认按相关度、更新时间、arXiv ID 稳定排序，`limit` 最大为 100；
  使用 keyset position 翻页，不执行深度 `OFFSET` 和精确总数统计。

列表结果只来自 `papers` 当前版本表，因此不会返回已被 OAI-PMH deleted record 软删除的论文。`get_paper` 的版本列表来自 `paper_versions`，保留同步到本地的历史版本事实。传入历史版本时，论文主体仍返回当前 metadata，同时通过 `selected_version` 返回指定版本事实。

## 索引和全文检索

快照构建为 arXiv ID、规范化 DOI、作者 key、论文-作者关联、分类和论文-分类关联建立索引。作者名包含搜索使用子串匹配，不依赖 B-tree 名称索引。

全文检索直接在 `papers` 的 `title`、`abstract`、`comments` 和 `journal_ref` 字段上建立 `fts_main_papers` BM25 索引，不再保存第二份 `paper_search` 文本表。构建快照时会加载或安装 `fts` 扩展，并使用 Porter 词干化、英文停用词、大小写归一化和 accent stripping。

FTS 扩展只在构建快照时加载或安装，构建失败会终止并保留旧快照。查询层只加载快照声明的扩展，不会在 MCP 请求期间安装扩展或访问网络；扩展缺失时返回明确错误。

## 快照 schema

当前查询快照使用 schema `6`，并要求包含物化的 `paper_details` 与 `excluded_records` 表。`get_paper` 和批量详情查询使用一次详情查询路径。schema `4/5` 快照仍可只读查询，但不能作为新版增量同步的基线。

要使用 schema `6` 的范围退出和作者语义，需要在新数据目录重新完整同步。
