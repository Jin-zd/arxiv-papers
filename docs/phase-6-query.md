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

快照构建会建立以下结构化索引：arXiv ID、DOI、创建日期、更新时间、作者 key、标准化作者名、论文-作者关联、分类和论文-分类关联。

全文检索数据集中在 `paper_search` 表，字段为 `title`、`abstract`、`comments` 和 `journal_ref` 的拼接内容。若当前 DuckDB 环境已经安装 `fts` 扩展，快照会创建 `fts_main_paper_search` BM25 索引，并使用 Porter 词干化、英文停用词、大小写归一化和 accent stripping。

如果扩展不可用，构建不会失败，查询层会自动回退到这四个字段的 `ILIKE` 检索。回退模式仍然完全本地执行；`search_metadata` 中的 `fts_enabled` 记录当前快照采用的模式。

## 增量基线兼容

阶段 6 将快照 schema 更新为 `3`。已有阶段 5 的 schema `2`（已包含 `arXivRaw` 版本历史）可以作为增量同步基线；下一次构建会生成 schema `3` 并加入查询索引。旧快照本身也可以由 `DuckDBQueryService` 以 ILIKE 回退模式读取。
