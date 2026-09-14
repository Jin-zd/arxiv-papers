# 本地 arXiv 论文检索 MCP 服务：任务拆解与分步实现方案

## 1. 项目目标

构建一个本地 arXiv 论文 metadata 数据库和 MCP Server：

- 使用 arXiv 官方 OAI-PMH 作为唯一同步数据源；
- 首次运行支持全量同步，后续支持增量同步；
- 支持断点续传、去重、错误重试和结构化日志；
- 使用 DuckDB + Parquet 保存和查询本地数据；
- 提供适合标题、摘要和关键词的全文检索；
- 通过 STDIO 接入 Codex；
- MCP 日常查询只访问本地数据库，不请求 arXiv 网络接口。

本项目第一阶段只处理论文 metadata，不下载 PDF，也不构建 PDF 全文索引。

## 2. 总体架构

```text
arXiv OAI-PMH
      |
      v
同步 CLI / Sync Worker
  请求、限速、重试、断点、解析、去重
      |
      +--> Raw XML Archive
      |
      +--> Parquet Data Lake
      |
      v
DuckDB Staging / Query Snapshot
      |
      v
只读 MCP Server（STDIO）
      |
      v
Codex
```

同步器和 MCP Server 必须分离：同步器负责网络访问和写入，MCP Server 只负责本地只读查询。

## 3. 推荐技术栈

| 模块 | 技术选择 | 用途 |
|---|---|---|
| 语言 | Python 3.12+ | 同步器、数据处理和 MCP Server |
| HTTP | httpx | OAI-PMH 请求、超时和连接管理 |
| XML | lxml | 解析 OAI-PMH 和 arXiv metadata |
| 数据模型 | Pydantic 2 | 解析结果校验和内部 DTO |
| 查询数据库 | DuckDB | 结构化查询、聚合和全文查询 |
| 列式存储 | PyArrow + Parquet | 长期数据存储和重建源 |
| MCP | MCP Python SDK / FastMCP | STDIO Server 和工具定义 |
| CLI | Typer | 同步、重建、校验和状态管理 |
| 重试 | Tenacity 或等价实现 | 指数退避、Retry-After 和有限重试 |
| 日志 | Python logging + JSON Formatter | 同步批次和错误日志 |
| 测试 | pytest | 单元、集成和恢复测试 |
| 项目管理 | uv + pyproject.toml | 依赖和命令管理 |

全文检索第一版使用 DuckDB FTS。若数据规模或查询延迟无法满足要求，再单独引入 Tantivy；不要在第一版同时维护两个全文索引系统。

## 4. 数据目录设计

建议使用以下目录布局：

```text
data/
  raw/
    YYYY-MM-DD/
      run-<id>/
        response-<batch>.xml.zst
        response-<batch>.json
  parquet/
    papers/
    paper_versions/
    authors/
    paper_authors/
    categories/
    sync_batches/
  duckdb/
    staging.duckdb
    library-<generation>.duckdb
    current.json
  logs/
    sync.jsonl
  state/
    sync-state.json
```

`raw/` 和 `parquet/` 是可重建的数据层；`duckdb/` 是服务查询层；`current.json` 指向当前可用的查询快照。

## 5. 数据模型任务

### 5.1 论文表 `papers`

保存论文级别的当前状态：

- canonical arXiv ID，不包含版本号；
- 当前版本号；
- 当前标题和摘要；
- 创建日期；
- 最新更新时间；
- primary category；
- DOI；
- journal reference；
- comments；
- license；
- report number；
- OAI identifier；
- OAI datestamp；
- 删除状态；
- 首次同步时间和最后同步时间。

### 5.2 版本表 `paper_versions`

保存每个 arXiv 版本：

- arXiv ID；
- version；
- version date；
- 标题；
- 摘要；
- metadata hash；
- 对应的 OAI datestamp；
- 原始 XML 文件位置。

### 5.3 作者表

`authors` 保存标准化作者名和原始作者名，`paper_authors` 保存论文、作者、作者顺序和 affiliation 的关系。

作者只做搜索用途，不在第一版尝试判断两个同名作者是否为同一个人。

### 5.4 分类表

`categories` 保存：

- arXiv ID；
- version；
- 分类代码；
- 是否 primary category。

分类应按版本保存，同时提供当前版本的查询视图。

## 6. 任务拆解

### 阶段 0：项目初始化

任务：

1. 初始化 Python 项目和 `pyproject.toml`。
2. 固定 Python、DuckDB、MCP SDK 等核心依赖版本范围。
3. 建立 `src/`、`tests/`、`docs/` 和数据目录配置。
4. 增加统一配置加载机制，支持 TOML、环境变量和 CLI 参数。
5. 增加基础日志初始化。
6. 增加 lint、格式化和 pytest 命令。

验收标准：

- 空项目可以安装依赖；
- CLI 可以显示版本和配置路径；
- 测试框架可以执行一个基础测试；
- 日志可以输出到标准输出和文件。

### 阶段 1：OAI-PMH 客户端

任务：

1. 实现 OAI-PMH endpoint 配置。
2. 支持 `ListRecords`。
3. 使用 arXiv metadata 格式，而不是逐篇调用 `GetRecord`。
4. 解析 `resumptionToken`。
5. 支持 `from`、`until` 和可选 set。
6. 实现连接超时、读取超时和响应大小限制。
7. 实现 HTTP 429、5xx、网络错误的重试。
8. 记录请求参数、响应状态、耗时和批次编号。
9. 处理 OAI-PMH XML 错误响应。

验收标准：

- 能从固定的测试 XML 解析多条记录；
- 能连续处理多个 resumption token；
- 临时网络错误可以自动重试；
- OAI 错误不会被误当成空结果；
- 所有请求使用明确的 User-Agent 和联系人信息。

### 阶段 2：Raw XML 和标准化解析

任务：

1. 每个成功响应先保存 Raw XML。
2. 保存请求元数据和响应校验值。
3. 解析 OAI header 和 arXiv metadata。
4. 标准化标题、摘要和 comments 的空白字符。
5. 统一 arXiv ID 和 version 的表示。
6. 保留原始 OAI identifier。
7. 解析作者顺序、分类、DOI 和 journal reference。
8. 记录缺失字段和格式异常。
9. 为每条记录生成 metadata hash。

验收标准：

- 能正确解析新格式和旧格式 arXiv ID；
- 缺失 DOI 或 journal reference 不会导致整批失败；
- XML 特殊字符、换行和多余空格处理一致；
- 解析结果可以由 Raw XML 重建。

### 阶段 3：首次全量同步

任务：

1. 实现 `sync full` CLI 命令。
2. 使用 `ListRecords` 顺序处理所有页面。
3. 每批请求响应先写 Raw XML。
4. 将标准化记录写入 staging 数据集。
5. 使用 `(paper_id, version)` 和 metadata hash 去重。
6. 处理 OAI deleted record，采用软删除。
7. 保存批次计数和同步耗时。
8. 支持中断后从最近成功批次恢复。
9. 初次同步结束后构建 DuckDB 查询快照。

验收标准：

- 同一批次重复执行不会产生重复论文；
- 进程在任意批次退出后可继续；
- 同步失败不会推进错误的 checkpoint；
- 全量同步结束后可以按 ID 查询论文；
- Parquet 和 DuckDB 中的记录数量可校验。

### 阶段 4：断点续传和同步状态

任务：

1. 设计 `sync_state` 和 `sync_batches` 表。
2. 保存当前 run、范围、最后成功 datestamp 和 resumption token。
3. 数据事务提交成功后才推进 token。
4. 处理 resumption token 失效。
5. token 失效时根据时间范围和重叠窗口重新获取。
6. 增加 dead-letter 记录，保存无法处理的响应或记录。
7. 增加 `sync status` 和 `sync verify` CLI 命令。

验收标准：

- 可以模拟进程被杀后恢复；
- token 失效时不会静默跳过数据；
- checkpoint 与已提交数据保持一致；
- 错误批次可以单独定位和重新处理。

### 阶段 5：增量同步

任务：

1. 实现 `sync incremental` CLI 命令。
2. 从上一次成功 datestamp 回退 1 至 3 天作为 overlap。
3. `until` 使用当前 UTC 时间减去 safety lag。
4. 对重叠范围内的记录执行幂等 upsert。
5. 处理新论文、新版本、metadata 更新和删除状态。
6. 更新当前论文视图和版本视图。
7. 保留同步前后统计差异。
8. 支持指定日期范围进行补同步。

验收标准：

- 连续执行两次增量同步不会重复数据；
- 新版本会更新当前版本，同时保留旧版本；
- 论文删除状态可以被本地查询层识别；
- 增量失败时仍保留上一版可用查询快照。

### 阶段 6：DuckDB 查询层和索引

任务：

1. 建立论文、版本、作者、分类和关联表。
2. 建立当前版本视图。
3. 建立作者标准化搜索字段。
4. 建立 DOI、arXiv ID、日期和分类的结构化索引或排序布局。
5. 建立 title、abstract、comments、journal reference 的全文索引。
6. 对全文索引配置英文分词、词干化和停用词策略。
7. 设计稳定排序：相关度、更新时间和 arXiv ID。
8. 为快照构建后执行查询 smoke test。

验收标准：

- 关键词可以搜索标题和摘要；
- 作者和分类过滤不依赖全文搜索；
- 精确 ID 和 DOI 查询足够快；
- 查询结果不会混入软删除记录；
- 快照重建结果与 Parquet 数量一致。

### 阶段 7：MCP Server

任务：

1. 实现 STDIO MCP Server 启动入口。
2. 只读打开当前 DuckDB 查询快照。
3. 禁止任意 SQL 执行工具。
4. 实现 `search_papers`。
5. 实现 `get_paper`。
6. 实现 `search_by_author`。
7. 实现 `search_by_category`。
8. 可选实现 `library_status`。
9. 为所有工具定义参数、默认值、最大 limit 和错误格式。
10. 使用 cursor 分页，而不是依赖不稳定的深度 offset 分页。
11. MCP Server 内禁止请求网络。

建议工具行为：

| 工具 | 核心参数 | 主要能力 |
|---|---|---|
| `search_papers` | query、author、category、日期、limit、cursor | 全文和组合过滤 |
| `get_paper` | arxiv_id、version | 论文详情和版本列表 |
| `search_by_author` | author、limit、cursor | 作者名搜索 |
| `search_by_category` | category、primary_only、limit、cursor | 分类搜索 |
| `library_status` | 无 | 本地数据库和同步状态 |

验收标准：

- MCP Server 可以通过 STDIO 启动；
- Codex 可以发现并调用所有工具；
- MCP 查询期间不会访问 arXiv；
- 查询结果包含用户需要的完整 metadata；
- 参数错误可以得到可读的错误信息；
- limit 有上限，避免一次返回过多数据。

### 阶段 8：查询快照与并发安全

任务：

1. 同步器写入 staging 数据库。
2. 构建新的 `library-<generation>.duckdb`。
3. 完成数据和索引校验后更新 `current.json`。
4. MCP 在请求边界检查当前 generation。
5. 新快照发布失败时继续使用旧快照。
6. 保留有限数量的旧快照，支持回滚和诊断。

验收标准：

- 同步过程中 MCP 仍可正常查询；
- MCP 不会读到半构建的数据库；
- 新快照损坏时旧快照仍可用；
- 快照切换过程不会产生空库。

### 阶段 9：自动化、性能和运维

任务：

1. 增加每日增量同步调度方式。
2. 增加同步超时和失败告警状态。
3. 增加结构化 JSON 日志。
4. 增加同步指标：请求数、重试数、记录数、更新数和失败数。
5. 增加数据完整性检查。
6. 增加 Parquet 和 DuckDB 备份策略。
7. 对大 Parquet 文件进行定期 compact。
8. 测试百万级数据下的全文查询和分类过滤。
9. 根据实际延迟决定是否引入 Tantivy。

## 7. 测试计划

### 单元测试

- OAI-PMH XML 解析；
- resumptionToken 处理；
- arXiv ID 和 version 规范化；
- 作者和分类解析；
- 日期、DOI 和 journal reference 解析；
- metadata hash 和去重；
- retry/backoff 行为；
- 删除记录处理。

### 集成测试

- 使用本地 fixture 模拟 OAI-PMH 分页；
- 模拟网络超时、429、503 和 XML 错误；
- 模拟进程在批次提交前退出；
- 模拟 token 失效并恢复；
- 从 Raw XML 重建 Parquet；
- 从 Parquet 重建 DuckDB；
- MCP STDIO 工具调用。

### 验收测试

- 全量同步后可以按 ID、作者、分类和关键词查询；
- 增量同步不会产生重复记录；
- 新版本会保留版本历史；
- 查询期间不发生外部网络请求；
- MCP 返回结果稳定、可分页且字段完整。

## 8. 关键设计约束

1. OAI-PMH 是唯一同步入口，查询流程不调用 arXiv API。
2. MCP Server 不执行网络请求，不执行任意 SQL，不修改数据。
3. checkpoint 必须在数据成功提交后更新。
4. 所有同步操作必须幂等，可安全重跑。
5. Raw XML 必须保留，Parquet 和 DuckDB 必须可重建。
6. 论文级别和版本级别数据必须分开建模。
7. OAI deleted record 采用软删除。
8. 增量同步必须使用重叠时间窗口。
9. 查询结果默认只返回当前版本，详情工具可以返回完整版本历史。
10. 第一版只索引 metadata，不把 PDF 全文检索纳入核心范围。

## 9. 推荐实现顺序

```text
项目初始化
  -> OAI-PMH 客户端
  -> Raw XML 和解析模型
  -> 全量同步
  -> checkpoint 和恢复
  -> 增量同步
  -> Parquet / DuckDB 查询层
  -> DuckDB FTS
  -> MCP 工具
  -> 快照切换
  -> 自动调度和性能优化
```

每个阶段完成后都应保留可运行的中间结果，避免把首次全量同步、索引和 MCP 接入绑定成一个不可调试的大步骤。

## 10. 第一版完成定义

第一版可以定义为完成，当满足以下条件：

- 能从空目录完成全量 OAI-PMH metadata 同步；
- 中断后可以继续，不需要从头开始；
- 能按日期执行增量同步；
- 论文、版本、作者、分类、DOI 和 journal reference 数据完整；
- DuckDB 能执行全文和结构化组合查询；
- MCP 通过 STDIO 提供四个核心工具；
- MCP 查询不产生网络请求；
- 同步错误有重试、日志和可定位的失败状态；
- Parquet 和 Raw XML 可以重建查询数据库；
- 有自动化测试覆盖解析、断点、去重和 MCP 调用。
