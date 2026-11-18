# 镜头目录同步服务 (Shot Catalog Synchronization Service)

一个完全本地运行、不访问任何外部站点的镜头目录同步系统。它从一个**本地 HTTP 分页日志源**
读取镜头提交历史（日志会乱序、重复、分页重叠、偶发失败），用 **Python + DuckDB** 把源端
历史解释成一个可查询、可崩溃恢复的镜头目录，并提供 HTTP 查询接口。

---

## 1. 它解决什么问题

源端日志只是“按某种顺序投递的提交记录”，投递过程不可靠：

* **重复投递**：同一条记录可能出现多次（同一页内或跨页）；
* **分页重叠 / 到达顺序变化**：记录可能乱序到达，页与页之间可能重叠；
* **新旧两种工作站版本并存**：旧版以**秒**记时长、没有修复说明；新版以**毫秒**记时长、
  可带修复说明；两种版本会交错修订同一条镜头；
* **部分修订**：一次修订可只改部分属性，可把镜头说明清空而不动其它属性；
* **撤回 / 重新发布**：撤回期间镜头不可检索，重新发布后恢复可检索；
* **跨页大批量修订**：一次资料修复可同批修订十万条镜头，事务跨越几千个日志页，必须整体可见；
* **进程被杀**：重启后要从持久状态继续，且已保存的进度与查询结果对应同一段完整历史。

目录的唯一事实来源是每条记录的 **`seq`（源端提交顺序）**，与网络投递顺序无关。

---

## 2. 日志格式（本项目自定义并解释）

日志在源端是**换行分隔的 JSON（JSONL）**，每条记录在源端有全局唯一、连续、单调递增的 `seq`。

### 2.1 公共字段

| 字段 | 含义 |
| --- | --- |
| `seq` | 整数，从 1 开始连续递增，源端提交顺序的唯一权威 |
| `event_id` | 字符串，全局唯一的幂等键（重复投递时保持不变） |
| `event_type` | `upsert` / `withdraw` / `republish` |
| `film_id` / `shot_id` | 影片与镜头标识 |
| `version` | 工作站 schema 版本：`1`（秒）或 `2`（毫秒） |
| `ts` | ISO-8601 UTC 提交时间 |
| `batch_id` | 可选。同一次资料修复的所有记录共享一个 `batch_id`，且在源端占据一段连续的 `seq`，可跨多页 |

### 2.2 三种事件

* `upsert`：携带部分/全部属性的修订。
  * **v1（秒）**：`duration_sec`（秒，整数或浮点），可带 `title/description/scene/camera`，**不能**带毫秒时长或修复说明。
  * **v2（毫秒）**：`duration_ms`（非负整数毫秒），可带 `title/description/scene/camera/repair_note`。
  * 任何属性（含时长）都可省略 → **部分修订**，省略的属性保持原值。
  * 属性显式写成 JSON `null` → **清空该属性**（例如 `"description": null` 清空说明，其它属性不变）。
* `withdraw`：无属性负载，镜头进入撤回状态，查询不可见。
* `republish`：无属性负载，解除最近一次撤回。

归一化规则（见 `src/shot_catalog/log_format.py`）：所有时长在落库前统一换算成**整数毫秒**；
`repair_note` 只可能由 v2 设置或清空，v1 修订不携带该属性，因此**旧版再次提交绝不会抹掉
既有的、本次未改变的信息**（修复说明、说明文字、相机等都按“最后一次显式出现”保留）。

示例：

```json
{"seq":1,"event_id":"evt-00000001","event_type":"upsert","film_id":"film-a","shot_id":"S001","version":1,"ts":"2026-11-01T00:00:01Z","payload":{"title":"开场全景","duration_sec":12,"camera":"ARRI-A"}}
{"seq":2,"event_id":"evt-00000002","event_type":"upsert","film_id":"film-a","shot_id":"S001","version":2,"ts":"2026-11-01T00:00:02Z","payload":{"duration_ms":12500,"repair_note":"修复时间码漂移"}}
{"seq":9,"event_id":"evt-00000009","event_type":"withdraw","film_id":"film-b","shot_id":"S010","version":2,"ts":"2026-11-01T00:00:09Z"}
```

---

## 3. HTTP 接口

系统有两个 HTTP 服务，**全部绑定 127.0.0.1，不访问外网**。

### 3.1 日志源（`log_store.py`，默认 8081）

| 方法/路径 | 说明 |
| --- | --- |
| `GET /health` | `{"status":"ok","total":N}` |
| `GET /meta` | 总记录数、建议页大小、文件路径 |
| `GET /logs?offset=0&limit=100` | 返回一页记录；**故意**乱序、含重复、按概率返回 500（可重试） |
| `GET /raw?seq=K` | 取单条原始记录（调试用，同步器不依赖它） |

`/logs` 响应：

```json
{"offset":0,"limit":100,"returned":131,"total":200004,"has_more":true,"records":[ ... ]}
```

### 3.2 查询服务（`server.py`，默认 8080）

| 方法/路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查 + 同步统计 |
| `GET /stats` | 暂存事件数、快照 seq、可见镜头数、影片数 |
| `GET /shot?film_id=F&shot_id=S` | 查询单个镜头；不存在或撤回中返回 `404` |
| `GET /film/<film_id>` | 返回该影片**完整镜头清单**，整个响应来自同一个一致快照 |

单镜头响应（时长永远是毫秒，修复说明为空时为 `null`）：

```json
{
  "film_id": "film-a", "shot_id": "S001", "version": 2,
  "last_event_seq": 18, "last_ts": "...",
  "title": "开场全景", "description": null,
  "duration_ms": 13100, "scene": "SC-01", "camera": "ARRI-A",
  "repair_note": "批量校准 13100ms"
}
```

影片清单响应带 `snapshot_seq`，标识这一整页答案对应的历史前缀：

```json
{"film_id":"film-a","snapshot_seq":24,"count":3,"shots":[ ... ]}
```

---

## 4. 目录如何在 DuckDB 中保持正确（核心设计）

持久状态全部在一个 DuckDB 文件里（`Catalog`，`src/shot_catalog/catalog.py`）：

* `event_staging`：归一化后的原始事件，按 `seq` 主键去重。页内重复在 Python 折叠（仅缓冲当前一页），
  跨页重复用基于主键的 anti-join 丢弃；是一条纯追加的高吞吐路径，**从不在 Python 里聚齐整批日志**。
* `shot_state`：每个镜头一行的当前状态（含 `withdrawn` 标志）。
* `catalog_current`：`shot_state WHERE NOT withdrawn` 的视图，撤回即从所有查询中消失，重新发布即恢复。
* `sync_state`：单行进度指针 `last_snapshot_seq`。

**提交组（commit group）**：普通记录各自成组（连续普通记录会合并刷盘以减少事务数）；
共享 `batch_id` 的连续记录构成**一个原子组**。一个组只有在其整段连续 `seq` 都已持久暂存、
且恰好接在已发布前缀之后时才可提交——**半批修订不可见，并会阻塞其后的所有组，直到整批到齐**。

每次 `apply_range` 是**一个 DuckDB 事务**：在库内用集合操作算出该 seq 区间内每个镜头的增量
（每个属性取“本区间内最后一次显式出现该属性的事件”的值），再 UPDATE/INSERT 到 `shot_state`，
并在同一事务里推进 `last_snapshot_seq`。因此：

* 部分修订：未出现的属性保留旧值；显式 `null` 能清空属性；
* 撤回/重发：按该镜头最后一次 withdraw/republish 决定可见性；
* 大批量修订：十万条镜头在**单个事务**内整体生效，不存在只发布一部分的中间状态；
* 进度与查询一致：进度指针和它描述的快照在同一事务提交。

测试里还内置了一个独立的“全前缀重建” oracle（`rebuild_mismatch_count`），
用另一条 SQL 路径从全部暂存事件重算目录，与增量状态逐行比对，应为 0。

### 崩溃恢复

每次提交依赖 DuckDB 的 WAL 持久化，批组边界额外 `CHECKPOINT`。进程被 `SIGKILL` 后，
重启打开同一个文件：`last_snapshot_seq` 指向最后一个完整提交的前缀，同步器从该前缀附近重新拉页
（重复投递本就幂等），继续追赶。已保存进度与查询结果始终对应同一段完整历史。
恢复过程中查询服务继续响应；每个请求在单一连接的锁内读到一个一致快照。

---

## 5. 目录结构

```
shot_catalog_project/
├── README.md
├── requirements.txt
├── run_tests.py                 # 隔离模式 pytest 引导（-I）
├── fixtures/                    # 自带夹具（生成的 JSONL 历史）
├── src/shot_catalog/
│   ├── log_format.py            # 日志格式、校验、v1/v2 归一化
│   ├── fixturegen.py            # 确定性夹具生成（含 10 万镜头历史）
│   ├── log_store.py             # 本地 HTTP 分页日志源（乱序/重复/500）
│   ├── catalog.py               # DuckDB 持久目录、提交组、增量事务、查询
│   ├── sync.py                  # 分页拉取、重试、同步循环、崩溃续跑
│   └── server.py                # HTTP 查询接口与进程入口
├── scripts/
│   ├── bootstrap.sh             # 依赖安装到独立 prefix
│   ├── start_service.sh         # 启动日志源 + 查询服务
│   ├── verify.sh                # 一键验证（80 秒预算）
│   ├── demo.py                  # 混合版本端到端演示
│   ├── demo_large_batch.py      # 跨页十万级同批修订演示
│   └── _sync_worker.py          # 恢复测试用的一次性同步 worker
└── tests/                       # 31 个自动化测试
```

---

## 6. 安装、启动、查询、恢复

### 6.1 安装依赖

目标环境没有系统 pip / venv 时，脚本会把依赖装进独立 prefix（不污染系统）：

```bash
scripts/bootstrap.sh            # 安装 duckdb==2.0.0 与 pytest==9.2.0
```

### 6.2 一键验证（安装依赖后，约 80 秒内）

```bash
scripts/verify.sh
```

它会运行覆盖全部必需场景的代表性测试、混合版本演示、跨页同批修订演示，并打印可读的分节结果。
完整 31 项测试可随时单独运行：

```bash
python3 -I run_tests.py -q
```

### 6.3 启动服务

```bash
scripts/start_service.sh                          # 用自带混合历史
# 或指定历史与数据库文件
scripts/start_service.sh fixtures/mixed_history.jsonl run/catalog.duckdb
```

### 6.4 查询

```bash
curl -s 'http://127.0.0.1:8080/shot?film_id=film-a&shot_id=S001'
curl -s 'http://127.0.0.1:8080/film/film-a'
curl -s 'http://127.0.0.1:8080/stats'
```

### 6.5 可复跑演示

```bash
python3 -I scripts/demo.py                    # 混合版本/撤回/恢复，小而全
python3 -I scripts/demo_large_batch.py        # 完整十万镜头同批修订
python3 -I scripts/demo_large_batch.py 3000   # 缩小规模快速演示
```

### 6.6 终止后恢复（手动演示）

```bash
# 直接对同一个 db 文件反复运行 worker；中途 Ctrl-C / kill 后再次运行即可续跑
python3 -I scripts/_sync_worker.py run/catalog.duckdb http://127.0.0.1:8081 100 0
```

---

## 7. 自动化测试覆盖的场景

| 需求 | 对应测试 |
| --- | --- |
| 乱序 / 重复 / 重叠分页投递 | `test_sync_semantics.py`、`conftest.py`（高混乱率日志源）、`test_idempotent_redelivery_does_not_duplicate` |
| 混合版本修订、秒→毫秒、修复说明保留 | `test_log_format.py`、`test_v1_revision_cannot_erase_v2_repair_note`、`test_durations_always_milliseconds` |
| 部分修订与清空说明 | `test_explicit_null_clears_but_still_present`、`test_explicit_description_clearing` |
| 撤回后重新发布 | `test_withdrawn_shot_is_unretrievable`、`test_republished_shot_is_retrievable` |
| 跨页大批修订的原子性 | `test_batch_atomicity.py`（半批不可见、整批一次可见、跨 15+ 页） |
| 终止后恢复 | `test_reopen_continues_from_durable_prefix`、`test_sigkill_then_restart_resumes`（真实子进程 SIGKILL） |
| 同步期间查询一致 | `test_http_api.py::test_concurrent_reads_while_syncing`、`test_film_listing_is_one_consistent_snapshot` |

夹具（`fixturegen.py`）是真实生成的提交历史：v1 与 v2 记录交错修订同一批镜头、
携带真实不同的秒/毫秒字段与修复说明，而不是给日志贴不同版本标签。
