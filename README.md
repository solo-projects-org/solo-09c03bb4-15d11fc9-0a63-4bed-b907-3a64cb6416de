# 岸站 AIS 可信航迹（离线分析）

FastAPI + SQLite 网页应用：海事数据员上传两处岸站收取的 AIS 定位 CSV 原档，
系统按船生成可信航迹、隔离不可信电文，并支持下载可信航迹与对应原档。
原档与判定**同一批次同时发布**；并发上传、写盘失败或服务重启后，页面与下载
只会看到完整版本，失败批次不会覆盖此前可用的航迹。

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 浏览器打开 http://127.0.0.1:8000
```

数据保存在命名卷 `ais-data` 中（`/data/ais.sqlite3` 与 `/data/originals/`）。
`./samples` 以只读方式挂载进容器为 `/samples`，便于在容器内直接取样例文件：

```bash
# 宿主机上传样例（两个批次，验证重投、冲突、迟到点）
curl -F 'file=@samples/batch1_base.csv' http://127.0.0.1:8000/upload
curl -F 'file=@samples/batch2_late_arrivals.csv' http://127.0.0.1:8000/upload
```

停止并保留数据：`docker compose down`；连同卷一起删除：`docker compose down -v`。

> 若本机没有 Docker（本开发环境就没有），可直接用 venv 运行，见下节；
> 两种方式使用完全相同的代码、环境变量与数据布局。

### 本地 venv

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt      # 运行
pip install -r requirements-dev.txt  # 另加测试依赖
uvicorn app.main:app --reload --port 8000
```

环境变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `AIS_DATA_DIR` | `./data` | SQLite 与 `originals/` 根目录 |
| `AIS_MAX_UPLOAD_BYTES` | 67108864 (64 MiB) | 单次上传上限，超限直接拒绝且不改动航迹 |
| `AIS_PORT` | 8000 | 仅 Compose 使用的宿主端口 |

## 输入格式

CSV 至少包含五列（表头大小写/别名均可，支持 `station/shore_station`、`utc/utc_seconds`、
`lat/latitude`、`lon/lng/longitude`），分隔符自动识别 `, ; \t |`：

```csv
station,mmsi,utc,lat,lon
SZ01,413000001,1767225600,22.500000,114.200000
```

* `utc` 为 UTC 秒（Unix epoch，2000–2100 年之间）；
* `mmsi` 为纯数字；
* 纬/经度有限且在合法范围内，且不是 (0,0)；
* 坏行不会中断整批，而是逐行记录在批次页的“解析拒绝行”中。

## 判定规则（`app/ais.py`）

按 MMSI 全量重算，与批次到达顺序无关：

1. **同秒同位置（重投）**：同一船同一 UTC 秒内所有候选位置一致时，只保留一个
   可信候选（先到者胜），其余标记 `duplicate` 隔离。重投不会制造伪轨迹。
2. **同秒异位（冲突）**：该秒只要出现两个不同位置，**该秒所有记录**都标记
   `conflict` 隔离。
3. **速度门限**：候选按 UTC 排序，与**上一可信点**计算的航速超过 **60 节**即
   标记 `speed_exceeded` 隔离；被隔离的点不参与后续“上一可信点”的计算。
4. **迟到电文**：后到的早期记录会触发该船**全部已发布行**的重算，旧点的
   可信/隔离结论及后续速度基准都可能改变；每次重算生成新的船舶 `version`。
5. 位置按 6 位小数（约 0.11 m）比较，恰好覆盖同一次定位的重投抖动。

## 发布与一致性（`app/store.py` / `app/db.py`）

* 原档先写入 `originals/.staging-*/` 并 `fsync`，再原子 `rename` 到
  `originals/<batch_id>/original.csv`，随后才在**单个 `BEGIN IMMEDIATE` 事务**
  中写入批次、原始行、拒绝行、每船判定与可信点并提交。
* 因此“已提交”即“原档已落盘且判定完整”；失败上传整体回滚并删除该目录，
  既有已发布航迹完全不变。
* 查询与下载在 `BEGIN`（WAL 快照）内固定船舶 `version`，并发重算不会让一次
  请求混入新旧两版数据；旧版本在事务内被裁剪。
* 启动时 `cleanup_staging` 清理崩溃遗留的 `.staging-*` 和无批次引用的目录，
  已发布批次永不删除。

## 页面与下载

| 路径 | 作用 |
| --- | --- |
| `GET /` | 批次列表与船舶列表 |
| `POST /upload` | 上传原档（multipart 字段名 `file`） |
| `GET /batches/{id}` | 本批原始行逐行判定 + 解析拒绝明细 |
| `GET /batches/{id}/original.csv` | 下载该批原档（响应头 `X-Content-SHA256`） |
| `GET /vessels/{mmsi}` | 可信点（含相对上一点航速）与隔离原因 |
| `GET /vessels/{mmsi}/track.csv` | 可信航迹 CSV（按 UTC 排序） |
| `GET /vessels/{mmsi}/isolated.csv` | 隔离记录与原因 CSV |
| `GET /export` | 时间窗导出表单（选船、UTC 时间窗） |
| `GET/POST /exports/window` | 下载单船时间窗复盘 ZIP（见下节） |
| `GET /healthz` | 健康检查 |

## 时间窗复盘包（`app/export.py`）

网页 `/export` 选船并填写 UTC 时间窗后，下载 `ais_window_<mmsi>_<start>_<end>.zip`。
时间窗**含起点、不含终点**；时间可写 UTC 秒，也可写 ISO-8601（`Z` 或显式偏移，
无偏移按 UTC 解释）。包内文件：

| 文件 | 内容 |
| --- | --- |
| `track.csv` | 单船 CSV，字段与上传入口一致，可直接经网页“上传原档”导入空库重演 |
| `manifest.csv` | 窗口内每条记录的判定对照：在 `track.csv` 中的行号、原批次、原档行号、可信/隔离结论与原因 |
| `package.json` | 窗口、船舶版本、基准点、文件 SHA-256 与重演说明（机器可读） |
| `README.txt` | 给外部复盘组的中文说明 |

**裁剪规则**（保证空库重演与导出时逐条一致）：

* `track.csv` 包含窗口内该船**全部已解析记录**，外加**窗前最后一个可信点**（若存在）。
  速度门限只依赖“上一可信点”，因此这一个基准点就是窗口判定所需的全部窗前状态；
  窗前没有可信点时则该行不存在，窗口从首条记录开始重演。
* 按整秒选择，同秒冲突/重投不会被窗口边界拆开；行序保持原始到达顺序
  （同秒重投的先到者仍是可信者）。解析拒绝行从未参与判定，不导出。
  岸站号若含换行，会因无法可靠导出与重演而在上传时作为 `invalid_station` 拒绝。
* 生成时用真实上传解析器重新解析 `track.csv` 并在内存中纯重演一遍；任何一条窗口
  记录的结论、原因或说明与清单不符，或行号对不上，就**整体拒绝生成**并返回错误页，
  不会提供残缺压缩包。包内 `track.csv` 超过上传上限时同样拒绝（提示缩小时间窗），
  以免发出无法经上传入口重演的包。
* 查询、基准与判定在同一个 `BEGIN` 读快照内完成（固定船舶 `version`），因此并发
  上传时 CSV、基准和对照清单来自同一已发布航迹版本。

示例（样例数据上传后导出 413000003 的 00:00–00:05）：

```bash
curl -f -o window.zip -X POST \
  -F mmsi=413000003 -F start=1767225600 -F end=1767225840 \
  http://127.0.0.1:8000/exports/window
unzip -l window.zip
# 空库（另起一个 AIS_DATA_DIR）中上传 track.csv，再比对 manifest.csv 的窗口判定
```

## 测试与样例

```bash
. .venv/bin/activate
pytest -q            # 规则 + HTTP/并发/重启/失败回滚
python tests/smoke.py  # 用 samples/ 跑一遍并打印每船判定
```

`samples/README.md` 详细说明了三个样例场景。手工验证顺序：

1. 上传 `samples/batch1_base.csv`：413000001 出现 `duplicate`，413000002 整秒
   `conflict`，413000003 出现 `speed_exceeded`，413000004 的坏行进入拒绝明细。
2. 上传 `samples/batch2_late_arrivals.csv`：413000005 补入迟到点后被重算，
   413000001/413000002 的重投结果保持稳定，同时出现新船 413000008。
3. 对比每次上传后 `/vessels/{mmsi}` 与 `track.csv`：413000006 在批次 2 后其
   01:00 的旧点变为隔离；同时旧批次原档仍可完整下载。
4. 在 `/export` 导出 413000006 的 00:00–01:00 窗口：包内 `track.csv` 含窗口前
   基准（00:00 可信点）与 00:59:59 迟到点；空库导入后 01:00 仍为 `speed_exceeded`。
   若把窗口起在 00:00:01，则基准行必不可少——去掉它，01:00 会被误判为可信。

## 目录

```
app/
  ais.py        CSV 解析、校验、纯函数航迹判定
  db.py         SQLite schema、WAL 连接、读写事务
  store.py      原档原子落盘 + 单事务发布 + 全船重算
  export.py     时间窗复盘包的裁剪、自检与 ZIP 生成
  main.py       FastAPI 路由与 CSV 导出
  templates/    网页（总览/批次/船舶/导出/错误）
samples/        重投、冲突、迟到点、坏行样例
tests/          规则单测、HTTP/并发/重启/回滚/时间窗重演测试、smoke
```
