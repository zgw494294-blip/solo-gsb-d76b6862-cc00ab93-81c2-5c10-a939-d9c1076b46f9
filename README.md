# Task Queue API

可自托管的可靠任务队列，基于 FastAPI + SQLite（WAL 模式）。支持幂等提交、限时租约、按 `group_key` 严格 FIFO、自动重试与死信队列。

## 快速启动（Docker，一键）

```bash
docker compose up --build
```

启动后访问：

- API 地址： **http://localhost:8000**
- 交互式文档（Swagger UI）： **http://localhost:8000/docs**
- 健康检查： `GET http://localhost:8000/healthz`

SQLite 数据库文件持久化在命名卷 `queue-data`（容器内 `/data/queue.db`），建表等初始化在容器启动时自动完成，无需手工迁移。

## 环境变量

均可选，在 `compose.yaml` 的 `environment` 段或 `docker run -e` 中配置：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TQ_DB_PATH` | 容器内 `/data/queue.db`，本地运行 `./queue.db` | SQLite 数据库文件路径 |
| `TQ_LEASE_TTL_SECONDS` | `30` | 领取任务时租约的默认有效期（秒） |
| `TQ_MAX_ATTEMPTS` | `3` | 任务默认最大尝试次数，达到后进入死信 |
| `TQ_MAX_BATCH_SIZE` | `100` | 单次领取接口允许的最大批量 |

端口映射在 `compose.yaml` 中（默认 `8000:8000`，改左侧端口即可换宿主端口）。

## 本地开发（不用 Docker）

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload --port 8000
pytest                 # 运行测试
```

## 核心语义

- **幂等提交**：`idempotency_key` 全局唯一。相同键 + 相同载荷（JSON 规范化后比较，键序无关）返回原任务（HTTP 200）；相同键 + 不同载荷返回 **409**。
- **租约**：领取返回限时 `lease_token`。只有持当前有效令牌才能 `ack` / `fail`；租约过期后任务可被重新领取，旧令牌一律失效（409）。
- **严格 FIFO**：每个 `group_key` 内只有队首任务（最早未完成任务）可被领取；前序任务成功（`succeeded`）或进入死信（`dead`）后，后序任务才会被发放。单次领取每组最多一个任务。
- **重试与死信**：每次发放计一次尝试。`fail` 或租约过期耗尽 `max_attempts` 后任务进入死信，不再被发放；死信可查询并重新入队（重置尝试次数，回到原 FIFO 位置）。
- **并发安全**：领取、回收、确认均在 `BEGIN IMMEDIATE` 事务内完成，并发领取不会重复发放同一任务。

## API 一览

### 提交任务

```bash
curl -X POST http://localhost:8000/tasks \
  -H 'content-type: application/json' \
  -d '{"group_key":"orders","idempotency_key":"ord-1","payload":{"sku":"A1","qty":2},"max_attempts":3}'
```

`201` 新建 / `200` 幂等命中返回原任务 / `409` 同键不同载荷。

### 批量领取（获得租约令牌）

```bash
curl -X POST http://localhost:8000/claims \
  -H 'content-type: application/json' \
  -d '{"limit":10,"lease_ttl_seconds":60,"group_keys":["orders"]}'
```

`limit`、`lease_ttl_seconds`、`group_keys`（过滤分组）均可选。响应中每个任务带 `lease_token` 与 `lease_expires_at`。

### 确认成功 / 标记失败

```bash
curl -X POST http://localhost:8000/tasks/<task_id>/ack \
  -H 'content-type: application/json' -d '{"lease_token":"<token>"}'

curl -X POST http://localhost:8000/tasks/<task_id>/fail \
  -H 'content-type: application/json' -d '{"lease_token":"<token>","error":"reason"}'
```

令牌不匹配或租约过期返回 `409`。`fail` 后若仍有剩余重试次数任务回到 `pending`，否则进入死信。

### 查询状态与死信、重新入队

```bash
curl http://localhost:8000/tasks/<task_id>
curl 'http://localhost:8000/tasks?status=pending&group_key=orders'
curl http://localhost:8000/dead-letter
curl -X POST http://localhost:8000/dead-letter/<task_id>/requeue
```

任务状态机：`pending` → `leased` → `succeeded` /（重试）`pending` /（耗尽）`dead`；死信经 `requeue` 回到 `pending`。

## 项目结构

```
app/
  config.py    # 环境变量配置
  db.py        # SQLite 连接与事务（WAL + BEGIN IMMEDIATE）
  schemas.py   # 请求/响应模型
  service.py   # 幂等、租约、FIFO、死信核心逻辑
  main.py      # FastAPI 路由
tests/test_queue.py
Dockerfile / compose.yaml
```
