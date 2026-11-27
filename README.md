# 图块传输 API（UDP 中继 + 隧道降档自适应）

隧道上限调小后超限 UDP 包会被静默丢弃。本系统让测绘终端把正在传的图块送完：
发送端按确认情况自动把 UDP 载荷上限从 1200 字节降到 600 字节，只重传尚未确认的部分；
接收端按原字节位置重组，重叠只计一次；传输成功后才开放原图块下载。

## 拓扑

```
客户端 --HTTP--> api(8000) --UDP--> relay(9999) --UDP--> receiver(UDP 9000)
                     ^                              |
                     |________ ACK _________________|
                 receiver HTTP 9001（状态/下载）   relay HTTP 9100（/stats）
```

- **api**（`app/api_main.py`）：FastAPI。接收上传（≤32 KiB），后台线程按停等协议经中继分段发送（`app/sender.py`）。
- **relay**（`app/relay.py`）：UDP 中继。静默丢弃超限数据包；转发指定数量后降档；可丢弃指定 seq 的一份确认。
- **receiver**（`app/receiver_main.py`）：UDP 重组（`app/intervals.py` 区间合并去重）+ FastAPI 查询/下载。

## 协议

- 数据包：`magic(4) | tile_id(16) | offset(4) | total(4) | seq(4) | payload`，头部 32 字节；UDP 载荷上限（1200/600）含头部，故每档数据片分别为 1168 / 568 字节。
- 确认包：`magic(4) | tile_id(16) | offset(4) | length(4) | seq(4)`，32 字节，回执刚写入的字节区间。
- 发送端规则：同一段连续 2 次超时（默认 0.5 s）→ 降到 600 字节档，**只对未确认区间重新分段**（seq 继续递增）；600 档下同一段再连续 2 次超时 → 传输失败。
- 接收端规则：按 offset 写入缓冲区，区间合并统计不重复字节；重复/重叠段照收照 ACK，但只计一次。

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build
```

compose 默认把 relay 配成：转发 3 个数据包后上限 1200→600（模拟隧道降档），并丢弃 seq=9 的一份确认（演示重传去重）。

### 本地（无 Docker）

```bash
pip install -r requirements.txt
UDP_PORT=9000 uvicorn app.receiver_main:app --port 9001 &
RELAY_TARGET_HOST=127.0.0.1 RELAY_DOWNGRADE_AFTER=3 RELAY_DROP_ACK_SEQ=9 python -m app.relay &
RELAY_HOST=127.0.0.1 RECEIVER_URL=http://127.0.0.1:9001 uvicorn app.api_main:app --port 8000 &
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tiles` | 上传图块（body 为原始字节，≤32 KiB，超出 413，空 400），返回 `tile_id` |
| GET | `/tiles/{id}/status` | 当前发包上限 `send_limit`、状态、超时/降档计数，及接收端 `unique_bytes`（不重复字节数）、`duplicate_bytes`、区间列表 |
| GET | `/tiles/{id}/download` | 传输成功（`state=done`）才开放，否则 409 |
| GET | `/health` | 健康检查 |

接收端（9001）：`GET /tiles/{id}/status`、`GET /tiles/{id}/download`（不完整 409）。
中继（9100）：`GET /stats`（转发/丢弃计数、当前生效上限）。

## 示例：降档后取回原件

```bash
./examples/retrieve_after_downgrade.sh
```

脚本流程：生成 8 KiB 图块 → 上传 → 轮询状态（可见 `send_limit` 由 1200 降为 600）→ 查看中继统计 → 下载并 `sha256sum`/`cmp` 校验。等价手工命令：

```bash
TILE_ID=$(curl -sf -X POST --data-binary @tile.bin http://localhost:8000/tiles | jq -r .tile_id)
curl -s http://localhost:8000/tiles/$TILE_ID/status | jq .   # 等到 state=done
curl -sf http://localhost:8000/tiles/$TILE_ID/download -o out.bin && cmp tile.bin out.bin
```

## 配置（环境变量）

**api**：`RELAY_HOST`/`RELAY_PORT`（中继地址）、`RECEIVER_URL`、`SEND_TIMEOUT_SEC`（确认超时，默认 0.5）。
**relay**：`RELAY_MAX_PAYLOAD`（初始上限 1200）、`RELAY_DOWNGRADE_AFTER`（转发 N 个数据包后降档，<0 关闭）、`RELAY_MAX_PAYLOAD_AFTER`（降档后上限 600）、`RELAY_DROP_ACK_SEQ`（丢弃指定 seq 的一份确认，仅一次，<0 关闭）、`RELAY_STATS_PORT`。
**receiver**：`UDP_PORT`（默认 9000）。

## 实测结果（本机 Python 3.11，三进程与 compose 同配置）

单元测试：`tests/test_intervals.py`、`tests/test_protocol.py` 全部通过。

降档演示（8 KiB，relay 转发 3 个后 1200→600，丢 seq=9 的 ACK）：

```
{"state":"transferring","send_limit":1200,"uniq":3504,"dup":0}     # 3 段 1168B 已确认
{"state":"transferring","send_limit":600, "uniq":4640,"dup":0}     # 2 次超时后降档重分段
{"state":"done",        "send_limit":600, "uniq":8192,"dup":568}   # 丢 ACK 触发重传，重叠 568B 只计一次
relay /stats: data_forwarded=13, dropped_oversize=2, dropped_acks=1, current_limit=600
下载 sha256 与原件一致
```

失败路径（relay 降档后上限设为 300，600 档也过不去）：

```
{"state":"failed","send_limit":600,"timeouts":4,"downgrades":1,
 "error":"segment at offset 3504 timed out 2x at 600-byte tier"}   # 小档两次超时判失败
下载（传输中/失败后）均 409
```

边界与异常：33 KiB → 413；空 body → 400；未知 tile → 404；32 KiB 满额传输成功且校验一致（60 个数据包，含降档后重分段）。

## 限制与未完成项

- 本环境无 Docker，`docker-compose.yml`/`Dockerfile` 经 YAML 解析与人工核对但未实际构建；三服务已用同一配置在本机进程级实测。
- 状态全部在内存：api/receiver/relay 重启即丢失，未做持久化与断点续传。
- relay 只记录最近一个发送端地址，多客户端并发经同一中继会互相抢回包地址（演示为单传输场景设计）。
- 停等协议（一次一段），未做滑动窗口/拥塞控制；32 KiB 量级下足够。
- 无鉴权、无 TLS，仅限可信内网；上传内容类型不校验（按字节流处理）。
- relay 的丢 ACK 配置是全局一次性（`RELAY_DROP_ACK_SEQ` 命中一次后即失效），不按 tile 区分。
