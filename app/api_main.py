"""图块传输 API（发送侧）。

- POST /tiles                上传图块（<=32 KiB），后台经 UDP 中继开始传输
- GET  /tiles/{id}/status    查询当前发包上限、传输状态及接收端已收的不重复字节数
- GET  /tiles/{id}/download  仅当发送端经 UDP 收到接收端的“收齐回执”后才开放

传输结果的权威判定来自 UDP 协议本身（分片确认 + 收齐回执），不依赖接收端
HTTP 查询。接收端 HTTP 暂时不可用时，status 仍按 UDP 判定返回 done/failed，
只是 receiver 字段为 null；download 在 UDP 未确认前一律 409。
"""
import os
import threading
import uuid

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

from .sender import COMPLETE_WAIT_SEC, LARGE_LIMIT, TransferState, run_transfer

MAX_TILE_BYTES = 32 * 1024  # 一次上传最多 32 KiB
RELAY_ADDR = (os.environ.get("RELAY_HOST", "127.0.0.1"),
              int(os.environ.get("RELAY_PORT", "9999")))
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://127.0.0.1:9001").rstrip("/")
SEND_TIMEOUT_SEC = float(os.environ.get("SEND_TIMEOUT_SEC", "0.5"))

app = FastAPI(title="Tile Transfer API")
_transfers: dict[str, TransferState] = {}


@app.post("/tiles", status_code=201)
async def upload_tile(request: Request):
    body = await request.body()
    if not body:
        raise HTTPException(400, "empty tile body")
    if len(body) > MAX_TILE_BYTES:
        raise HTTPException(413, f"tile too large: {len(body)} bytes > {MAX_TILE_BYTES}")
    tile_id = uuid.uuid4().hex
    state = TransferState(tile_id, len(body), complete_wait_s=COMPLETE_WAIT_SEC)
    _transfers[tile_id] = state
    threading.Thread(
        target=run_transfer,
        args=(state, body, RELAY_ADDR, SEND_TIMEOUT_SEC),
        daemon=True,
    ).start()
    return {"tile_id": tile_id, "size": len(body), "send_limit": LARGE_LIMIT}


@app.get("/tiles/{tile_id}/status")
def tile_status(tile_id: str):
    state = _transfers.get(tile_id)
    if state is None:
        raise HTTPException(404, "unknown tile_id")
    snap = state.snapshot()
    receiver = None
    try:
        resp = httpx.get(f"{RECEIVER_URL}/tiles/{tile_id}/status", timeout=2.0)
        if resp.status_code == 200:
            receiver = resp.json()
    except httpx.HTTPError:
        # 接收端 HTTP 暂时不可用不影响 UDP 判定；state 仍来自发送端的收齐回执。
        pass
    return {**snap, "receiver": receiver}


@app.get("/tiles/{tile_id}/download")
def tile_download(tile_id: str):
    state = _transfers.get(tile_id)
    if state is None:
        raise HTTPException(404, "unknown tile_id")
    snap = state.snapshot()
    if snap["state"] != "done" or not snap["complete_confirmed"]:
        # 未收到接收端收齐回执（或已判 failed）不开放原图块下载。
        # 注意：分片回执拼起来覆盖整块不算收齐。
        raise HTTPException(
            409,
            f"transfer not confirmed complete via UDP (state={snap['state']}, "
            f"complete_confirmed={snap['complete_confirmed']})",
        )
    try:
        resp = httpx.get(f"{RECEIVER_URL}/tiles/{tile_id}/download", timeout=5.0)
    except httpx.HTTPError as exc:
        # 传输结果已由 UDP 确认；此处只是取字节时接收端 HTTP 不可用。
        raise HTTPException(502, f"receiver unreachable after UDP-confirmed transfer: {exc}")
    if resp.status_code != 200:
        raise HTTPException(502, f"receiver refused download: HTTP {resp.status_code}")
    return Response(content=resp.content, media_type="application/octet-stream")


@app.get("/health")
def health():
    return {"ok": True}
