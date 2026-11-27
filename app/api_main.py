"""图块传输 API（发送侧）。

- POST /tiles                上传图块（<=32 KiB），后台经 UDP 中继开始传输
- GET  /tiles/{id}/status    查询当前发包上限、传输状态及接收端已收的不重复字节数
- GET  /tiles/{id}/download  传输成功后取回接收端重组出的原图块
"""
import os
import threading
import uuid

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

from .sender import LARGE_LIMIT, TransferState, run_transfer

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
    state = TransferState(tile_id, len(body))
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
        pass
    return {**snap, "receiver": receiver}


@app.get("/tiles/{tile_id}/download")
def tile_download(tile_id: str):
    state = _transfers.get(tile_id)
    if state is None:
        raise HTTPException(404, "unknown tile_id")
    snap = state.snapshot()
    if snap["state"] != "done":
        # 传输成功才开放原图块下载
        raise HTTPException(409, f"transfer not complete (state={snap['state']})")
    try:
        resp = httpx.get(f"{RECEIVER_URL}/tiles/{tile_id}/download", timeout=5.0)
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"receiver unreachable: {exc}")
    if resp.status_code != 200:
        raise HTTPException(502, f"receiver refused download: HTTP {resp.status_code}")
    return Response(content=resp.content, media_type="application/octet-stream")


@app.get("/health")
def health():
    return {"ok": True}
