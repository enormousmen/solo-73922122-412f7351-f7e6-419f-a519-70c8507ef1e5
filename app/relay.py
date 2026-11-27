"""UDP 中继：模拟隧道 MTU 调小。

- 静默丢弃超过当前上限的数据包（发送端 -> 接收端方向）；
- 转发指定数量的数据包后降档（上限由 RELAY_MAX_PAYLOAD 降为 RELAY_MAX_PAYLOAD_AFTER）；
- 可丢弃指定 seq 的一份分片确认（RELAY_DROP_ACK_SEQ，只丢一次）；
- 可丢弃第一份收齐回执（RELAY_DROP_COMPLETE=1，只丢一次），演示“接收端已收齐、
  只是收齐回执丢了”时发送端重试后仍能确认成功；
- GET /stats 暴露转发/丢弃计数与当前生效上限。
"""
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from .protocol import MAX_DATAGRAM, unpack_ack, unpack_complete

LISTEN_PORT = int(os.environ.get("RELAY_LISTEN_PORT", "9999"))
TARGET_HOST = os.environ.get("RELAY_TARGET_HOST", "127.0.0.1")
TARGET_PORT = int(os.environ.get("RELAY_TARGET_PORT", "9000"))
MAX_PAYLOAD = int(os.environ.get("RELAY_MAX_PAYLOAD", "1200"))
DOWNGRADE_AFTER = int(os.environ.get("RELAY_DOWNGRADE_AFTER", "-1"))  # <0 表示不降档
MAX_PAYLOAD_AFTER = int(os.environ.get("RELAY_MAX_PAYLOAD_AFTER", "600"))
DROP_ACK_SEQ = int(os.environ.get("RELAY_DROP_ACK_SEQ", "-1"))        # <0 表示不丢
DROP_COMPLETE = int(os.environ.get("RELAY_DROP_COMPLETE", "0")) != 0  # 丢弃首份收齐回执
STATS_PORT = int(os.environ.get("RELAY_STATS_PORT", "9100"))


class Relay:
    def __init__(self):
        self.data_forwarded = 0    # 已转发的数据包数（发送端 -> 接收端）
        self.ack_forwarded = 0     # 已转发的分片确认数
        self.complete_forwarded = 0  # 已转发的收齐回执数
        self.dropped_oversize = 0  # 超限被静默丢弃的数据包数
        self.dropped_acks = 0      # 被丢弃的分片确认数
        self.dropped_complete = 0  # 被丢弃的收齐回执数
        self.sender_addr = None
        self._ack_dropped_once = False
        self._complete_dropped_once = False
        self._lock = threading.Lock()

    def current_limit(self) -> int:
        if 0 <= DOWNGRADE_AFTER <= self.data_forwarded:
            return MAX_PAYLOAD_AFTER
        return MAX_PAYLOAD

    def handle(self, datagram: bytes, addr, target, sock: socket.socket):
        if addr == target:
            # 接收端 -> 发送端（确认方向）
            complete = unpack_complete(datagram)
            if complete is not None:
                with self._lock:
                    if DROP_COMPLETE and not self._complete_dropped_once:
                        self._complete_dropped_once = True
                        self.dropped_complete += 1
                        return  # 丢弃首份收齐回执
                if self.sender_addr is not None:
                    sock.sendto(datagram, self.sender_addr)
                    with self._lock:
                        self.complete_forwarded += 1
                return

            parsed = unpack_ack(datagram)
            if parsed is not None:
                _tid, _off, _len, seq = parsed
                with self._lock:
                    if seq == DROP_ACK_SEQ and not self._ack_dropped_once:
                        self._ack_dropped_once = True
                        self.dropped_acks += 1
                        return  # 丢弃指定的一份分片确认
            if self.sender_addr is not None:
                sock.sendto(datagram, self.sender_addr)
                with self._lock:
                    self.ack_forwarded += 1
            return
        # 发送端 -> 接收端（数据方向）
        self.sender_addr = addr
        if len(datagram) > self.current_limit():
            with self._lock:
                self.dropped_oversize += 1
            return  # 静默丢弃超限数据包
        sock.sendto(datagram, target)
        with self._lock:
            self.data_forwarded += 1

    def stats(self) -> dict:
        with self._lock:
            return {
                "data_forwarded": self.data_forwarded,
                "ack_forwarded": self.ack_forwarded,
                "complete_forwarded": self.complete_forwarded,
                "dropped_oversize": self.dropped_oversize,
                "dropped_acks": self.dropped_acks,
                "dropped_complete": self.dropped_complete,
                "current_limit": self.current_limit(),
                "config": {
                    "max_payload": MAX_PAYLOAD,
                    "downgrade_after": DOWNGRADE_AFTER,
                    "max_payload_after": MAX_PAYLOAD_AFTER,
                    "drop_ack_seq": DROP_ACK_SEQ,
                    "drop_complete": DROP_COMPLETE,
                },
            }


def _serve_stats(relay: Relay):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/stats":
                self.send_response(404)
                self.end_headers()
                return
            body = json.dumps(relay.stats()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    HTTPServer(("0.0.0.0", STATS_PORT), Handler).serve_forever()


def _resolve(host: str, attempts: int = 30) -> str:
    for _ in range(attempts):
        try:
            return socket.gethostbyname(host)
        except socket.gaierror:
            time.sleep(1)
    raise SystemExit(f"cannot resolve target host: {host}")


def main():
    target = (_resolve(TARGET_HOST), TARGET_PORT)
    relay = Relay()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", LISTEN_PORT))
    threading.Thread(target=_serve_stats, args=(relay,), daemon=True).start()
    print(f"relay udp:{LISTEN_PORT} -> {target[0]}:{target[1]} "
          f"limit={MAX_PAYLOAD} downgrade_after={DOWNGRADE_AFTER} "
          f"limit_after={MAX_PAYLOAD_AFTER} drop_ack_seq={DROP_ACK_SEQ}", flush=True)
    while True:
        datagram, addr = sock.recvfrom(MAX_DATAGRAM)
        relay.handle(datagram, addr, target, sock)


if __name__ == "__main__":
    main()
