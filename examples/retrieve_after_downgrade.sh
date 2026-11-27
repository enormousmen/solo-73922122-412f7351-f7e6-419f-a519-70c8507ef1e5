#!/usr/bin/env bash
# 演示：隧道降档（1200 -> 600 字节）后仍传完图块，并取回原件校验。
# 用法：先启动服务（docker compose up 或本地起三个进程），然后运行本脚本。
set -euo pipefail

API=${API_URL:-http://localhost:8000}
RELAY=${RELAY_STATS_URL:-http://localhost:9100}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "== 1. 生成 8 KiB 测试图块 =="
head -c 8192 /dev/urandom > "$WORK/tile.bin"
sha256sum "$WORK/tile.bin"

echo "== 2. 上传图块（上限 32 KiB） =="
TILE_ID=$(curl -sf -X POST --data-binary @"$WORK/tile.bin" \
  -H 'Content-Type: application/octet-stream' "$API/tiles" | jq -r .tile_id)
echo "tile_id=$TILE_ID"

echo "== 3. 轮询状态：观察发包上限 1200 -> 600 降档、不重复字节数增长 =="
while :; do
  S=$(curl -sf "$API/tiles/$TILE_ID/status")
  echo "$S" | jq -c '{state, send_limit, uniq: .receiver.unique_bytes, dup: .receiver.duplicate_bytes}'
  STATE=$(echo "$S" | jq -r .state)
  case "$STATE" in
    done)   break ;;
    failed) echo "传输失败: $(echo "$S" | jq -r .error)"; exit 1 ;;
  esac
  sleep 0.3
done

echo "== 4. 中继统计（超限丢弃 / 丢弃确认） =="
curl -sf "$RELAY/stats" | jq .

echo "== 5. 下载原件并校验一致性 =="
curl -sf "$API/tiles/$TILE_ID/download" -o "$WORK/out.bin"
sha256sum "$WORK/tile.bin" "$WORK/out.bin"
cmp "$WORK/tile.bin" "$WORK/out.bin"
echo "OK: 降档后取回的图块与原件一致"
