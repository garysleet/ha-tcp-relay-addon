#!/bin/sh
set -e

CONFIG_PATH=/data/options.json
LISTEN_PORT=$(jq -r '.listen_port' "$CONFIG_PATH")
TARGET_HOST=$(jq -r '.target_host' "$CONFIG_PATH")
TARGET_PORT=$(jq -r '.target_port' "$CONFIG_PATH")

echo "TCP Relay: listening on 0.0.0.0:${LISTEN_PORT} -> ${TARGET_HOST}:${TARGET_PORT}"
exec socat TCP-LISTEN:"${LISTEN_PORT}",reuseaddr,fork TCP:"${TARGET_HOST}":"${TARGET_PORT}"
