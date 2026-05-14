#!/usr/bin/env bash
set -euo pipefail

# =========================
# 基本配置
# =========================
BENCH="../../target/release/benchmark"
TARGET="${TARGET:-cachecat}"
OUT_DIR="${OUT_DIR:-./bench_results_1}"

# 服务端启动命令
SERVER_CMD=(./start_cluster.sh)

# 服务端启动后等待秒数
SERVER_START_WAIT="${SERVER_START_WAIT:-10}"

# 每轮测试间隔
SLEEP_SECS="${SLEEP_SECS:-10}"

mkdir -p "$OUT_DIR"

# =========================
# 测试参数规划
# =========================

# 吞吐量测试：不同客户端数
TP_CLIENTS_LIST=(
  1000
)

# 吞吐量测试：不同总请求数
TP_TOTAL_LIST=(
  20000000
)

# 预热请求数
TP_WARMUP="${TP_WARMUP:-100000}"

# 延迟测试：不同请求数
LAT_COUNT_LIST=(
  100
)

# =========================
# 进程管理
# =========================
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    echo ">>> 停止服务端进程 PID=$SERVER_PID"
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  pkill -f cache_cat 2>/dev/null || true
}

trap cleanup EXIT INT TERM

restart_server() {
  local server_log="$1"

  echo "============================================================"
  echo ">>> 重启服务端"
  echo ">>> 日志文件: $server_log"
  echo "============================================================"

#   # 停掉旧服务
#   if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
#     echo ">>> 停止旧服务端 PID=$SERVER_PID"
#     kill "$SERVER_PID" 2>/dev/null || true
#     wait "$SERVER_PID" 2>/dev/null || true
#   fi


  # 如有残留，也尝试清理同名进程
  pkill -f cache_cat 2>/dev/null || true
  sleep 2
  pkill -9 -f cache_cat 2>/dev/null || true
  # 启动新服务端，并同时输出到终端和文件
  "${SERVER_CMD[@]}" > "$server_log" 2>&1 &
  SERVER_PID=$!

  echo ">>> 新服务端已启动，后台PID=$SERVER_PID"
  echo ">>> 等待 ${SERVER_START_WAIT}s 让服务端完成启动"
  sleep "$SERVER_START_WAIT"
}

run_case() {
  local name="$1"
  local cmd="$2"
  local bench_log="$OUT_DIR/${name}.log"

  echo ">>> 开始测试: $name"
  echo ">>> 命令: $cmd"

  bash -c "$cmd" 2>&1 | tee "$bench_log"

  echo ">>> 测试完成: $name"
  sleep "$SLEEP_SECS"
}

run_case_group() {
  local group_name="$1"
  local write_name="$2"
  local write_cmd="$3"
  local read_name="$4"
  local read_cmd="$5"

  local server_log="$OUT_DIR/${group_name}_server.log"

  restart_server "$server_log"
  run_case "$write_name" "$write_cmd"
  run_case "$read_name" "$read_cmd"
}

# =========================
# 吞吐量测试
# =========================
for clients in "${TP_CLIENTS_LIST[@]}"; do
  for total in "${TP_TOTAL_LIST[@]}"; do
    run_case_group \
      "tp_c${clients}_t${total}" \
      "tp_write_c${clients}_t${total}" \
      "$BENCH -T $TARGET -m throughput -o write -n $clients -t $total -p $TP_WARMUP" \
      "tp_read_c${clients}_t${total}" \
      "$BENCH -T $TARGET -m throughput -o read -n $clients -t $total -p $TP_WARMUP"
  done
done

# =========================
# 延迟测试
# =========================


echo "============================================================"
echo "全部测试完成，结果保存在: $OUT_DIR"
echo "============================================================"
