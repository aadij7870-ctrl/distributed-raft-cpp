#!/bin/bash

pkill -f "./raft_node" 2>/dev/null
sleep 1

echo "=== 1. Starting 3-Node Raft Cluster ==="
./raft_node 1 9001 > node1.log 2>&1 &
PID1=$!
./raft_node 2 9002 > node2.log 2>&1 &
PID2=$!
./raft_node 3 9003 > node3.log 2>&1 &
PID3=$!

echo "Node 1 PID: $PID1 | Node 2 PID: $PID2 | Node 3 PID: $PID3"
sleep 2.5

# Detect initial Leader
declare -A PIDS=([9001]=$PID1 [9002]=$PID2 [9003]=$PID3)
declare -A NODES=([9001]=1 [9002]=2 [9003]=3)

LEADER_PORT=""
for PORT in 9001 9002 9003; do
    if grep -q "WON ELECTION" "node${NODES[$PORT]}.log"; then
        LEADER_PORT=$PORT
        LEADER_ID=${NODES[$PORT]}
    fi
done

if [ -z "$LEADER_PORT" ]; then
    echo "[FAIL] Initial Leader could not be identified."
    kill $PID1 $PID2 $PID3 2>/dev/null
    exit 1
fi

echo "Initial Leader elected: Node $LEADER_ID on port $LEADER_PORT (PID: ${PIDS[$LEADER_PORT]})"

echo -e "\n=== 2. Write Pre-Crash Data to Initial Leader ==="
RESP1=$(echo "SET status pre_crash_ok" | nc -w 1 127.0.0.1 $LEADER_PORT)
echo "Leader write response: $RESP1"
sleep 0.5

echo -e "\n=== 3. Killing Initial Leader (Node $LEADER_ID) ==="
kill -9 ${PIDS[$LEADER_PORT]}
echo "Leader process ${PIDS[$LEADER_PORT]} killed abruptly."

echo "Waiting for surviving nodes to trigger new election..."
sleep 3.5

SURVIVING_PORTS=()
for PORT in 9001 9002 9003; do
    if [ "$PORT" -ne "$LEADER_PORT" ]; then
        SURVIVING_PORTS+=($PORT)
    fi
done

NEW_LEADER_PORT=""
for PORT in "${SURVIVING_PORTS[@]}"; do
    RESP=$(echo "SET recovery post_crash_success" | nc -w 1 127.0.0.1 $PORT 2>/dev/null)
    if [ "$RESP" == "OK" ]; then
        NEW_LEADER_PORT=$PORT
        NEW_LEADER_ID=${NODES[$PORT]}
        break
    fi
done

if [ -z "$NEW_LEADER_PORT" ]; then
    echo "[FAIL] No surviving node stepped up as Leader."
    pkill -f "./raft_node" 2>/dev/null
    exit 1
fi

echo "New Leader confirmed: Node $NEW_LEADER_ID on port $NEW_LEADER_PORT"
echo "Post-crash write succeeded on new leader."
sleep 0.5

echo -e "\n=== 4. Verifying State Consistency Across Surviving Nodes ==="
for PORT in "${SURVIVING_PORTS[@]}"; do
    echo "--- Checking Node ${NODES[$PORT]} (port $PORT) ---"
    VAL1=$(echo "GET status" | nc -w 1 127.0.0.1 $PORT)
    VAL2=$(echo "GET recovery" | nc -w 1 127.0.0.1 $PORT)
    echo "  status   -> $VAL1"
    echo "  recovery -> $VAL2"
done

echo -e "\n=== 5. Cleaning Up Processes ==="
pkill -f "./raft_node" 2>/dev/null
echo "Failover verification complete."
