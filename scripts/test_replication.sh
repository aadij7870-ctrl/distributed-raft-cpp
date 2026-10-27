#!/bin/bash

pkill -f "./raft_node" 2>/dev/null
sleep 0.5

echo "=== 1. Starting 3-Node Raft Cluster ==="
./raft_node 1 9001 > node1.log 2>&1 &
PID1=$!
./raft_node 2 9002 > node2.log 2>&1 &
PID2=$!
./raft_node 3 9003 > node3.log 2>&1 &
PID3=$!

echo "Waiting for Leader Election..."
sleep 2.5

# Detect Leader port from logs
LEADER_PORT=""
if grep -q "WON ELECTION" node1.log; then
    LEADER_PORT=9001
    LEADER_ID=1
elif grep -q "WON ELECTION" node2.log; then
    LEADER_PORT=9002
    LEADER_ID=2
elif grep -q "WON ELECTION" node3.log; then
    LEADER_PORT=9003
    LEADER_ID=3
fi

if [ -z "$LEADER_PORT" ]; then
    echo "[FAIL] No leader elected within timeout period."
    kill $PID1 $PID2 $PID3 2>/dev/null
    exit 1
fi

echo "Detected Leader: Node $LEADER_ID on port $LEADER_PORT"

echo -e "\n=== 2. Issuing Replicated Write to Leader ==="
WRITE_RESP=$(echo "SET cloud_project distributed_raft_kv" | nc -w 1 127.0.0.1 $LEADER_PORT)
echo "Leader response: $WRITE_RESP"

# Allow brief moment for heartbeat commit propagation
sleep 0.5

echo -e "\n=== 3. Querying All Nodes for Replicated Value ==="
for PORT in 9001 9002 9003; do
    VAL=$(echo "GET cloud_project" | nc -w 1 127.0.0.1 $PORT)
    echo "Query port $PORT -> $VAL"
done

echo -e "\n=== Cleaning up processes ==="
kill $PID1 $PID2 $PID3 2>/dev/null
echo "Test complete."
