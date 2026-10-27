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

declare -A PIDS=([9001]=$PID1 [9002]=$PID2 [9003]=$PID3)
declare -A NODES=([9001]=1 [9002]=2 [9003]=3)

sleep 2.5

LEADER_PORT=""
for PORT in 9001 9002 9003; do
    if grep -q "WON ELECTION" "node${NODES[$PORT]}.log"; then
        LEADER_PORT=$PORT
        LEADER_ID=${NODES[$PORT]}
    fi
done

echo "Initial Leader: Node $LEADER_ID on port $LEADER_PORT"

echo -e "\n=== 2. Write data1 to Initial Leader ==="
echo "SET data1 initial_value" | nc -w 1 127.0.0.1 $LEADER_PORT
sleep 0.4

echo -e "\n=== 3. Killing Leader (Node $LEADER_ID) ==="
kill -9 ${PIDS[$LEADER_PORT]}
echo "Node $LEADER_ID killed."

echo "Waiting for remaining nodes to elect new leader..."
sleep 3.5

SURVIVING_PORTS=()
for PORT in 9001 9002 9003; do
    if [ "$PORT" -ne "$LEADER_PORT" ]; then
        SURVIVING_PORTS+=($PORT)
    fi
done

NEW_LEADER_PORT=""
for PORT in "${SURVIVING_PORTS[@]}"; do
    RESP=$(echo "SET data2 post_failover_1" | nc -w 1 127.0.0.1 $PORT 2>/dev/null)
    if [ "$RESP" == "OK" ]; then
        NEW_LEADER_PORT=$PORT
        NEW_LEADER_ID=${NODES[$PORT]}
        break
    fi
done

echo "New Leader: Node $NEW_LEADER_ID on port $NEW_LEADER_PORT"
echo "Writing data3 to New Leader..."
echo "SET data3 post_failover_2" | nc -w 1 127.0.0.1 $NEW_LEADER_PORT
sleep 0.5

echo -e "\n=== 4. Restarting Node $LEADER_ID on port $LEADER_PORT (Rejoining Cluster) ==="
./raft_node $LEADER_ID $LEADER_PORT > "node${LEADER_ID}.log" 2>&1 &
REJOIN_PID=$!
echo "Node $LEADER_ID restarted with PID: $REJOIN_PID"

echo "Waiting for Leader heartbeats to replicate missed logs to Node $LEADER_ID..."
sleep 2.0

echo -e "\n=== 5. Querying Rejoined Node $LEADER_ID for All Committed Keys ==="
V1=$(echo "GET data1" | nc -w 1 127.0.0.1 $LEADER_PORT)
V2=$(echo "GET data2" | nc -w 1 127.0.0.1 $LEADER_PORT)
V3=$(echo "GET data3" | nc -w 1 127.0.0.1 $LEADER_PORT)

echo "Node $LEADER_ID data1 -> $V1"
echo "Node $LEADER_ID data2 -> $V2"
echo "Node $LEADER_ID data3 -> $V3"

echo -e "\n=== Cleaning Up ==="
pkill -f "./raft_node" 2>/dev/null
echo "Rejoin test complete."
