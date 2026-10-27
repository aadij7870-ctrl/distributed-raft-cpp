#!/bin/bash

pkill -f "./raft_node" 2>/dev/null
rm -rf data/*.wal*
sleep 1

echo "=== 1. Starting Fresh 3-Node Raft Cluster ==="
./raft_node 1 9001 > node1.log 2>&1 &
PID1=$!
./raft_node 2 9002 > node2.log 2>&1 &
PID2=$!
./raft_node 3 9003 > node3.log 2>&1 &
PID3=$!

sleep 2.5

declare -A NODES=([9001]=1 [9002]=2 [9003]=3)
LEADER_PORT=""
for PORT in 9001 9002 9003; do
    if grep -q "WON ELECTION" "node${NODES[$PORT]}.log"; then
        LEADER_PORT=$PORT
        LEADER_ID=${NODES[$PORT]}
    fi
done

echo "Cluster Leader: Node $LEADER_ID on port $LEADER_PORT"

echo -e "\n=== 2. Writing Entries to Leader (To be Persisted in WAL) ==="
echo "SET record_alpha 1001" | nc -w 1 127.0.0.1 $LEADER_PORT
echo "SET record_beta 2002" | nc -w 1 127.0.0.1 $LEADER_PORT
sleep 0.5

echo -e "\n=== 3. Inspecting Generated WAL Files on Disk ==="
ls -lh data/*.wal

echo -e "\n=== 4. SIMULATING TOTAL POWER OUTAGE (Killing all 3 nodes) ==="
kill -9 $PID1 $PID2 $PID3
echo "All processes abruptly terminated."
sleep 1.0

echo -e "\n=== 5. Restarting Cluster from Persistent WAL Files ==="
./raft_node 1 9001 > node1.log 2>&1 &
PID1=$!
./raft_node 2 9002 > node2.log 2>&1 &
PID2=$!
./raft_node 3 9003 > node3.log 2>&1 &
PID3=$!

sleep 2.0

echo -e "\n=== 6. Querying All Nodes Directly for Restored Keys ==="
for PORT in 9001 9002 9003; do
    echo "--- Checking Node ${NODES[$PORT]} on port $PORT ---"
    V1=$(echo "GET record_alpha" | nc -w 1 127.0.0.1 $PORT)
    V2=$(echo "GET record_beta" | nc -w 1 127.0.0.1 $PORT)
    echo "  record_alpha -> $V1"
    echo "  record_beta  -> $V2"
done

echo -e "\n=== Cleaning Up ==="
kill -9 $PID1 $PID2 $PID3 2>/dev/null
echo "Persistence test completed successfully."
