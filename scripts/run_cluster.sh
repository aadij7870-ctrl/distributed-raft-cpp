#!/bin/bash

# Kill any existing lingering node processes on ports 9001, 9002, 9003
pkill -f "./raft_node" 2>/dev/null
sleep 0.5

echo "=== Starting 3-Node Raft Cluster ==="
./raft_node 1 9001 > node1.log 2>&1 &
PID1=$!
./raft_node 2 9002 > node2.log 2>&1 &
PID2=$!
./raft_node 3 9003 > node3.log 2>&1 &
PID3=$!

echo "Node 1 PID: $PID1 (port 9001)"
echo "Node 2 PID: $PID2 (port 9002)"
echo "Node 3 PID: $PID3 (port 9003)"

echo "Waiting 3 seconds for election timeout and leader consensus..."
sleep 3

echo -e "\n=== Checking Cluster Election Results ==="
grep -E "WON ELECTION|Starting election" node1.log node2.log node3.log

echo -e "\n=== Cleaning up processes ==="
kill $PID1 $PID2 $PID3 2>/dev/null
echo "Cluster stopped successfully."
