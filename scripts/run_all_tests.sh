#!/bin/bash

set -e

echo "=========================================="
echo "    COMPILING RAFT CONSENSUS ENGINE       "
echo "=========================================="
g++ -std=c++17 -Wall -Wextra -pthread src/main.cpp -o raft_node
echo "[OK] Build succeeded."

echo -e "\n=========================================="
echo "    RUNNING TEST SUITE 1: REPLICATION     "
echo "=========================================="
./scripts/test_replication.sh

echo -e "\n=========================================="
echo "    RUNNING TEST SUITE 2: DYNAMIC FAILOVER"
echo "=========================================="
./scripts/test_failover.sh

echo -e "\n=========================================="
echo "    RUNNING TEST SUITE 3: NODE CATCH-UP   "
echo "=========================================="
./scripts/test_rejoin.sh

echo -e "\n=========================================="
echo "    RUNNING TEST SUITE 4: WAL PERSISTENCE "
echo "=========================================="
./scripts/test_persistence.sh

echo -e "\n=========================================="
echo "    ALL RAFT VERIFICATION SUITES PASSED   "
echo "=========================================="
