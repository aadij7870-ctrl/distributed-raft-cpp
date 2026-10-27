# Distributed Raft Consensus Engine (C++17)

A lightweight, fault-tolerant distributed key-value store implementing the Raft Consensus Algorithm from scratch in modern C++17.

## Key Features

- Leader Election: Randomized election timeouts (300-600ms) with split-vote avoidance.
- Log Replication: Two-phase consensus commit mechanism requiring majority quorum (N/2 + 1).
- Dynamic Failover: Sub-second leader crash detection with automatic follower promotion and continuous availability.
- Lagged Follower Catch-Up: Dynamic tracking of next_index and match_index to fast-forward stale or rejoining nodes.
- Crash Durability (WAL): Write-Ahead Logging using atomic file swaps to persist current_term, voted_for, and log entries across total cluster power outages.
- Custom Wire Protocol: Minimal text-based TCP framing for RequestVote, AppendEntries, and client key-value operations (SET, GET).

## Architecture Overview

Client Requests (SET / GET)
         |
         v TCP (Ports 9001, 9002, 9003)
+-------------------------------------------------------------+
| RaftNode                                                    |
|  - Event Loop (Election Timeouts & Heartbeats)              |
|  - Replication Engine (Quorum Acks & Log Commit)            |
|  - Replicated State Machine (Key-Value Store)               |
|  - PersistenceManager (WAL disk storage in data/)           |
+-------------------------------------------------------------+

## Building the Binary

Requirements:
- Linux / WSL
- GCC supporting C++17 (g++ >= 9.0)
- POSIX pthreads

Build command:
g++ -std=c++17 -Wall -Wextra -pthread src/main.cpp -o raft_node

## Verification Test Harness

The test harness executes 4 automated end-to-end suites:
./scripts/run_all_tests.sh

Individual suites:
- ./scripts/test_replication.sh : State machine replication across healthy nodes.
- ./scripts/test_failover.sh    : Leader crash detection and term promotion.
- ./scripts/test_rejoin.sh      : Log catch-up for lagged/restarted nodes.
- ./scripts/test_persistence.sh : Total cluster power loss and WAL recovery.

## Wire Protocol Specification

- VOTE_REQ: Candidate -> Peer
  Format: VOTE_REQ <term> <candidate_id> <last_log_index> <last_log_term>
- VOTE_REPLY: Peer -> Candidate
  Format: VOTE_REPLY <term> <vote_granted: 0|1>
- APPEND_REQ: Leader -> Follower
  Format: APPEND_REQ <term> <leader_id> <prev_idx> <prev_term> <commit_idx> <num_entries> [entries...]
- APPEND_REPLY: Follower -> Leader
  Format: APPEND_REPLY <term> <success: 0|1> <match_index>
- SET: Client -> Leader
  Format: SET <key> <val> -> replies "OK" or "ERR_NOT_LEADER"
- GET: Client -> Any Node
  Format: GET <key> -> replies "VALUE <val>" or "NOT_FOUND"
