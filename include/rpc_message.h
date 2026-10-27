#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include "log_entry.h"

// RPC 1: Candidate -> Peer (RequestVote)
struct RequestVoteArgs {
    uint64_t term;          // Candidate's current term
    int candidate_id;       // Candidate requesting vote
    uint64_t last_log_index;// Index of candidate's last log entry
    uint64_t last_log_term; // Term of candidate's last log entry
};

struct RequestVoteReply {
    uint64_t term;          // Current term of recipient, for candidate to update itself
    bool vote_granted;      // True means candidate received vote
};

// RPC 2: Leader -> Follower (AppendEntries / Heartbeat)
struct AppendEntriesArgs {
    uint64_t term;          // Leader's current term
    int leader_id;          // So followers can redirect clients
    uint64_t prev_log_index;// Index of log entry immediately preceding new ones
    uint64_t prev_log_term; // Term of prev_log_index entry
    std::vector<LogEntry> entries; // Log entries to store (empty for heartbeat)
    uint64_t leader_commit; // Leader's commit index
};

struct AppendEntriesReply {
    uint64_t term;          // Current term of recipient, for leader to update itself
    bool success;           // True if follower contained entry matching prev_log_index/term
    uint64_t match_index;   // Follower's latest matched index (for fast synchronization)
};
