#pragma once
#include <iostream>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <random>
#include <thread>
#include <atomic>
#include <functional>
#include "log_entry.h"
#include "rpc_message.h"

enum class NodeRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

class RaftNode {
public:
    int id;
    int cluster_size;
    
    mutable std::mutex mtx;
    std::atomic<bool> running{true};

    NodeRole role{NodeRole::FOLLOWER};
    uint64_t current_term{0};
    int voted_for{-1};

    std::vector<LogEntry> log;
    std::unordered_map<std::string, std::string> kv_store;

    uint64_t commit_index{0};
    uint64_t last_applied{0};

    std::unordered_map<int, uint64_t> next_index;
    std::unordered_map<int, uint64_t> match_index;

    std::chrono::steady_clock::time_point last_heartbeat_time;
    int election_timeout_ms;

    RaftNode(int node_id, int total_nodes) 
        : id(node_id), cluster_size(total_nodes) {
        reset_election_timeout();
        last_heartbeat_time = std::chrono::steady_clock::now();
        log.push_back({0, 0, CommandType::NOOP, "", ""});
    }

    void reset_election_timeout() {
        static thread_local std::mt19937 generator(std::random_device{}() + id);
        std::uniform_int_distribution<int> distribution(150, 300);
        election_timeout_ms = distribution(generator);
    }

    RequestVoteReply handle_request_vote(const RequestVoteArgs& args) {
        std::lock_guard<std::mutex> lock(mtx);
        RequestVoteReply reply;
        reply.term = current_term;
        reply.vote_granted = false;

        if (args.term < current_term) {
            return reply;
        }

        if (args.term > current_term) {
            current_term = args.term;
            role = NodeRole::FOLLOWER;
            voted_for = -1;
        }

        uint64_t my_last_index = log.empty() ? 0 : log.back().index;
        uint64_t my_last_term = log.empty() ? 0 : log.back().term;

        bool log_ok = (args.last_log_term > my_last_term) ||
                      (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

        if ((voted_for == -1 || voted_for == args.candidate_id) && log_ok) {
            voted_for = args.candidate_id;
            reply.vote_granted = true;
            last_heartbeat_time = std::chrono::steady_clock::now();
        }

        reply.term = current_term;
        return reply;
    }

    AppendEntriesReply handle_append_entries(const AppendEntriesArgs& args) {
        std::lock_guard<std::mutex> lock(mtx);
        AppendEntriesReply reply;
        reply.term = current_term;
        reply.success = false;
        reply.match_index = 0;

        if (args.term < current_term) {
            return reply;
        }

        if (args.term > current_term || role == NodeRole::CANDIDATE) {
            current_term = args.term;
            role = NodeRole::FOLLOWER;
            voted_for = -1;
        }
        last_heartbeat_time = std::chrono::steady_clock::now();

        if (args.prev_log_index >= log.size() || log[args.prev_log_index].term != args.prev_log_term) {
            return reply;
        }

        uint64_t insert_index = args.prev_log_index + 1;
        for (const auto& entry : args.entries) {
            if (insert_index < log.size()) {
                if (log[insert_index].term != entry.term) {
                    log.erase(log.begin() + insert_index, log.end());
                    log.push_back(entry);
                }
            } else {
                log.push_back(entry);
            }
            insert_index++;
        }

        reply.success = true;
        reply.match_index = log.size() - 1;

        if (args.leader_commit > commit_index) {
            commit_index = std::min(args.leader_commit, static_cast<uint64_t>(log.size() - 1));
            apply_logs_to_state_machine();
        }

        return reply;
    }

    void apply_logs_to_state_machine() {
        while (last_applied < commit_index) {
            last_applied++;
            const auto& entry = log[last_applied];
            if (entry.type == CommandType::SET) {
                kv_store[entry.key] = entry.value;
            } else if (entry.type == CommandType::DEL) {
                kv_store.erase(entry.key);
            }
        }
    }
};
