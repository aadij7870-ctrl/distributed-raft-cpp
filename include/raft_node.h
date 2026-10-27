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
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include "log_entry.h"
#include "rpc_message.h"
#include "network_protocol.h"

enum class NodeRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

struct PeerInfo {
    int id;
    int port;
};

class RaftNode {
public:
    int id;
    int cluster_size;
    std::vector<PeerInfo> peers;
    
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

    void set_peers(const std::vector<PeerInfo>& p) {
        peers = p;
    }

    void reset_election_timeout() {
        static thread_local std::mt19937 generator(std::random_device{}() + id);
        std::uniform_int_distribution<int> distribution(150, 300);
        election_timeout_ms = distribution(generator);
    }

    // Helper: Send a TCP string message to a peer port and read one-line reply
    static std::string send_rpc(int port, const std::string& message) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return "";

        sockaddr_in serv_addr{};
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

        // Fast connection timeout
        struct timeval tv{0, 50000}; // 50ms timeout
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sock);
            return "";
        }

        send(sock, message.c_str(), message.size(), 0);

        char buf[1024];
        ssize_t bytes = recv(sock, buf, sizeof(buf) - 1, 0);
        close(sock);

        if (bytes <= 0) return "";
        buf[bytes] = '\0';
        return std::string(buf);
    }

    // --- RPC Handler 1: RequestVote ---
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

    // --- RPC Handler 2: AppendEntries ---
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

    // --- Background Loop: Election Timer & Heartbeats ---
    void run_event_loop() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));

            std::unique_lock<std::mutex> lock(mtx);
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_time).count();

            if (role == NodeRole::LEADER) {
                // Heartbeat period: 50ms
                if (elapsed >= 50) {
                    last_heartbeat_time = now;
                    AppendEntriesArgs args;
                    args.term = current_term;
                    args.leader_id = id;
                    args.prev_log_index = log.back().index;
                    args.prev_log_term = log.back().term;
                    args.leader_commit = commit_index;
                    auto peer_list = peers;
                    lock.unlock();

                    std::string payload = NetworkProtocol::serialize_append_entries(args);
                    for (const auto& p : peer_list) {
                        std::thread([p, payload, this]() {
                            std::string reply_str = send_rpc(p.port, payload);
                            AppendEntriesReply reply;
                            if (NetworkProtocol::parse_append_entries_reply(reply_str, reply)) {
                                std::lock_guard<std::mutex> lk(mtx);
                                if (reply.term > current_term) {
                                    current_term = reply.term;
                                    role = NodeRole::FOLLOWER;
                                    voted_for = -1;
                                }
                            }
                        }).detach();
                    }
                }
            } else if (elapsed >= election_timeout_ms) {
                // Trigger election
                role = NodeRole::CANDIDATE;
                current_term++;
                voted_for = id;
                reset_election_timeout();
                last_heartbeat_time = now;

                std::cout << "[Node " << id << "] Election timeout reached! Starting election for Term " << current_term << std::endl;

                RequestVoteArgs args;
                args.term = current_term;
                args.candidate_id = id;
                args.last_log_index = log.back().index;
                args.last_log_term = log.back().term;

                int votes_received = 1; // Vote for self
                auto peer_list = peers;
                uint64_t election_term = current_term;
                lock.unlock();

                std::string payload = NetworkProtocol::serialize_request_vote(args);
                for (const auto& p : peer_list) {
                    std::string reply_str = send_rpc(p.port, payload);
                    RequestVoteReply reply;
                    if (NetworkProtocol::parse_request_vote_reply(reply_str, reply)) {
                        std::lock_guard<std::mutex> lk(mtx);
                        if (reply.term > current_term) {
                            current_term = reply.term;
                            role = NodeRole::FOLLOWER;
                            voted_for = -1;
                            break;
                        }
                        if (role == NodeRole::CANDIDATE && current_term == election_term && reply.vote_granted) {
                            votes_received++;
                            if (votes_received > cluster_size / 2) {
                                role = NodeRole::LEADER;
                                std::cout << ">>> [Node " << id << "] WON ELECTION! Became LEADER for Term " << current_term << " <<<\n";
                                last_heartbeat_time = std::chrono::steady_clock::now();
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
};
