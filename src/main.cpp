#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include "../include/raft_node.h"
#include "../include/network_protocol.h"

void client_worker(int client_fd, RaftNode& node) {
    char buffer[2048];
    std::memset(buffer, 0, sizeof(buffer));
    ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    
    if (bytes_read > 0) {
        std::string msg(buffer, bytes_read);
        std::string response;

        if (msg.rfind("VOTE_REQ", 0) == 0) {
            RequestVoteArgs args;
            if (NetworkProtocol::parse_request_vote(msg, args)) {
                auto reply = node.handle_request_vote(args);
                response = NetworkProtocol::serialize_request_vote_reply(reply);
            }
        } else if (msg.rfind("APPEND_REQ", 0) == 0) {
            AppendEntriesArgs args;
            if (NetworkProtocol::parse_append_entries(msg, args)) {
                auto reply = node.handle_append_entries(args);
                response = NetworkProtocol::serialize_append_entries_reply(reply);
            }
        } else if (msg.rfind("SET", 0) == 0) {
            std::istringstream ss(msg);
            std::string cmd, key, val;
            ss >> cmd >> key >> val;

            std::unique_lock<std::mutex> lock(node.mtx);
            if (node.role != NodeRole::LEADER) {
                response = "ERR_NOT_LEADER\n";
            } else {
                LogEntry new_entry;
                new_entry.term = node.current_term;
                new_entry.index = node.log.size();
                new_entry.type = CommandType::SET;
                new_entry.key = key;
                new_entry.value = val;
                node.log.push_back(new_entry);

                uint64_t target_index = new_entry.index;
                uint64_t current_term = node.current_term;
                auto peers = node.peers;
                auto acks = std::make_shared<std::atomic<int>>(1);
                lock.unlock();

                AppendEntriesArgs replicate_args;
                replicate_args.term = current_term;
                replicate_args.leader_id = node.id;
                replicate_args.prev_log_index = target_index - 1;
                replicate_args.prev_log_term = node.log[target_index - 1].term;
                replicate_args.entries.push_back(new_entry);
                replicate_args.leader_commit = node.commit_index;

                std::string payload = NetworkProtocol::serialize_append_entries(replicate_args);

                for (const auto& p : peers) {
                    std::thread([p, payload, acks, &node]() {
                        std::string reply_str = RaftNode::send_rpc(p.port, payload);
                        AppendEntriesReply reply;
                        if (NetworkProtocol::parse_append_entries_reply(reply_str, reply) && reply.success) {
                            (*acks)++;
                        }
                    }).detach();
                }

                auto start_time = std::chrono::steady_clock::now();
                bool committed = false;
                while (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count() < 350) {
                    if (*acks >= 2) {
                        committed = true;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }

                if (committed) {
                    std::lock_guard<std::mutex> lk(node.mtx);
                    if (target_index > node.commit_index) {
                        node.commit_index = target_index;
                        node.apply_logs_to_state_machine();
                    }
                    response = "OK\n";
                } else {
                    response = "ERR_REPLICATION_TIMEOUT\n";
                }
            }
        } else if (msg.rfind("GET", 0) == 0) {
            std::istringstream ss(msg);
            std::string cmd, key;
            ss >> cmd >> key;
            std::lock_guard<std::mutex> lock(node.mtx);
            auto it = node.kv_store.find(key);
            if (it != node.kv_store.end()) {
                response = "VALUE " + it->second + "\n";
            } else {
                response = "NOT_FOUND\n";
            }
        } else {
            response = "ERR_UNKNOWN_CMD\n";
        }

        if (!response.empty()) {
            send(client_fd, response.c_str(), response.size(), 0);
        }
    }
    shutdown(client_fd, SHUT_WR);
    close(client_fd);
}

void tcp_server_loop(int port, RaftNode& node) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "[Node " << node.id << "] Failed to bind port " << port << std::endl;
        return;
    }
    listen(server_fd, 64);

    std::cout << "[Node " << node.id << "] Server listening on TCP port " << port << std::endl;

    while (node.running) {
        sockaddr_in client_addr{};
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addrlen);
        if (client_fd >= 0) {
            std::thread(client_worker, client_fd, std::ref(node)).detach();
        }
    }
    close(server_fd);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: ./raft_node <node_id> <port>\n";
        return 1;
    }

    int node_id = std::stoi(argv[1]);
    int port = std::stoi(argv[2]);

    RaftNode node(node_id, 3);

    std::vector<PeerInfo> peers;
    for (int id = 1; id <= 3; ++id) {
        if (id != node_id) {
            peers.push_back({id, 9000 + id});
        }
    }
    node.set_peers(peers);

    std::cout << "Starting Raft Node " << node_id << " on port " << port << "...\n";

    std::thread server_thread(tcp_server_loop, port, std::ref(node));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::thread raft_loop_thread(&RaftNode::run_event_loop, &node);

    server_thread.join();
    raft_loop_thread.join();

    return 0;
}
