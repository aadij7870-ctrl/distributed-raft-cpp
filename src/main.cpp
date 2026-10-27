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

struct PeerConfig {
    int id;
    int port;
};

void client_worker(int client_fd, RaftNode& node) {
    char buffer[2048];
    while (node.running) {
        std::memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_read <= 0) break;

        std::string msg(buffer);
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
        } else if (msg.rfind("GET", 0) == 0) {
            // Client query: "GET <key>\n"
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

    bind(server_fd, (struct sockaddr*)&address, sizeof(address));
    listen(server_fd, 10);

    std::cout << "[Node " << node.id << "] TCP Server listening on port " << port << std::endl;

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
        std::cerr << "Example: ./raft_node 1 9001\n";
        return 1;
    }

    int node_id = std::stoi(argv[1]);
    int port = std::stoi(argv[2]);

    RaftNode node(node_id, 3);

    std::cout << "Starting Raft Node " << node_id << " (Cluster Size: 3)...\n";
    std::thread server_thread(tcp_server_loop, port, std::ref(node));
    server_thread.join();

    return 0;
}
