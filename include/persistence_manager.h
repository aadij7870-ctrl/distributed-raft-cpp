#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <sys/stat.h>
#include "log_entry.h"

class PersistenceManager {
public:
    static std::string get_wal_path(int node_id) {
        mkdir("data", 0777);
        return "data/raft_node_" + std::to_string(node_id) + ".wal";
    }

    static void save_state(int node_id, uint64_t current_term, int voted_for, const std::vector<LogEntry>& log) {
        std::string path = get_wal_path(node_id);
        std::string tmp_path = path + ".tmp";

        std::ofstream out(tmp_path, std::ios::trunc);
        if (!out.is_open()) return;

        // Write Term and VotedFor metadata
        out << "META " << current_term << " " << voted_for << "\n";

        // Write Log Entries
        for (const auto& entry : log) {
            out << "ENTRY " << entry.term << " " << entry.index << " "
                << static_cast<int>(entry.type) << " "
                << (entry.key.empty() ? "_" : entry.key) << " "
                << (entry.value.empty() ? "_" : entry.value) << "\n";
        }

        out.flush();
        out.close();

        // Atomic file swap to avoid corrupted writes during crash
        rename(tmp_path.c_str(), path.c_str());
    }

    static bool load_state(int node_id, uint64_t& current_term, int& voted_for, std::vector<LogEntry>& log) {
        std::string path = get_wal_path(node_id);
        std::ifstream in(path);
        if (!in.is_open()) return false;

        std::string line;
        bool loaded_meta = false;
        std::vector<LogEntry> restored_log;

        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream ss(line);
            std::string tag;
            ss >> tag;

            if (tag == "META") {
                ss >> current_term >> voted_for;
                loaded_meta = true;
            } else if (tag == "ENTRY") {
                LogEntry entry;
                int type_int = 0;
                std::string k, v;
                ss >> entry.term >> entry.index >> type_int >> k >> v;
                entry.type = static_cast<CommandType>(type_int);
                entry.key = (k == "_") ? "" : k;
                entry.value = (v == "_") ? "" : v;
                restored_log.push_back(entry);
            }
        }

        if (loaded_meta && !restored_log.empty()) {
            log = restored_log;
            return true;
        }

        return false;
    }
};
