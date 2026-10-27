#pragma once
#include <string>
#include <sstream>
#include <vector>
#include "rpc_message.h"

class NetworkProtocol {
public:
    // Format: "VOTE_REQ <term> <candidate_id> <last_log_index> <last_log_term>\n"
    static std::string serialize_request_vote(const RequestVoteArgs& args) {
        std::ostringstream ss;
        ss << "VOTE_REQ " << args.term << " " << args.candidate_id << " "
           << args.last_log_index << " " << args.last_log_term << "\n";
        return ss.str();
    }

    // Format: "VOTE_REPLY <term> <vote_granted>\n"
    static std::string serialize_request_vote_reply(const RequestVoteReply& reply) {
        std::ostringstream ss;
        ss << "VOTE_REPLY " << reply.term << " " << (reply.vote_granted ? 1 : 0) << "\n";
        return ss.str();
    }

    // Format: "APPEND_REQ <term> <leader_id> <prev_idx> <prev_term> <leader_commit> <entry_count> [entries...]\n"
    static std::string serialize_append_entries(const AppendEntriesArgs& args) {
        std::ostringstream ss;
        ss << "APPEND_REQ " << args.term << " " << args.leader_id << " "
           << args.prev_log_index << " " << args.prev_log_term << " "
           << args.leader_commit << " " << args.entries.size();
        for (const auto& e : args.entries) {
            ss << " " << e.term << "," << e.index << "," << static_cast<int>(e.type)
               << "," << e.key << "," << e.value;
        }
        ss << "\n";
        return ss.str();
    }

    // Format: "APPEND_REPLY <term> <success> <match_index>\n"
    static std::string serialize_append_entries_reply(const AppendEntriesReply& reply) {
        std::ostringstream ss;
        ss << "APPEND_REPLY " << reply.term << " " << (reply.success ? 1 : 0)
           << " " << reply.match_index << "\n";
        return ss.str();
    }

    static bool parse_request_vote(const std::string& line, RequestVoteArgs& args) {
        std::istringstream ss(line);
        std::string tag;
        if (!(ss >> tag >> args.term >> args.candidate_id >> args.last_log_index >> args.last_log_term)) {
            return false;
        }
        return tag == "VOTE_REQ";
    }

    static bool parse_request_vote_reply(const std::string& line, RequestVoteReply& reply) {
        std::istringstream ss(line);
        std::string tag;
        int granted = 0;
        if (!(ss >> tag >> reply.term >> granted)) return false;
        reply.vote_granted = (granted == 1);
        return tag == "VOTE_REPLY";
    }

    static bool parse_append_entries(const std::string& line, AppendEntriesArgs& args) {
        std::istringstream ss(line);
        std::string tag;
        size_t count = 0;
        if (!(ss >> tag >> args.term >> args.leader_id >> args.prev_log_index
                 >> args.prev_log_term >> args.leader_commit >> count)) {
            return false;
        }
        if (tag != "APPEND_REQ") return false;

        args.entries.clear();
        for (size_t i = 0; i < count; ++i) {
            std::string item;
            if (!(ss >> item)) return false;
            std::replace(item.begin(), item.end(), ',', ' ');
            std::istringstream item_ss(item);
            LogEntry e;
            int type_int = 0;
            item_ss >> e.term >> e.index >> type_int >> e.key >> e.value;
            e.type = static_cast<CommandType>(type_int);
            args.entries.push_back(e);
        }
        return true;
    }

    static bool parse_append_entries_reply(const std::string& line, AppendEntriesReply& reply) {
        std::istringstream ss(line);
        std::string tag;
        int success_val = 0;
        if (!(ss >> tag >> reply.term >> success_val >> reply.match_index)) return false;
        reply.success = (success_val == 1);
        return tag == "APPEND_REPLY";
    }
};
