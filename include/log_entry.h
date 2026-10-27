#pragma once
#include <string>
#include <cstdint>

enum class CommandType {
    SET,
    DEL,
    NOOP
};

struct LogEntry {
    uint64_t term{0};       // Election term when entry was received by leader
    uint64_t index{0};      // Monotonically increasing index in the log
    CommandType type{CommandType::NOOP};
    std::string key;
    std::string value;

    // Wire serialization: "term index type key value\n"
    std::string serialize() const {
        return std::to_string(term) + " " +
               std::to_string(index) + " " +
               std::to_string(static_cast<int>(type)) + " " +
               key + " " +
               value;
    }
};
