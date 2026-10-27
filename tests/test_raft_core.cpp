#include <iostream>
#include <cassert>
#include "../include/raft_node.h"

void test_initial_state() {
    RaftNode node(1, 3);
    assert(node.role == NodeRole::FOLLOWER);
    assert(node.current_term == 0);
    assert(node.voted_for == -1);
    assert(node.log.size() == 1); // 1-indexed dummy entry
    std::cout << "[PASS] test_initial_state\n";
}

void test_request_vote_grant() {
    RaftNode node(1, 3);

    RequestVoteArgs args;
    args.term = 1;
    args.candidate_id = 2;
    args.last_log_index = 0;
    args.last_log_term = 0;

    RequestVoteReply reply = node.handle_request_vote(args);
    assert(reply.vote_granted == true);
    assert(node.voted_for == 2);
    assert(node.current_term == 1);

    // Node should NOT grant vote to another candidate in the same term
    args.candidate_id = 3;
    reply = node.handle_request_vote(args);
    assert(reply.vote_granted == false);

    std::cout << "[PASS] test_request_vote_grant\n";
}

void test_log_replication_and_commit() {
    RaftNode follower(2, 3);

    // Simulate Leader sending AppendEntries with a new key-value entry
    AppendEntriesArgs append_args;
    append_args.term = 1;
    append_args.leader_id = 1;
    append_args.prev_log_index = 0;
    append_args.prev_log_term = 0;
    append_args.leader_commit = 1;

    LogEntry entry;
    entry.term = 1;
    entry.index = 1;
    entry.type = CommandType::SET;
    entry.key = "alpha_key";
    entry.value = "replicated_999";
    append_args.entries.push_back(entry);

    AppendEntriesReply reply = follower.handle_append_entries(append_args);
    assert(reply.success == true);
    assert(follower.commit_index == 1);
    assert(follower.last_applied == 1);
    assert(follower.kv_store["alpha_key"] == "replicated_999");

    std::cout << "[PASS] test_log_replication_and_commit\n";
}

int main() {
    std::cout << "Running Raft Core Logic Unit Tests...\n";
    test_initial_state();
    test_request_vote_grant();
    test_log_replication_and_commit();
    std::cout << "All core Raft tests passed successfully!\n";
    return 0;
}
