// Replication logic is integrated into RaftNode (node.rs):
// - create_append_requests(): generates AppendEntries for all peers
// - handle_append_response(): updates nextIndex/matchIndex and advances commit
// Standalone replication functions will be added in Phase 4 for the async
// replication loop that runs on the leader.
