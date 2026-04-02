pub mod election;
pub mod leadership_transfer;
pub mod log;
pub mod membership;
pub mod message;
pub mod node;
pub mod read_index;
pub mod replication;
pub mod rpc;
pub mod snapshot;
pub mod state;
pub mod tick;

/// Generated protobuf types for Raft RPCs.
pub mod proto {
    pub mod raft {
        tonic::include_proto!("raft");
    }
    pub mod membership {
        tonic::include_proto!("membership");
    }
}
