pub mod client;
pub mod lease;
pub mod watch;

/// Generated protobuf client stubs.
pub mod proto {
    pub mod kv {
        tonic::include_proto!("kv");
    }
    pub mod watch {
        tonic::include_proto!("watch");
    }
    pub mod lease {
        tonic::include_proto!("lease");
    }
    pub mod admin {
        tonic::include_proto!("admin");
    }
}
