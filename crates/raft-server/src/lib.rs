pub mod admin_service;
pub mod apply;
pub mod backup;
pub mod http;
pub mod kv_service;
pub mod lease_service;
pub mod server;
pub mod watch_service;

/// Generated protobuf types for client-facing services.
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
