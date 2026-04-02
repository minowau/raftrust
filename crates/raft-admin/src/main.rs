use std::env;

/// Generated protobuf client stubs.
mod proto {
    pub mod kv {
        tonic::include_proto!("kv");
    }
    pub mod admin {
        tonic::include_proto!("admin");
    }
}

use proto::admin::admin_service_client::AdminServiceClient;
use proto::kv::kv_service_client::KvServiceClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    let addr = get_arg(&args, "--addr").unwrap_or_else(|| "http://127.0.0.1:5001".to_string());
    let command = &args[1];

    match command.as_str() {
        "put" => {
            let key = args.get(2).expect("Usage: raft-admin put <key> <value>");
            let value = args.get(3).expect("Usage: raft-admin put <key> <value>");

            let mut client = KvServiceClient::connect(addr).await?;
            let resp = client
                .put(proto::kv::PutRequest {
                    key: key.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                    lease_id: 0,
                    ttl_seconds: 0,
                })
                .await?;

            println!("OK (revision: {})", resp.into_inner().revision);
        }
        "get" => {
            let key = args.get(2).expect("Usage: raft-admin get <key>");

            let mut client = KvServiceClient::connect(addr).await?;
            let resp = client
                .get(proto::kv::GetRequest {
                    key: key.as_bytes().to_vec(),
                    linearizable: false,
                })
                .await?;

            let inner = resp.into_inner();
            if inner.revision == 0 && inner.value.is_empty() {
                println!("(not found)");
            } else {
                println!(
                    "{} (revision: {})",
                    String::from_utf8_lossy(&inner.value),
                    inner.revision
                );
            }
        }
        "delete" => {
            let key = args.get(2).expect("Usage: raft-admin delete <key>");

            let mut client = KvServiceClient::connect(addr).await?;
            let resp = client
                .delete(proto::kv::DeleteRequest {
                    key: key.as_bytes().to_vec(),
                })
                .await?;

            let inner = resp.into_inner();
            if inner.deleted {
                println!("Deleted (revision: {})", inner.revision);
            } else {
                println!("Key not found");
            }
        }
        "status" => {
            let mut client = AdminServiceClient::connect(addr).await?;
            let resp = client.status(proto::admin::StatusRequest {}).await?;

            let s = resp.into_inner();
            println!("Node ID:      {}", s.node_id);
            println!("Role:         {}", s.role);
            println!("Term:         {}", s.term);
            println!(
                "Leader:       {}",
                if s.leader_id > 0 {
                    format!("{}", s.leader_id)
                } else {
                    "none".to_string()
                }
            );
            println!("Commit Index: {}", s.commit_index);
            println!("Applied:      {}", s.applied_index);
            println!("Cluster Size: {}", s.cluster_size);
        }
        "transfer" => {
            let target = args
                .get(2)
                .expect("Usage: raft-admin transfer <target_node_id>")
                .parse::<u64>()
                .expect("target must be a number");

            let mut client = AdminServiceClient::connect(addr).await?;
            let resp = client
                .transfer_leadership(proto::admin::TransferLeadershipRequest { transferee: target })
                .await?;

            if resp.into_inner().success {
                println!("Leadership transfer initiated to node {}", target);
            } else {
                println!("Transfer failed");
            }
        }
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }

    Ok(())
}

fn print_usage() {
    eprintln!("Usage: raft-admin <command> [args...] [--addr <server_addr>]");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  put <key> <value>     Write a key-value pair");
    eprintln!("  get <key>             Read a key");
    eprintln!("  delete <key>          Delete a key");
    eprintln!("  status                Show node status");
    eprintln!("  transfer <node_id>    Transfer leadership to a node");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --addr <url>          Server address (default: http://127.0.0.1:5001)");
    eprintln!();
    eprintln!("Example:");
    eprintln!("  raft-admin put mykey myvalue");
    eprintln!("  raft-admin get mykey");
    eprintln!("  raft-admin status --addr http://127.0.0.1:5002");
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
