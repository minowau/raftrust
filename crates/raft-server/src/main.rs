use raft_server::server::ServerConfig;
use std::collections::HashMap;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse CLI args: raft-server --id <N> --peers <id=addr,id=addr,...> [--addr <listen>] [--http <addr>] [--data-dir <path>]
    let args: Vec<String> = env::args().collect();

    let node_id = get_arg(&args, "--id")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| {
            eprintln!("Usage: raft-server --id <node_id> --peers <id=addr,id=addr,...>");
            eprintln!();
            eprintln!("Example (3-node cluster):");
            eprintln!("  Terminal 1: raft-server --id 1 --peers 1=http://127.0.0.1:5001,2=http://127.0.0.1:5002,3=http://127.0.0.1:5003");
            eprintln!("  Terminal 2: raft-server --id 2 --peers 1=http://127.0.0.1:5001,2=http://127.0.0.1:5002,3=http://127.0.0.1:5003");
            eprintln!("  Terminal 3: raft-server --id 3 --peers 1=http://127.0.0.1:5001,2=http://127.0.0.1:5002,3=http://127.0.0.1:5003");
            eprintln!();
            eprintln!("Options:");
            eprintln!("  --id <N>           Node ID (required)");
            eprintln!("  --peers <spec>     Comma-separated id=addr pairs (required)");
            eprintln!("  --data-dir <path>  Data directory (default: data/node-<id>)");
            eprintln!("  --http <addr>      HTTP address for /metrics,/health,/ready (default: 127.0.0.1:909<id>)");
            std::process::exit(1);
        });

    let peers_str = get_arg(&args, "--peers").unwrap_or_else(|| {
        eprintln!("Error: --peers is required");
        std::process::exit(1);
    });

    let cluster = parse_peers(&peers_str);
    if cluster.is_empty() {
        eprintln!("Error: --peers must contain at least one entry (format: id=addr,id=addr,...)");
        std::process::exit(1);
    }

    let listen_addr = cluster.get(&node_id).cloned().unwrap_or_else(|| {
        eprintln!("Error: node_id {} not found in --peers", node_id);
        std::process::exit(1);
    });

    // Strip http:// prefix for binding (tonic binds to socket addr, not URL)
    let bind_addr = listen_addr
        .strip_prefix("http://")
        .unwrap_or(&listen_addr)
        .to_string();

    let data_dir = get_arg(&args, "--data-dir").unwrap_or_else(|| format!("data/node-{}", node_id));

    let http_addr = get_arg(&args, "--http").unwrap_or_else(|| format!("127.0.0.1:909{}", node_id));

    tracing::info!(
        node_id = node_id,
        listen = %bind_addr,
        http = %http_addr,
        data_dir = %data_dir,
        cluster_size = cluster.len(),
        "Starting raft server"
    );

    let config = ServerConfig {
        node_id,
        cluster,
        data_dir,
        listen_addr: bind_addr,
        http_addr: Some(http_addr),
        election_timeout_min_ms: 10000,
        election_timeout_max_ms: 15000,
        heartbeat_interval_ms: 1000,
    };

    let server = raft_server::server::RaftServer::new(config)?;
    server.run().await?;

    Ok(())
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn parse_peers(s: &str) -> HashMap<u64, String> {
    let mut map = HashMap::new();
    for pair in s.split(',') {
        let pair = pair.trim();
        if let Some((id_str, addr)) = pair.split_once('=') {
            if let Ok(id) = id_str.trim().parse::<u64>() {
                map.insert(id, addr.trim().to_string());
            }
        }
    }
    map
}
