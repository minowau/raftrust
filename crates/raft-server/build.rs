fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../proto/kv.proto",
                "../../proto/watch.proto",
                "../../proto/lease.proto",
                "../../proto/admin.proto",
            ],
            &["../../proto"],
        )?;
    Ok(())
}
