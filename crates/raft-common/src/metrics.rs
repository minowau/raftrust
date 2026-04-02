use prometheus::Registry;

/// Creates a new Prometheus registry with default process and runtime metrics.
pub fn create_registry() -> Registry {
    Registry::new()
}
