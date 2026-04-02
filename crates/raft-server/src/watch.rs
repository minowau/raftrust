use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, warn};

/// The type of mutation event on a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEventType {
    Put,
    Delete,
}

/// A single watch event representing a key mutation.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub mod_revision: u64,
    pub create_revision: u64,
    pub prev_value: Option<Vec<u8>>,
}

/// A watcher subscription filtering events by key/range.
#[derive(Debug)]
struct Watcher {
    key: Vec<u8>,
    range_end: Vec<u8>,
    start_revision: u64,
}

impl Watcher {
    /// Check if this watcher should receive an event.
    fn matches(&self, event: &WatchEvent) -> bool {
        if event.mod_revision < self.start_revision {
            return false;
        }

        if self.range_end.is_empty() {
            // Exact key match
            self.key == event.key
        } else {
            // Range watch: key >= self.key && key < self.range_end
            event.key >= self.key && event.key < self.range_end
        }
    }
}

/// Central hub for watch subscriptions and event broadcasting.
///
/// The apply loop publishes events here after applying each KV mutation.
/// Watch gRPC streams subscribe and receive filtered events.
pub struct WatchHub {
    /// Broadcast channel for all events (watchers filter locally).
    sender: broadcast::Sender<Arc<WatchEvent>>,
    /// Active watchers by ID.
    watchers: RwLock<HashMap<i64, Watcher>>,
    /// Next watcher ID.
    next_id: AtomicI64,
}

impl WatchHub {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            watchers: RwLock::new(HashMap::new()),
            next_id: AtomicI64::new(1),
        }
    }

    /// Publish an event to all watchers.
    pub fn publish(&self, event: WatchEvent) {
        let event = Arc::new(event);
        // If no receivers, that's fine — the event is just dropped.
        let _ = self.sender.send(event);
    }

    /// Create a new watcher. Returns (watch_id, receiver).
    pub fn create_watcher(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        start_revision: u64,
    ) -> (i64, broadcast::Receiver<Arc<WatchEvent>>) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let watcher = Watcher {
            key,
            range_end,
            start_revision,
        };

        self.watchers.write().insert(id, watcher);
        let receiver = self.sender.subscribe();

        debug!(watch_id = id, "Created watcher");
        (id, receiver)
    }

    /// Cancel a watcher.
    pub fn cancel_watcher(&self, id: i64) -> bool {
        let removed = self.watchers.write().remove(&id).is_some();
        if removed {
            debug!(watch_id = id, "Canceled watcher");
        } else {
            warn!(watch_id = id, "Attempted to cancel unknown watcher");
        }
        removed
    }

    /// Check if a watcher should receive a given event.
    pub fn watcher_matches(&self, watch_id: i64, event: &WatchEvent) -> bool {
        let watchers = self.watchers.read();
        watchers.get(&watch_id).is_some_and(|w| w.matches(event))
    }

    /// Number of active watchers.
    pub fn watcher_count(&self) -> usize {
        self.watchers.read().len()
    }
}

impl Default for WatchHub {
    fn default() -> Self {
        Self::new(4096)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_and_receive() {
        let hub = WatchHub::new(16);
        let (id, mut rx) = hub.create_watcher(b"key1".to_vec(), vec![], 0);

        hub.publish(WatchEvent {
            event_type: WatchEventType::Put,
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
            mod_revision: 1,
            create_revision: 1,
            prev_value: None,
        });

        let event = rx.try_recv().unwrap();
        assert!(hub.watcher_matches(id, &event));
        assert_eq!(event.key, b"key1");
        assert_eq!(event.value, b"val1");
    }

    #[test]
    fn exact_key_filter() {
        let hub = WatchHub::new(16);
        let (id, mut rx) = hub.create_watcher(b"key1".to_vec(), vec![], 0);

        // Event for different key — should not match
        hub.publish(WatchEvent {
            event_type: WatchEventType::Put,
            key: b"key2".to_vec(),
            value: b"val".to_vec(),
            mod_revision: 1,
            create_revision: 1,
            prev_value: None,
        });

        let event = rx.try_recv().unwrap();
        assert!(!hub.watcher_matches(id, &event));
    }

    #[test]
    fn range_watch() {
        let hub = WatchHub::new(16);
        // Watch keys in range [a, d)
        let (id, mut rx) = hub.create_watcher(b"a".to_vec(), b"d".to_vec(), 0);

        // "b" is in range
        hub.publish(WatchEvent {
            event_type: WatchEventType::Put,
            key: b"b".to_vec(),
            value: b"val".to_vec(),
            mod_revision: 1,
            create_revision: 1,
            prev_value: None,
        });
        let event = rx.try_recv().unwrap();
        assert!(hub.watcher_matches(id, &event));

        // "d" is NOT in range (exclusive end)
        hub.publish(WatchEvent {
            event_type: WatchEventType::Put,
            key: b"d".to_vec(),
            value: b"val".to_vec(),
            mod_revision: 2,
            create_revision: 2,
            prev_value: None,
        });
        let event = rx.try_recv().unwrap();
        assert!(!hub.watcher_matches(id, &event));
    }

    #[test]
    fn start_revision_filter() {
        let hub = WatchHub::new(16);
        let (id, mut rx) = hub.create_watcher(b"key".to_vec(), vec![], 5);

        // Event at revision 3 — before start_revision
        hub.publish(WatchEvent {
            event_type: WatchEventType::Put,
            key: b"key".to_vec(),
            value: b"old".to_vec(),
            mod_revision: 3,
            create_revision: 1,
            prev_value: None,
        });
        let event = rx.try_recv().unwrap();
        assert!(!hub.watcher_matches(id, &event));

        // Event at revision 5 — at start_revision
        hub.publish(WatchEvent {
            event_type: WatchEventType::Put,
            key: b"key".to_vec(),
            value: b"new".to_vec(),
            mod_revision: 5,
            create_revision: 1,
            prev_value: None,
        });
        let event = rx.try_recv().unwrap();
        assert!(hub.watcher_matches(id, &event));
    }

    #[test]
    fn cancel_watcher() {
        let hub = WatchHub::new(16);
        let (id, _rx) = hub.create_watcher(b"key".to_vec(), vec![], 0);
        assert_eq!(hub.watcher_count(), 1);

        assert!(hub.cancel_watcher(id));
        assert_eq!(hub.watcher_count(), 0);

        // Double cancel returns false
        assert!(!hub.cancel_watcher(id));
    }

    #[test]
    fn multiple_watchers() {
        let hub = WatchHub::new(16);
        let (id1, _rx1) = hub.create_watcher(b"key1".to_vec(), vec![], 0);
        let (id2, _rx2) = hub.create_watcher(b"key2".to_vec(), vec![], 0);
        assert_eq!(hub.watcher_count(), 2);

        // Publish event for key1
        let event = WatchEvent {
            event_type: WatchEventType::Put,
            key: b"key1".to_vec(),
            value: b"val".to_vec(),
            mod_revision: 1,
            create_revision: 1,
            prev_value: None,
        };

        assert!(hub.watcher_matches(id1, &event));
        assert!(!hub.watcher_matches(id2, &event));
    }

    #[test]
    fn delete_event() {
        let hub = WatchHub::new(16);
        let (id, mut rx) = hub.create_watcher(b"key".to_vec(), vec![], 0);

        hub.publish(WatchEvent {
            event_type: WatchEventType::Delete,
            key: b"key".to_vec(),
            value: vec![],
            mod_revision: 2,
            create_revision: 1,
            prev_value: Some(b"old_val".to_vec()),
        });

        let event = rx.try_recv().unwrap();
        assert!(hub.watcher_matches(id, &event));
        assert_eq!(event.event_type, WatchEventType::Delete);
        assert_eq!(event.prev_value, Some(b"old_val".to_vec()));
    }

    #[test]
    fn no_receivers_ok() {
        let hub = WatchHub::new(16);
        // Publishing with no watchers shouldn't panic
        hub.publish(WatchEvent {
            event_type: WatchEventType::Put,
            key: b"key".to_vec(),
            value: b"val".to_vec(),
            mod_revision: 1,
            create_revision: 1,
            prev_value: None,
        });
    }
}
