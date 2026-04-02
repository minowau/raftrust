/// Versioned key encoding for MVCC.
///
/// Internal key format: `[user_key][!revision as 8 bytes BE]`
///
/// The revision is bitwise-inverted so that newer revisions (higher numbers)
/// sort BEFORE older ones in the BTreeMap/SSTable. This means a scan for a
/// user key will encounter the newest version first.
///
/// Example: user_key="foo", revision=5
///   encoded = b"foo" ++ (!5u64).to_be_bytes()
///
/// This ensures all versions of the same key are contiguous and newest-first.

/// Length of the revision suffix in bytes.
pub const REVISION_SUFFIX_LEN: usize = 8;

/// Encode a user key + revision into an internal MVCC key.
pub fn encode_key(user_key: &[u8], revision: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(user_key.len() + REVISION_SUFFIX_LEN);
    encoded.extend_from_slice(user_key);
    encoded.extend_from_slice(&(!revision).to_be_bytes());
    encoded
}

/// Decode an internal MVCC key into (user_key, revision).
pub fn decode_key(encoded: &[u8]) -> (&[u8], u64) {
    let split = encoded.len() - REVISION_SUFFIX_LEN;
    let user_key = &encoded[..split];
    let rev_bytes: [u8; 8] = encoded[split..].try_into().unwrap();
    let revision = !u64::from_be_bytes(rev_bytes);
    (user_key, revision)
}

/// Encode the start of the range for a user key (inclusive, all revisions).
/// This is the smallest possible internal key for this user key (newest revision).
pub fn encode_key_prefix_start(user_key: &[u8]) -> Vec<u8> {
    encode_key(user_key, u64::MAX)
}

/// Encode the end of the range for a user key (exclusive).
/// Any internal key starting with this user key will be < this value.
pub fn encode_key_prefix_end(user_key: &[u8]) -> Vec<u8> {
    // The next user key after `user_key` — append a byte past the revision suffix.
    let mut end = user_key.to_vec();
    // Increment the user key to get the exclusive upper bound
    increment_bytes(&mut end);
    // Append max revision suffix so it sorts after all versions of the incremented key
    end.extend_from_slice(&[0x00; REVISION_SUFFIX_LEN]);
    end
}

/// Increment a byte string (treating it as a big-endian number).
fn increment_bytes(bytes: &mut Vec<u8>) {
    for byte in bytes.iter_mut().rev() {
        if *byte < 0xFF {
            *byte += 1;
            return;
        }
        *byte = 0x00;
    }
    // All bytes were 0xFF — prepend a 0x01
    bytes.insert(0, 0x01);
}

/// Value stored alongside MVCC data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    /// The actual user value, or None if this is a delete marker.
    pub value: Option<Vec<u8>>,
    /// The revision at which this version was created.
    pub create_revision: u64,
    /// The revision at which this version was last modified.
    pub mod_revision: u64,
    /// Associated lease ID (0 = no lease).
    pub lease_id: i64,
    /// TTL in seconds (0 = no expiry).
    pub ttl_seconds: i64,
}

impl VersionedValue {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Tag: 0x01 = live value, 0x02 = delete marker
        match &self.value {
            Some(v) => {
                buf.push(0x01);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            None => {
                buf.push(0x02);
            }
        }
        buf.extend_from_slice(&self.create_revision.to_le_bytes());
        buf.extend_from_slice(&self.mod_revision.to_le_bytes());
        buf.extend_from_slice(&self.lease_id.to_le_bytes());
        buf.extend_from_slice(&self.ttl_seconds.to_le_bytes());
        buf
    }

    pub fn decode(data: &[u8]) -> Self {
        let tag = data[0];
        let (value, offset) = if tag == 0x01 {
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            (Some(data[5..5 + len].to_vec()), 5 + len)
        } else {
            (None, 1)
        };
        let create_revision = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        let mod_revision = u64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap());
        let lease_id = i64::from_le_bytes(data[offset + 16..offset + 24].try_into().unwrap());
        let ttl_seconds = i64::from_le_bytes(data[offset + 24..offset + 32].try_into().unwrap());

        Self {
            value,
            create_revision,
            mod_revision,
            lease_id,
            ttl_seconds,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let encoded = encode_key(b"hello", 42);
        let (user_key, revision) = decode_key(&encoded);
        assert_eq!(user_key, b"hello");
        assert_eq!(revision, 42);
    }

    #[test]
    fn newest_first_ordering() {
        let v1 = encode_key(b"key", 1);
        let v5 = encode_key(b"key", 5);
        let v100 = encode_key(b"key", 100);

        // Higher revision should sort BEFORE lower revision
        assert!(v100 < v5);
        assert!(v5 < v1);
    }

    #[test]
    fn different_keys_separate() {
        let a5 = encode_key(b"a", 5);
        let b1 = encode_key(b"b", 1);

        // "a" keys always before "b" keys regardless of revision
        assert!(a5 < b1);
    }

    #[test]
    fn versioned_value_roundtrip() {
        let vv = VersionedValue {
            value: Some(b"hello".to_vec()),
            create_revision: 1,
            mod_revision: 5,
            lease_id: 42,
            ttl_seconds: 300,
        };
        let encoded = vv.encode();
        let decoded = VersionedValue::decode(&encoded);
        assert_eq!(vv, decoded);
    }

    #[test]
    fn versioned_value_delete_marker() {
        let vv = VersionedValue {
            value: None,
            create_revision: 1,
            mod_revision: 3,
            lease_id: 0,
            ttl_seconds: 0,
        };
        let encoded = vv.encode();
        let decoded = VersionedValue::decode(&encoded);
        assert_eq!(vv, decoded);
    }

    #[test]
    fn prefix_range() {
        let start = encode_key_prefix_start(b"foo");
        let end = encode_key_prefix_end(b"foo");

        // All versions of "foo" should be in [start, end)
        let foo_v1 = encode_key(b"foo", 1);
        let foo_v100 = encode_key(b"foo", 100);
        assert!(foo_v1 >= start);
        assert!(foo_v1 < end);
        assert!(foo_v100 >= start);
        assert!(foo_v100 < end);

        // "fop" should NOT be in range
        let fop = encode_key(b"fop", 1);
        assert!(fop >= end);
    }
}
