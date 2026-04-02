use bitvec::prelude::*;
use std::hash::{Hash, Hasher};

/// Bloom filter for fast negative key lookups in SSTables.
///
/// Uses the Kirsch-Mitzenmacker optimization: two hash functions (h1, h2)
/// generate k hash values as `h1 + i * h2` for i in 0..k.
/// This is as effective as k independent hashes for bloom filters.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: BitVec<u8, Lsb0>,
    num_hashes: u32,
    num_bits: usize,
}

impl BloomFilter {
    /// Create a new bloom filter sized for `expected_items` with the given
    /// target false positive rate.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let expected_items = expected_items.max(1);
        let fp = false_positive_rate.clamp(0.0001, 0.5);

        // Optimal number of bits: m = -n * ln(p) / (ln(2))^2
        let num_bits =
            (-(expected_items as f64) * fp.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_bits = num_bits.max(8);

        // Optimal number of hashes: k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()).round() as u32;
        let num_hashes = num_hashes.max(1);

        Self {
            bits: bitvec![u8, Lsb0; 0; num_bits],
            num_hashes,
            num_bits,
        }
    }

    /// Create a bloom filter from raw parts (for deserialization from SSTable).
    pub fn from_raw(bits: Vec<u8>, num_hashes: u32, num_bits: usize) -> Self {
        let mut bitvec = BitVec::from_vec(bits);
        bitvec.resize(num_bits, false);
        Self {
            bits: bitvec,
            num_hashes,
            num_bits,
        }
    }

    /// Insert a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            self.bits.set(idx, true);
        }
    }

    /// Check if a key *might* be in the set.
    /// Returns `false` if definitely not present, `true` if possibly present.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            if !self.bits[idx] {
                return false;
            }
        }
        true
    }

    /// Raw bytes of the bit array (for serialization into SSTable).
    pub fn as_bytes(&self) -> &[u8] {
        self.bits.as_raw_slice()
    }

    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Compute two independent hashes using SipHash with different seeds.
    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        let h1 = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        };
        let h2 = {
            // Use a different seed by mixing h1
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            0xDEAD_BEEF_u64.hash(&mut hasher);
            hasher.finish()
        };
        (h1, h2)
    }

    fn get_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        (h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.num_bits as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_found() {
        let mut bf = BloomFilter::new(100, 0.01);
        for i in 0..100 {
            bf.insert(format!("key-{}", i).as_bytes());
        }
        for i in 0..100 {
            assert!(
                bf.may_contain(format!("key-{}", i).as_bytes()),
                "key-{} should be found",
                i
            );
        }
    }

    #[test]
    fn false_positive_rate() {
        let n = 10_000;
        let target_fpr = 0.01;
        let mut bf = BloomFilter::new(n, target_fpr);

        for i in 0..n {
            bf.insert(format!("inserted-{}", i).as_bytes());
        }

        // Test with keys that were NOT inserted
        let test_count = 100_000;
        let false_positives = (0..test_count)
            .filter(|i| bf.may_contain(format!("not-inserted-{}", i).as_bytes()))
            .count();

        let actual_fpr = false_positives as f64 / test_count as f64;
        // Allow 3x tolerance over target
        assert!(
            actual_fpr < target_fpr * 3.0,
            "FPR {} exceeds 3x target {}",
            actual_fpr,
            target_fpr
        );
    }

    #[test]
    fn empty_filter_returns_false() {
        let bf = BloomFilter::new(100, 0.01);
        assert!(!bf.may_contain(b"anything"));
    }

    #[test]
    fn serialization_roundtrip() {
        let mut bf = BloomFilter::new(100, 0.01);
        for i in 0..50 {
            bf.insert(format!("key-{}", i).as_bytes());
        }

        let bytes = bf.as_bytes().to_vec();
        let num_hashes = bf.num_hashes();
        let num_bits = bf.num_bits();

        let restored = BloomFilter::from_raw(bytes, num_hashes, num_bits);

        for i in 0..50 {
            assert!(restored.may_contain(format!("key-{}", i).as_bytes()));
        }
    }

    #[test]
    fn small_filter() {
        let mut bf = BloomFilter::new(1, 0.01);
        bf.insert(b"hello");
        assert!(bf.may_contain(b"hello"));
    }
}
