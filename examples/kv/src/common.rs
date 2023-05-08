use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

pub fn partition(key: &str, server_nums: u64) -> u64 {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    let hv = h.finish();
    (hv % server_nums) + 1
}