/// Define propose data to oceanraft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KVData {
    key: String,
    value: Vec<u8>,
}

/// Define propose response to oceanraft.
pub struct Response {
    pub index: u64,
    pub term: u64,
}
