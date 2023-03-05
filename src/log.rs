pub struct LogID {
    pub term: u64,
    pub index: u64,
}

pub struct LogEntry {
    pub id: LogID,
    pub content: [u8],
}
