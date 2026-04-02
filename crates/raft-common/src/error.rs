use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("raft error: {0}")]
    Raft(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("not leader: leader is node {leader_id:?}")]
    NotLeader { leader_id: Option<u64> },

    #[error("key not found")]
    KeyNotFound,

    #[error("transaction conflict")]
    TransactionConflict,

    #[error("corruption: {0}")]
    Corruption(String),
}

pub type Result<T> = std::result::Result<T, Error>;
