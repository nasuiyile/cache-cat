pub mod discard;
pub mod exec;
pub mod multi;

use crate::raft::types::entry::request::Operation;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedOperation {
    pub db_number: u16,
    pub operation: Operation,
}

impl QueuedOperation {
    pub fn new(db_number: u16, operation: Operation) -> Self {
        Self {
            db_number,
            operation,
        }
    }
}

/// Connection-local queue. SELECT changes this database until EXEC commits it.
pub struct TransactionQueue {
    pub db_number: u16,
    pub operations: Vec<QueuedOperation>,
}

impl TransactionQueue {
    pub fn new(db_number: u16) -> Self {
        Self {
            db_number,
            operations: Vec::new(),
        }
    }

    pub fn push(&mut self, operation: Operation) {
        self.operations
            .push(QueuedOperation::new(self.db_number, operation));
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}
