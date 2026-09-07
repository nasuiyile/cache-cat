use crate::error::ProtocolError;
use crate::raft::types::core::mocha::bloom_filter::BloomError;

pub(super) const NOT_FOUND: &str = "ERR not found";

/// The same Bloom engine error has different Redis replies depending on the command phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BloomOperation {
    Create,
    Insert,
    LoadChunk,
}

pub(super) fn from_engine(error: BloomError, operation: BloomOperation) -> ProtocolError {
    let message = match operation {
        BloomOperation::Create => match error {
            BloomError::OutOfMemory => "ERR Insufficient memory to create filter",
            _ => "ERR could not create filter",
        },
        BloomOperation::Insert => match error {
            BloomError::Full => "ERR non scaling filter is full",
            _ => "ERR problem inserting into filter",
        },
        BloomOperation::LoadChunk => match error {
            BloomError::InvalidDumpOffset => "ERR invalid offset - no link found",
            BloomError::DumpChunkTooBig => "ERR invalid chunk - Too big for current filter",
            BloomError::OutOfMemory => "ERR Insufficient memory to create filter",
            BloomError::BadDumpData
            | BloomError::Overflow
            | BloomError::Invalid
            | BloomError::Full => "ERR received bad data",
        },
    };

    ProtocolError::response(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_engine_errors_using_operation_context() {
        assert_eq!(
            from_engine(BloomError::OutOfMemory, BloomOperation::Create).to_string(),
            "ERR Insufficient memory to create filter"
        );
        assert_eq!(
            from_engine(BloomError::Full, BloomOperation::Insert).to_string(),
            "ERR non scaling filter is full"
        );
        assert_eq!(
            from_engine(BloomError::InvalidDumpOffset, BloomOperation::LoadChunk).to_string(),
            "ERR invalid offset - no link found"
        );
        assert_eq!(
            from_engine(BloomError::Invalid, BloomOperation::LoadChunk).to_string(),
            "ERR received bad data"
        );
        assert_eq!(
            from_engine(BloomError::DumpChunkTooBig, BloomOperation::LoadChunk).to_string(),
            "ERR invalid chunk - Too big for current filter"
        );
        assert_eq!(
            from_engine(BloomError::OutOfMemory, BloomOperation::LoadChunk).to_string(),
            "ERR Insufficient memory to create filter"
        );
    }
}
