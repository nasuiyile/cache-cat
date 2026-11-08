//! Module for fallible allocation

use std::error::Error;
use std::fmt;
use thiserror::Error;

use crate::FloatType;

/// Error type for fallible allocation
/// This error is returned when an allocation fails.
/// It does not contain any additional information.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct AllocError;

impl Error for AllocError {}

impl fmt::Display for AllocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("memory allocation failed")
    }
}

/// Error type for ijson
#[derive(Error, Debug)]
pub enum IJsonError {
    /// Memory allocation failed
    #[error("memory allocation failed")]
    Alloc(#[from] AllocError),
    /// Value out of range for the specified floating-point type
    #[error("value out of range for {0}")]
    OutOfRange(FloatType),
}
