pub mod command;
pub mod connection;
pub mod key;
pub mod resp;
pub mod string;

/// Current format version for all encoded types (stored in high 4 bits of flags)
pub const CURRENT_VERSION: u8 = 1;

/// Data type constants (stored in low 4 bits of flags)
pub const TYPE_STRING: u8 = 0x01;
pub const TYPE_HASH: u8 = 0x02;

/// Special value indicating no expiration (0 means never expire)
pub const NO_EXPIRATION: u64 = 0;
