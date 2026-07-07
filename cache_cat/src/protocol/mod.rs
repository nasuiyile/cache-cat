pub mod command;
pub mod connection;
pub mod hash;
pub mod key;
pub mod list;
pub mod resp;
pub mod set;
pub mod string;
pub mod transaction;
pub mod zset;

pub mod bitmap;
pub mod pub_sub;
mod raft_command;
pub mod sentinel;
mod server;

#[cfg(all(feature = "lua", feature = "redis"))]
pub mod lua;
#[cfg(all(feature = "lua", feature = "redis"))]
pub mod lua_env;

/// Current format version for all encoded types (stored in high 4 bits of flags)
pub const CURRENT_VERSION: u8 = 1;

/// Data type constants (stored in low 4 bits of flags)
pub const TYPE_STRING: u8 = 0x01;
pub const TYPE_HASH: u8 = 0x02;

/// Special value indicating no expiration (0 means never expire)
pub const NO_EXPIRATION: u64 = 0;
