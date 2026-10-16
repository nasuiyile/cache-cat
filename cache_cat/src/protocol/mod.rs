pub mod command;
pub mod connection;
pub mod hash;
pub mod key;
pub mod list;
pub mod lua;
pub mod resp;
pub mod set;
pub mod string;
pub mod transaction;
pub mod zset;

pub mod bf;
pub mod bitmap;
pub mod lua_env;
pub mod pub_sub;
mod raft_command;
pub mod sentinel;
pub mod server;
pub mod stream;
mod test;

/// Special value indicating no expiration (0 means never expire)
pub const NO_EXPIRATION: u64 = 0;

/// Match an option name compared by Redis using `strcasecmp`.
#[inline]
pub(crate) fn option_eq<const N: usize>(arg: &[u8], option: &[u8; N]) -> bool {
    // Check only the expected name and its C-string terminator. A long suffix
    // after NUL needs no scan or allocation.
    (arg.len() == N || arg.get(N) == Some(&0)) && arg[..N].eq_ignore_ascii_case(option)
}
