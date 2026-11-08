//! This crate offers a replacement for `serde-json`'s `Value` type, which is
//! significantly more memory efficient.
//!
//! As a ballpark figure, it will typically use half as much memory as
//! `serde-json` when deserializing a value and the memory footprint of cloning
//! a value is more than 7x smaller.
//!
//! The primary type exposed by this crate is the [`IValue`] type. It is guaranteed
//! to be pointer-sized and has a niche (so `Option<IValue>` is also guaranteed
//! to be pointer-sized).
//!
//! The Cache-cat copy always synchronizes the shared string cache while retaining
//! the compact string representation. See `UPSTREAM.md` for the local changes.
#![deny(missing_docs, missing_debug_implementations)]

#[macro_use]
mod macros;

pub mod array;
pub mod number;
pub mod object;

pub mod unsafe_string;
pub use unsafe_string::IString;

pub mod convert;
pub mod error;
mod thin;
mod value;

pub use array::{FloatType, IArray};
pub use convert::{TryCollect, TryExtend, TryFromIterator};
pub use number::INumber;
pub use object::IObject;
use std::alloc::Layout;

pub use value::{
    BoolMut, Destructured, DestructuredMut, DestructuredRef, IValue, ValueIndex, ValueType,
};

mod de;
mod ser;
pub use de::{from_value, FPHAConfig, IValueDeserSeed};
pub use ser::to_value;

/// Trait to implement defrag allocator
pub trait DefragAllocator {
    /// Gets a pointer and return an new pointer that points to a copy
    /// of the exact same data. The old pointer should not be used anymore.
    unsafe fn realloc_ptr<T>(&mut self, ptr: *mut T, layout: Layout) -> *mut T;

    /// Allocate memory for defrag
    unsafe fn alloc(&mut self, layout: Layout) -> *mut u8;

    /// Free memory for defrag
    unsafe fn free<T>(&mut self, ptr: *mut T, layout: Layout);
}

/// Trait for object that implements defrag
pub trait Defrag<A: DefragAllocator> {
    /// Defrag implementation
    fn defrag(self, defrag_allocator: &mut A) -> Self;
}
/// Reinitialized the shared strings cache.
/// Any json that still uses a shared string will continue using it.
/// But new strings will be reinitialized instead of reused the old ones.
pub fn reinit_shared_string_cache() {
    unsafe_string::reinit_cache();
}

/// Initializes the shared string cache. This copy is always thread safe; the
/// argument is retained for source compatibility. Returns an error if initialized.
pub fn init_shared_string_cache(thread_safe: bool) -> Result<(), String> {
    unsafe_string::init_cache(thread_safe)
}

#[cfg(all(test, not(miri)))]
mod tests {
    use mockalloc::Mockalloc;
    use std::alloc::System;

    #[global_allocator]
    static ALLOCATOR: Mockalloc<System> = Mockalloc(System);
}
