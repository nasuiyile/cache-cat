//! Fallible iterator / collection-conversion traits shared by [`IArray`] and
//! [`IObject`]. These mirror the standard `Extend`, `FromIterator`, and
//! `Iterator::collect`, but return a `Result`.
//!
//! [`IArray`]: crate::IArray
//! [`IObject`]: crate::IObject

use crate::error::IJsonError;

/// Trait for types that can be fallibly extended from an iterator.
/// This is similar to the standard `Extend` trait, but allows for allocation failures.
pub trait TryExtend<T> {
    /// Attempts to extend `self` by appending items from the given iterator.
    /// # Errors
    /// Returns an `AllocError` if memory allocation fails during the extension.
    fn try_extend(&mut self, iter: impl IntoIterator<Item = T>) -> Result<(), IJsonError>;
}

/// Trait for types that can be fallibly constructed from an iterator.
/// This is similar to the standard `FromIterator` trait, but allows for allocation failures.
pub trait TryFromIterator<T> {
    /// Attempts to create an instance of `Self` from an iterator of items of type `T`.
    /// # Errors
    /// Returns `AllocError` if memory allocation fails during the construction.
    fn try_from_iter<U: IntoIterator<Item = T>>(iter: U) -> Result<Self, IJsonError>
    where
        Self: Sized;
}

/// Extension trait for iterators to collect into a fallible collection.
/// This is similar to the standard `collect` method, but allows for allocation failures.
pub trait TryCollect<T>: Iterator<Item = T> + Sized {
    /// Attempts to collect the iterator into a collection `B`.
    /// # Errors
    /// Returns `AllocError` if memory allocation fails during the collection.
    fn try_collect<B>(self) -> Result<B, IJsonError>
    where
        B: TryFromIterator<T>;
}

impl<T, I: Iterator<Item = T>> TryCollect<T> for I {
    fn try_collect<B>(self) -> Result<B, IJsonError>
    where
        B: TryFromIterator<T>,
    {
        B::try_from_iter(self)
    }
}
