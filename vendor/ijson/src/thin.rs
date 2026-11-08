use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

#[repr(transparent)]
pub struct ThinRef<'a, T> {
    ptr: NonNull<T>,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> ThinRef<'a, T> {
    pub unsafe fn new(ptr: *const T) -> Self {
        Self {
            ptr: NonNull::new_unchecked(ptr as *mut T),
            phantom: PhantomData,
        }
    }
}

impl<'a, T> Deref for ThinRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr() }
    }
}

impl<'a, T> Copy for ThinRef<'a, T> {}
impl<'a, T> Clone for ThinRef<'a, T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[repr(transparent)]
pub struct ThinMut<'a, T> {
    ptr: NonNull<T>,
    phantom: PhantomData<&'a mut T>,
}

impl<'a, T> ThinMut<'a, T> {
    pub unsafe fn new(ptr: *mut T) -> Self {
        Self {
            ptr: NonNull::new_unchecked(ptr),
            phantom: PhantomData,
        }
    }
}

impl<'a, T> Deref for ThinMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // Safety: `ptr` must be valid
        unsafe { &*self.ptr() }
    }
}

impl<'a, T> DerefMut for ThinMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety: `ptr` must be valid
        unsafe { &mut *self.ptr_mut() }
    }
}

pub trait ThinRefExt<'a, T>: Deref<Target = T> {
    fn ptr(&self) -> *const T;
}

pub trait ThinMutExt<'a, T>: DerefMut<Target = T> + ThinRefExt<'a, T> + Sized {
    fn ptr_mut(&mut self) -> *mut T;
    fn reborrow(&mut self) -> ThinMut<'_, T>;
}

impl<'a, T> ThinRefExt<'a, T> for ThinRef<'a, T> {
    fn ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }
}

impl<'a, T> ThinRefExt<'a, T> for ThinMut<'a, T> {
    fn ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }
}

impl<'a, T> ThinMutExt<'a, T> for ThinMut<'a, T> {
    fn ptr_mut(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }
    fn reborrow(&mut self) -> ThinMut<'_, T> {
        Self {
            ptr: self.ptr,
            phantom: self.phantom,
        }
    }
}
