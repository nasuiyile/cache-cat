//! Functionality relating to the JSON string type

use hashbrown::HashSet;
use std::alloc::{alloc, dealloc, handle_alloc_error, Layout, LayoutError};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::{self, Debug, Formatter};
use std::hash::Hash;
use std::mem::{self, transmute};
use std::ops::Deref;
use std::ptr::{copy_nonoverlapping, NonNull};
use std::sync::{atomic::AtomicU32, Mutex, MutexGuard, OnceLock};

use crate::{
    thin::{ThinMut, ThinMutExt, ThinRef, ThinRefExt},
    value::{TypeTag, ALIGNMENT, TAG_SIZE_BITS},
    Defrag, DefragAllocator, IValue,
};

#[repr(align(8))]
struct Header {
    rc: AtomicU32,
    // We use 32 bits for the length, which allows up to 4 GiB (safely covers 512MB)
    len: u32,
}

trait HeaderRef<'a>: ThinRefExt<'a, Header> {
    fn len(&self) -> usize {
        self.len as usize
    }
    fn str_ptr(&self) -> *const u8 {
        // Safety: pointers to the end of structs are allowed
        unsafe { self.ptr().add(1).cast() }
    }
    fn bytes(&self) -> &'a [u8] {
        // Safety: Header `len` must be accurate
        unsafe { std::slice::from_raw_parts(self.str_ptr(), self.len()) }
    }
    fn str(&self) -> &'a str {
        // Safety: UTF-8 enforced on construction
        unsafe { std::str::from_utf8_unchecked(self.bytes()) }
    }
}

trait HeaderMut<'a>: ThinMutExt<'a, Header> {
    fn str_ptr_mut(mut self) -> *mut u8 {
        // Safety: pointers to the end of structs are allowed
        unsafe { self.ptr_mut().add(1).cast() }
    }
}

impl<'a, T: ThinRefExt<'a, Header>> HeaderRef<'a> for T {}
impl<'a, T: ThinMutExt<'a, Header>> HeaderMut<'a> for T {}

// Constants for inline string storage
const INLINE_STRING_MAX_LEN: usize = 7;
/// Check if a string can be stored inline
fn can_inline_string(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.len() <= INLINE_STRING_MAX_LEN
}

// Every cache access is protected before any reference to its entries is made.
// Short strings bypass this cache entirely.
static STRING_CACHE: OnceLock<Mutex<HashSet<WeakIString>>> = OnceLock::new();

pub(crate) fn reinit_cache() {
    *get_cache_guard().0 = HashSet::new();
}

pub(crate) fn init_cache(_thread_safe: bool) -> Result<(), String> {
    STRING_CACHE
        .set(Mutex::new(HashSet::new()))
        .map_err(|_| "Cache is already initialized".to_owned())
}

struct CacheGuard(MutexGuard<'static, HashSet<WeakIString>>);

impl CacheGuard {
    fn get_or_insert(&mut self, value: &str, f: impl FnOnce(&str) -> WeakIString) -> &WeakIString {
        self.0.get_or_insert_with(value, f)
    }

    fn get_val(&self, val: &str) -> Option<&WeakIString> {
        self.0.get(val)
    }

    fn remove_val(&mut self, val: &str) -> bool {
        self.0.remove(val)
    }

    #[cfg(test)]
    fn check_if_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[cfg(test)]
    fn shrink(&mut self) {
        self.0.shrink_to_fit();
    }
}

fn get_cache_guard() -> CacheGuard {
    CacheGuard(
        STRING_CACHE
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}
struct WeakIString {
    ptr: NonNull<Header>,
}

// Weak handles live only inside STRING_CACHE. The mutex prevents removal while
// a handle is inspected/upgraded; strong references keep immutable bytes alive.
unsafe impl Send for WeakIString {}

impl PartialEq for WeakIString {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl Eq for WeakIString {}
impl Hash for WeakIString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl Deref for WeakIString {
    type Target = str;
    fn deref(&self) -> &str {
        self.borrow()
    }
}

impl Borrow<str> for WeakIString {
    fn borrow(&self) -> &str {
        self.header().str()
    }
}
impl WeakIString {
    fn header(&self) -> ThinRef<'_, Header> {
        // Safety: pointer is always valid
        unsafe { ThinRef::new(self.ptr.as_ptr()) }
    }
    fn upgrade(&self) -> IString {
        unsafe {
            self.header()
                .rc
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            IString(IValue::new_ptr(
                self.ptr.as_ptr().cast::<u8>(),
                TypeTag::StringOrNull,
            ))
        }
    }
}

/// The `IString` type is an interned, immutable string, and is where this crate
/// gets its name.
///
/// Cloning an `IString` is cheap, and it can be easily converted from `&str` or
/// `String` types. Comparisons between `IString`s is a simple pointer
/// comparison.
///
/// The memory backing an `IString` is reference counted, so that unlike many
/// string interning libraries, memory is not leaked as new strings are interned.
/// A shared mutex protects the intern table. Already-interned strings use atomic
/// reference counts; inline strings and ordinary string reads never take it.
///
/// Given the nature of `IString` it is better to intern a string once and reuse
/// it, rather than continually convert from `&str` to `IString`.
#[repr(transparent)]
#[derive(Clone)]
pub struct IString(pub(crate) IValue);

value_subtype_impls!(IString, into_string, as_string, as_string_mut);

static EMPTY_HEADER: Header = Header {
    len: 0,
    rc: AtomicU32::new(0),
};

impl IString {
    fn layout(len: usize) -> Result<Layout, LayoutError> {
        Ok(Layout::new::<Header>()
            .extend(Layout::array::<u8>(len)?)?
            .0
            .pad_to_align())
    }

    fn alloc<A: FnOnce(Layout) -> *mut u8>(s: &str, allocator: A) -> *mut Header {
        assert!((s.len()) < u32::MAX as usize);
        unsafe {
            let layout = Self::layout(s.len()).expect("valid string layout");
            let ptr = allocator(layout).cast::<Header>();
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            ptr.write(Header {
                len: s.len() as u32,
                rc: AtomicU32::new(0),
            });
            let hd = ThinMut::new(ptr);
            copy_nonoverlapping(s.as_ptr(), hd.str_ptr_mut(), s.len());
            ptr
        }
    }

    fn dealloc<D: FnOnce(*mut u8, Layout)>(ptr: *mut Header, deallocator: D) {
        unsafe {
            let hd = ThinRef::new(ptr);
            let layout = Self::layout(hd.len()).unwrap();
            deallocator(ptr.cast::<u8>(), layout);
        }
    }

    fn intern_with_allocator<A: FnOnce(Layout) -> *mut u8>(s: &str, allocator: A) -> Self {
        if s.is_empty() {
            return Self::new();
        }

        let mut cache = get_cache_guard();

        let k = cache.get_or_insert(s, |s| WeakIString {
            ptr: unsafe { NonNull::new_unchecked(Self::alloc(s, allocator)) },
        });
        k.upgrade()
    }

    /// Create an inline string by storing bytes in upper bits
    /// Safety: String must be < 8 bytes and valid UTF-8
    unsafe fn new_inline_string(s: &str) -> Self {
        // 1 byte for the tag(3 bits for tag and rest for the length), 7 bytes for the string
        let bytes = s.as_bytes();
        let mut data_bytes = [0u8; 8];

        // Set the length in the first byte (after tag bits)
        data_bytes[0] = (s.len() << TAG_SIZE_BITS) as u8;
        data_bytes[1..1 + bytes.len()].copy_from_slice(bytes);
        let data: usize = usize::from_ne_bytes(data_bytes);

        Self(IValue::new_ptr(data as *mut u8, TypeTag::InlineString))
    }

    /// Converts a `&str` to an `IString` by interning it in the global string cache.
    #[must_use]
    pub fn intern(s: &str) -> Self {
        if s.is_empty() {
            return Self::new();
        } else if can_inline_string(s) {
            unsafe { Self::new_inline_string(s) }
        } else {
            Self::intern_with_allocator(s, |layout| unsafe { alloc(layout) })
        }
    }

    fn is_inline(&self) -> bool {
        (self.0.ptr_usize() % ALIGNMENT) == TypeTag::InlineString as usize
    }

    fn header(&self) -> ThinRef<'_, Header> {
        unsafe { ThinRef::new(self.0.ptr().cast()) }
    }

    /// Returns the length (in bytes) of this string.
    #[must_use]
    pub fn len(&self) -> usize {
        if self.is_inline() {
            let data = self.0.ptr_usize() as u64;
            let len_data = (data & 0xFF) >> TAG_SIZE_BITS;
            len_data as usize
        } else {
            self.header().len()
        }
    }

    /// Returns `true` if this is the empty string "".
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Extract string from inline storage
    /// Safety: Must be called on inline string(strings are valid UTF-8)
    unsafe fn extract_inline_str(&self) -> &str {
        let data_ptr = &self.0 as *const IValue as *const u8;
        let bytes: &[u8; 8] = transmute(data_ptr);
        str::from_utf8_unchecked(&bytes[1..self.len() + 1])
    }

    /// Obtains a `&str` from this `IString`. This is a cheap operation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        if self.is_inline() {
            unsafe { self.extract_inline_str() }
        } else {
            self.header().str()
        }
    }

    /// Obtains a byte slice from this `IString`. This is a cheap operation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }

    /// Returns the empty string.
    #[must_use]
    pub fn new() -> Self {
        unsafe { IString(IValue::new_ref(&EMPTY_HEADER, TypeTag::StringOrNull)) }
    }

    pub(crate) fn clone_impl(&self) -> IValue {
        if self.is_empty() {
            Self::new().0
        } else if self.is_inline() {
            unsafe { self.0.raw_copy() }
        } else {
            self.header()
                .rc
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            unsafe { self.0.raw_copy() }
        }
    }

    fn drop_impl_with_deallocator<D: FnOnce(*mut u8, Layout)>(&mut self, deallocator: D) {
        if !self.is_empty() && !self.is_inline() {
            let hd = self.header();

            {
                // Optimization for the thread safe case, we want to avoid locking the cache if the ref count
                // is not potentially going to reach zero.
                let mut rc = hd.rc.load(std::sync::atomic::Ordering::Relaxed);
                while rc > 1 {
                    match hd.rc.compare_exchange_weak(
                        rc,
                        rc - 1,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    ) {
                        Ok(_) => return,
                        Err(new_rc) => rc = new_rc,
                    }
                }
            }

            let mut cache = get_cache_guard();
            if hd.rc.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) == 1 {
                // Reference count reached zero, free the string
                if let Some(element) = cache.get_val(hd.str()) {
                    // we can not simply remove the element from the cache, while we
                    // perform active defrag, the element might be in the cache but will
                    // point to another (newer) value. In this case we do not want to remove it.
                    if element.ptr.as_ptr().cast() == unsafe { self.0.ptr() } {
                        cache.remove_val(hd.str());
                    }
                }

                // Shrink the cache if it is empty in tests to verify no memory leaks
                #[cfg(test)]
                if cache.check_if_empty() {
                    cache.shrink();
                }
                Self::dealloc(unsafe { self.0.ptr().cast() }, deallocator);
            }
        }
    }

    pub(crate) fn drop_impl(&mut self) {
        self.drop_impl_with_deallocator(|ptr, layout| unsafe { dealloc(ptr, layout) });
    }

    pub(crate) fn mem_allocated(&self) -> usize {
        if self.is_empty() || self.is_inline() {
            0
        } else {
            Self::layout(self.len()).unwrap().size()
        }
    }
}

impl Deref for IString {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for IString {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl From<&str> for IString {
    fn from(other: &str) -> Self {
        Self::intern(other)
    }
}

impl From<&mut str> for IString {
    fn from(other: &mut str) -> Self {
        Self::intern(other)
    }
}

impl From<String> for IString {
    fn from(other: String) -> Self {
        Self::intern(other.as_str())
    }
}

impl From<&String> for IString {
    fn from(other: &String) -> Self {
        Self::intern(other.as_str())
    }
}

impl From<&mut String> for IString {
    fn from(other: &mut String) -> Self {
        Self::intern(other.as_str())
    }
}

impl From<IString> for String {
    fn from(other: IString) -> Self {
        other.as_str().into()
    }
}

impl PartialEq for IString {
    fn eq(&self, other: &Self) -> bool {
        if self.0.raw_eq(&other.0) {
            // if we have the same exact point we know they are equals.
            return true;
        }
        // otherwise we need to compare the strings.
        let s1 = self.as_str();
        let s2 = other.as_str();
        let res = s1 == s2;
        res
    }
}

impl PartialEq<str> for IString {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<IString> for str {
    fn eq(&self, other: &IString) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<String> for IString {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<IString> for String {
    fn eq(&self, other: &IString) -> bool {
        self == other.as_str()
    }
}

impl Default for IString {
    fn default() -> Self {
        Self::new()
    }
}

impl Eq for IString {}
impl Ord for IString {
    fn cmp(&self, other: &Self) -> Ordering {
        if self == other {
            Ordering::Equal
        } else {
            self.as_str().cmp(other.as_str())
        }
    }
}
impl PartialOrd for IString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Hash for IString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl Debug for IString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.as_str(), f)
    }
}

impl<A: DefragAllocator> Defrag<A> for IString {
    fn defrag(mut self, defrag_allocator: &mut A) -> Self {
        let new = Self::intern_with_allocator(self.as_str(), |layout| unsafe {
            defrag_allocator.alloc(layout)
        });
        self.drop_impl_with_deallocator(|ptr, layout| unsafe {
            defrag_allocator.free(ptr, layout)
        });
        mem::forget(self);
        new
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockalloc::record_allocs;

    fn assert_no_allocs<F: FnOnce()>(f: F) {
        let alloc_info = record_allocs(f);
        assert_eq!(
            alloc_info.num_allocs(),
            0,
            "Expected zero allocations, but {} occurred",
            alloc_info.num_allocs()
        );
    }

    #[test]
    fn test_inline_string_as_str() {
        assert_no_allocs(|| {
            let s = IString::intern("hello");
            assert_eq!(s.as_str(), "hello");
        });
    }

    #[mockalloc::test]
    fn can_intern() {
        let x = IString::intern("foofoofoo");
        let y = IString::intern("bar");
        let z = IString::intern("foofoofoo");

        assert_eq!(x.as_ptr(), z.as_ptr());
        assert_ne!(x.as_ptr(), y.as_ptr());
        assert_eq!(x.as_str(), "foofoofoo");
        assert_eq!(y.as_str(), "bar");
    }

    #[test]
    fn default_interns_string() {
        assert_no_allocs(|| {
            let x = IString::intern("");
            let y = IString::new();
            let z = IString::intern("foo");

            assert_eq!(x.as_ptr(), y.as_ptr());
            assert_ne!(x.as_ptr(), z.as_ptr());
        });
    }

    #[mockalloc::test]
    fn test_inline_strings() {
        // Test strings that should be stored inline (≤ 7 bytes)
        let short_strings = ["", "a", "hi", "hello", "world", "1234567", "12345678"];

        for s in &short_strings {
            let istr = IString::intern(s);

            if s.is_empty() {
                // Empty strings use static header, not inline
                assert!(!istr.is_inline());
            } else if s.len() <= INLINE_STRING_MAX_LEN {
                assert!(istr.is_inline(), "String '{}' should be inline", s);
                assert_eq!(istr.as_str(), *s);
                assert_eq!(istr.len(), s.len());
                assert_eq!(istr.as_bytes(), s.as_bytes());

                // Inline strings should have minimal memory overhead
                assert_eq!(istr.mem_allocated(), 0);
            } else {
                assert!(!istr.is_inline(), "String '{}' should not be inline", s);
            }
        }
    }

    #[mockalloc::test]
    fn test_heap_strings() {
        // Test strings that should be stored on heap (> 7 bytes)
        let long_string = "a".repeat(100);
        let long_strings = ["12345678", "toolongstring", &long_string];

        for s in &long_strings {
            let istr = IString::intern(s);
            assert!(!istr.is_inline(), "String '{}' should not be inline", s);
            assert_eq!(istr.as_str(), *s);
            assert_eq!(istr.len(), s.len());
            assert_eq!(istr.as_bytes(), s.as_bytes());

            // Heap strings should have memory overhead
            assert!(istr.mem_allocated() > 0);
        }
    }

    #[mockalloc::test]
    fn test_utf8_boundary_safety() {
        // Test that we don't inline strings that would break UTF-8 boundaries
        let emoji = "🦀"; // 4 bytes in UTF-8
        let multi_emoji = "🦀🔥"; // 8 bytes in UTF-8 - too long for inline

        let crab = IString::intern(emoji);
        assert!(crab.is_inline(), "Single emoji should be inline");
        assert_eq!(crab.as_str(), emoji);

        let fire_crab = IString::intern(multi_emoji);
        assert!(!fire_crab.is_inline(), "Two emojis should not be inline");
        assert_eq!(fire_crab.as_str(), multi_emoji);
    }

    #[test]
    fn test_inline_string_cloning() {
        assert_no_allocs(|| {
            let original = IString::intern("hello");
            assert!(original.is_inline());

            let cloned = original.clone();
            assert!(cloned.is_inline());
            assert_eq!(original.as_str(), cloned.as_str());

            // Both should point to the same inline data
            assert_eq!(original.0.ptr_usize(), cloned.0.ptr_usize());
        });
    }
}
