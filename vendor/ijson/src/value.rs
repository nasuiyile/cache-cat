use half::{bf16, f16};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fmt::{self, Debug, Formatter};
use std::hash::Hash;
use std::mem;
use std::ops::{Deref, Index, IndexMut};
use std::ptr::NonNull;

use crate::{Defrag, DefragAllocator, IArray, INumber, IObject, IString};

/// Stores an arbitrary JSON value.
///
/// Compared to [`serde_json::Value`] this type is a struct rather than an enum, as
/// this is necessary to achieve the important size reductions. This means that
/// you cannot directly `match` on an `IValue` to determine its type.
///
/// Instead, an `IValue` offers several ways to get at the inner type:
///
/// - Destructuring using `IValue::destructure[{_ref,_mut}]()`
///
///   These methods return wrapper enums which you _can_ directly match on, so
///   these methods are the most direct replacement for matching on a `Value`.
///
/// - Borrowing using `IValue::as_{array,object,string,number}[_mut]()`
///
///   These methods return an `Option` of the corresponding reference if the
///   type matches the one expected. These methods exist for the variants
///   which are not `Copy`.
///
/// - Converting using `IValue::into_{array,object,string,number}()`
///
///   These methods return a `Result` of the corresponding type (or the
///   original `IValue` if the type is not the one expected). These methods
///   also exist for the variants which are not `Copy`.
///
/// - Getting using `IValue::to_{bool,{i,u,f}{32,64}[_lossy]}()`
///
///   These methods return an `Option` of the corresponding type. These
///   methods exist for types where the return value would be `Copy`.
///
/// You can also check the type of the inner value without specifically
/// accessing it using one of these methods:
///
/// - Checking using `IValue::is_{null,bool,number,string,array,object,true,false}()`
///
///   These methods exist for all types.
///
/// - Getting the type with [`IValue::type_`]
///
///   This method returns the [`ValueType`] enum, which has a variant for each of the
///   six JSON types.
#[repr(transparent)]
pub struct IValue {
    ptr: NonNull<u8>,
}

/// Enum returned by [`IValue::destructure`] to allow matching on the type of
/// an owned [`IValue`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destructured {
    /// Null.
    Null,
    /// Boolean.
    Bool(bool),
    /// Number.
    Number(INumber),
    /// String.
    String(IString),
    /// Array.
    Array(IArray),
    /// Object.
    Object(IObject),
}

impl Destructured {
    /// Convert to the borrowed form of thie enum.
    #[must_use]
    pub fn as_ref(&self) -> DestructuredRef<'_> {
        use DestructuredRef::{Array, Bool, Null, Number, Object, String};
        match self {
            Self::Null => Null,
            Self::Bool(b) => Bool(*b),
            Self::Number(v) => Number(v),
            Self::String(v) => String(v),
            Self::Array(v) => Array(v),
            Self::Object(v) => Object(v),
        }
    }
}

/// Enum returned by [`IValue::destructure_ref`] to allow matching on the type of
/// a reference to an [`IValue`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DestructuredRef<'a> {
    /// Null.
    Null,
    /// Boolean.
    /// [`IValue`]s do not directly contain booleans, so the value is returned
    /// directly instead of as a reference.
    Bool(bool),
    /// Number.
    Number(&'a INumber),
    /// String.
    String(&'a IString),
    /// Array.
    Array(&'a IArray),
    /// Object.
    Object(&'a IObject),
}

/// Enum returned by [`IValue::destructure_mut`] to allow matching on the type of
/// a mutable reference to an [`IValue`].
#[derive(Debug)]
pub enum DestructuredMut<'a> {
    /// Null.
    Null,
    /// Boolean.
    /// [`IValue`]s do not directly contain booleans, so this variant contains
    /// a proxy type which allows getting and setting the original [`IValue`]
    /// as a `bool`.
    Bool(BoolMut<'a>),
    /// Number.
    Number(&'a mut INumber),
    /// String.
    String(&'a mut IString),
    /// Array.
    Array(&'a mut IArray),
    /// Object.
    Object(&'a mut IObject),
}

/// A proxy type which imitates a `&mut bool`.
#[derive(Debug)]
pub struct BoolMut<'a>(&'a mut IValue);

impl<'a> BoolMut<'a> {
    /// Set the [`IValue`] referenced by this proxy type to either
    /// `true` or `false`.
    pub fn set(&mut self, value: bool) {
        *self.0 = value.into();
    }
    /// Get the boolean value stored in the [`IValue`] from which
    /// this proxy was obtained.
    #[must_use]
    pub fn get(&self) -> bool {
        self.0.is_true()
    }
}

impl<'a> Deref for BoolMut<'a> {
    type Target = bool;
    fn deref(&self) -> &bool {
        if self.get() {
            &true
        } else {
            &false
        }
    }
}

pub(crate) const ALIGNMENT: usize = 8;
pub(crate) const TAG_SIZE_BITS: u32 = ALIGNMENT.trailing_zeros();

#[repr(usize)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum TypeTag {
    F64 = 0,
    StringOrNull = 1,
    ArrayOrFalse = 2,
    ObjectOrTrue = 3,
    I64 = 4,
    U64 = 5,
    InlineInt = 6,
    InlineString = 7,
}

impl From<usize> for TypeTag {
    fn from(other: usize) -> Self {
        // Safety: `% ALIGNMENT` can only return valid variants
        unsafe { mem::transmute(other % ALIGNMENT) }
    }
}

/// Enum which distinguishes the six JSON types.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueType {
    // Stored inline
    /// Null.
    Null,
    /// Boolean.
    Bool,

    // May be stored either inline or behind pointer
    /// Number.
    Number,
    /// String.
    String,

    // Stored behind pointer
    /// Array.
    Array,
    /// Object.
    Object,
}

unsafe impl Send for IValue {}
unsafe impl Sync for IValue {}

impl<A: DefragAllocator> Defrag<A> for IValue {
    fn defrag(self, defrag_allocator: &mut A) -> Self {
        use Destructured::*;
        match self.destructure() {
            Null => IValue::NULL,
            Bool(val) => {
                if val {
                    IValue::TRUE
                } else {
                    IValue::FALSE
                }
            }
            Array(array) => array.defrag(defrag_allocator).0,
            Object(obj) => obj.defrag(defrag_allocator).0,
            String(s) => s.defrag(defrag_allocator).0,
            Number(n) => n.defrag(defrag_allocator).0,
        }
    }
}

impl IValue {
    // Safety: Tag must not be `Number`
    const unsafe fn new_inline(tag: TypeTag) -> Self {
        Self {
            ptr: NonNull::new_unchecked(tag as usize as *mut u8),
        }
    }
    // Safety: Pointer must be non-null and aligned to at least ALIGNMENT
    pub(crate) unsafe fn new_ptr(p: *mut u8, tag: TypeTag) -> Self {
        Self {
            ptr: NonNull::new_unchecked(p.add(tag as usize)),
        }
    }
    // Safety: Reference must be aligned to at least ALIGNMENT
    pub(crate) unsafe fn new_ref<T>(r: &T, tag: TypeTag) -> Self {
        Self::new_ptr(r as *const _ as *mut u8, tag)
    }

    /// JSON `null`.
    pub const NULL: Self = unsafe { Self::new_inline(TypeTag::StringOrNull) };
    /// JSON `false`.
    pub const FALSE: Self = unsafe { Self::new_inline(TypeTag::ArrayOrFalse) };
    /// JSON `true`.
    pub const TRUE: Self = unsafe { Self::new_inline(TypeTag::ObjectOrTrue) };

    pub(crate) fn ptr_usize(&self) -> usize {
        self.ptr.as_ptr() as usize
    }
    // Safety: Must only be called on non-inline types
    pub(crate) unsafe fn ptr(&self) -> *mut u8 {
        self.ptr
            .as_ptr()
            .wrapping_offset(-((self.ptr_usize() % ALIGNMENT) as isize))
    }
    // Safety: Pointer must be non-null and aligned to at least ALIGNMENT
    pub(crate) unsafe fn set_ptr(&mut self, ptr: *mut u8) {
        let tag = self.type_tag();
        self.ptr = NonNull::new_unchecked(ptr.add(tag as usize));
    }
    // Safety: Reference must be aligned to at least ALIGNMENT
    pub(crate) unsafe fn set_ref<T>(&mut self, r: &T) {
        self.set_ptr(r as *const T as *mut u8);
    }
    pub(crate) unsafe fn raw_copy(&self) -> Self {
        Self { ptr: self.ptr }
    }
    pub(crate) fn raw_eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
    fn is_ptr(&self) -> bool {
        self.ptr_usize() >= ALIGNMENT
    }
    fn type_tag(&self) -> TypeTag {
        self.ptr_usize().into()
    }

    /// Returns the type of this value.
    #[must_use]
    pub fn type_(&self) -> ValueType {
        use TypeTag::*;
        match (self.type_tag(), self.is_ptr()) {
            // Numbers
            (InlineInt, _) | (I64, _) | (U64, _) | (F64, _) => ValueType::Number,

            // Pointers
            (StringOrNull, true) => ValueType::String,
            (ArrayOrFalse, true) => ValueType::Array,
            (ObjectOrTrue, true) => ValueType::Object,
            (InlineString, _) => ValueType::String,

            // Non-pointers
            (StringOrNull, false) => ValueType::Null,
            (ArrayOrFalse, false) | (ObjectOrTrue, false) => ValueType::Bool,
        }
    }

    /// Destructures this value into an enum which can be `match`ed on.
    #[must_use]
    pub fn destructure(self) -> Destructured {
        use ValueType::*;
        match self.type_() {
            Null => Destructured::Null,
            Bool => Destructured::Bool(self.is_true()),
            Number => Destructured::Number(INumber(self)),
            String => Destructured::String(IString(self)),
            Array => Destructured::Array(IArray(self)),
            Object => Destructured::Object(IObject(self)),
        }
    }

    /// Destructures a reference to this value into an enum which can be `match`ed on.
    #[must_use]
    pub fn destructure_ref(&self) -> DestructuredRef<'_> {
        use ValueType::*;
        // Safety: we check the type
        unsafe {
            match self.type_() {
                Null => DestructuredRef::Null,
                Bool => DestructuredRef::Bool(self.is_true()),
                Number => DestructuredRef::Number(self.as_number_unchecked()),
                String => DestructuredRef::String(self.as_string_unchecked()),
                Array => DestructuredRef::Array(self.as_array_unchecked()),
                Object => DestructuredRef::Object(self.as_object_unchecked()),
            }
        }
    }

    /// Destructures a mutable reference to this value into an enum which can be `match`ed on.
    pub fn destructure_mut(&mut self) -> DestructuredMut<'_> {
        use ValueType::*;
        // Safety: we check the type
        unsafe {
            match self.type_() {
                Null => DestructuredMut::Null,
                Bool => DestructuredMut::Bool(BoolMut(self)),
                Number => DestructuredMut::Number(self.as_number_unchecked_mut()),
                String => DestructuredMut::String(self.as_string_unchecked_mut()),
                Array => DestructuredMut::Array(self.as_array_unchecked_mut()),
                Object => DestructuredMut::Object(self.as_object_unchecked_mut()),
            }
        }
    }

    /// Reports dynamic memory allocated by this value.
    pub fn mem_allocated(&self) -> usize {
        use ValueType::*;
        match self.type_() {
            // inline types consume no extra memory
            Null | Bool => 0,
            // Safety: We checked the type
            Number => unsafe { self.as_number_unchecked() }.mem_allocated(),
            String => unsafe { self.as_string_unchecked() }.mem_allocated(),
            Array => unsafe { self.as_array_unchecked() }.mem_allocated(),
            Object => unsafe { self.as_object_unchecked() }.mem_allocated(),
        }
    }

    /// Indexes into this value with a number or string.
    /// Panics if the value is not an array or object.
    /// Panics if attempting to index an array with a string.
    /// Panics if attempting to index an object with a number.
    /// Returns `None` if the index type is correct, but there is
    /// no value at this index.
    pub fn get(&self, index: impl ValueIndex) -> Option<&IValue> {
        index.index_into(self)
    }

    /// Mutably indexes into this value with a number or string.
    /// Panics if the value is not an array or object.
    /// Panics if attempting to index an array with a string.
    /// Panics if attempting to index an object with a number.
    /// Returns `None` if the index type is correct, but there is
    /// no value at this index.
    pub fn get_mut(&mut self, index: impl ValueIndex) -> Option<&mut IValue> {
        index.index_into_mut(self)
    }

    /// Removes a value at the specified numberic or string index.
    /// Panics if this is not an array or object.
    /// Panics if attempting to index an array with a string.
    /// Panics if attempting to index an object with a number.
    /// Returns `None` if the index type is correct, but there is
    /// no value at this index.
    pub fn remove(&mut self, index: impl ValueIndex) -> Option<IValue> {
        index.remove(self)
    }

    /// Takes this value, replacing it with [`IValue::NULL`].
    pub fn take(&mut self) -> IValue {
        mem::replace(self, IValue::NULL)
    }

    /// Returns the length of this value if it is an array or object.
    /// Returns `None` for other types.
    #[must_use]
    pub fn len(&self) -> Option<usize> {
        use ValueType::*;
        // Safety: we checked the type
        match self.type_() {
            Array => Some(unsafe { self.as_array_unchecked().len() as usize }),
            Object => Some(unsafe { self.as_object_unchecked().len() as usize }),
            _ => None,
        }
    }

    /// Returns whether this value is empty if it is an array or object.
    /// Returns `None` for other types.
    #[must_use]
    pub fn is_empty(&self) -> Option<bool> {
        use ValueType::*;
        // Safety: we checked the type
        match self.type_() {
            Array => Some(unsafe { self.as_array_unchecked().is_empty() }),
            Object => Some(unsafe { self.as_object_unchecked().is_empty() }),
            _ => None,
        }
    }

    // # Null methods
    /// Returns `true` if this is the `null` value.
    #[must_use]
    pub fn is_null(&self) -> bool {
        self.ptr == Self::NULL.ptr
    }

    // # Bool methods
    /// Returns `true` if this is a boolean.
    #[must_use]
    pub fn is_bool(&self) -> bool {
        self.ptr == Self::TRUE.ptr || self.ptr == Self::FALSE.ptr
    }

    /// Returns `true` if this is the `true` value.
    #[must_use]
    pub fn is_true(&self) -> bool {
        self.ptr == Self::TRUE.ptr
    }

    /// Returns `true` if this is the `false` value.
    #[must_use]
    pub fn is_false(&self) -> bool {
        self.ptr == Self::FALSE.ptr
    }

    /// Converts this value to a `bool`.
    /// Returns `None` if it's not a boolean.
    #[must_use]
    pub fn to_bool(&self) -> Option<bool> {
        if self.is_bool() {
            Some(self.is_true())
        } else {
            None
        }
    }

    // # Number methods
    /// Returns `true` if this is a number.
    #[must_use]
    pub fn is_number(&self) -> bool {
        use TypeTag::*;
        match self.type_tag() {
            InlineInt | I64 | U64 | F64 => true,
            _ => false,
        }
    }

    unsafe fn unchecked_cast_ref<T>(&self) -> &T {
        &*(self as *const Self).cast::<T>()
    }

    unsafe fn unchecked_cast_mut<T>(&mut self) -> &mut T {
        &mut *(self as *mut Self).cast::<T>()
    }

    // Safety: Must be a number
    unsafe fn as_number_unchecked(&self) -> &INumber {
        self.unchecked_cast_ref()
    }

    // Safety: Must be a number
    unsafe fn as_number_unchecked_mut(&mut self) -> &mut INumber {
        self.unchecked_cast_mut()
    }

    /// Gets a reference to this value as an [`INumber`].
    /// Returns `None` if it's not a number.
    #[must_use]
    pub fn as_number(&self) -> Option<&INumber> {
        // Safety: INumber is a `#[repr(transparent)]` wrapper around IValue
        self.is_number()
            .then(|| unsafe { self.as_number_unchecked() })
    }

    /// Gets a mutable reference to this value as an [`INumber`].
    /// Returns `None` if it's not a number.
    pub fn as_number_mut(&mut self) -> Option<&mut INumber> {
        // Safety: INumber is a `#[repr(transparent)]` wrapper around IValue
        self.is_number()
            .then(|| unsafe { self.as_number_unchecked_mut() })
    }

    /// Converts this value to an [`INumber`].
    ///
    /// # Errors
    ///
    /// Returns `Err(self)` if it's not a number.
    pub fn into_number(self) -> Result<INumber, IValue> {
        if self.is_number() {
            Ok(INumber(self))
        } else {
            Err(self)
        }
    }

    /// Converts this value to an i64 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_i64(&self) -> Option<i64> {
        self.as_number()?.to_i64()
    }
    /// Converts this value to a u64 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_u64(&self) -> Option<u64> {
        self.as_number()?.to_u64()
    }
    /// Converts this value to an f64 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_f64(&self) -> Option<f64> {
        self.as_number()?.to_f64()
    }
    /// Converts this value to an f32 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_f32(&self) -> Option<f32> {
        self.as_number()?.to_f32()
    }
    /// Converts this value to an f16 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_f16(&self) -> Option<f16> {
        self.as_number()?.to_f16()
    }
    /// Converts this value to a bf16 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_bf16(&self) -> Option<bf16> {
        self.as_number()?.to_bf16()
    }
    /// Converts this value to an i8 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_i8(&self) -> Option<i8> {
        self.as_number()?.to_i8()
    }
    /// Converts this value to a u8 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_u8(&self) -> Option<u8> {
        self.as_number()?.to_u8()
    }
    /// Converts this value to an i16 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_i16(&self) -> Option<i16> {
        self.as_number()?.to_i16()
    }
    /// Converts this value to a u16 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_u16(&self) -> Option<u16> {
        self.as_number()?.to_u16()
    }
    /// Converts this value to an i32 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_i32(&self) -> Option<i32> {
        self.as_number()?.to_i32()
    }
    /// Converts this value to a u32 if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_u32(&self) -> Option<u32> {
        self.as_number()?.to_u32()
    }
    /// Converts this value to an isize if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_isize(&self) -> Option<isize> {
        self.as_number()?.to_isize()
    }
    /// Converts this value to a usize if it is a number that can be represented exactly.
    #[must_use]
    pub fn to_usize(&self) -> Option<usize> {
        self.as_number()?.to_usize()
    }
    /// Converts this value to an f64 if it is a number, potentially losing precision
    /// in the process.
    #[must_use]
    pub fn to_f64_lossy(&self) -> Option<f64> {
        Some(self.as_number()?.to_f64_lossy())
    }
    /// Converts this value to an f32 if it is a number, potentially losing precision
    /// in the process.
    #[must_use]
    pub fn to_f32_lossy(&self) -> Option<f32> {
        Some(self.as_number()?.to_f32_lossy())
    }
    /// Converts this value to an f16 if it is a number, potentially losing precision
    /// in the process.
    #[must_use]
    pub fn to_f16_lossy(&self) -> Option<f16> {
        Some(self.as_number()?.to_f16_lossy())
    }
    /// Converts this value to an bf16 if it is a number, potentially losing precision
    /// in the process.
    #[must_use]
    pub fn to_bf16_lossy(&self) -> Option<bf16> {
        Some(self.as_number()?.to_bf16_lossy())
    }

    // # String methods
    /// Returns `true` if this is a string.
    #[must_use]
    pub fn is_string(&self) -> bool {
        (self.type_tag() == TypeTag::StringOrNull && self.is_ptr())
            || self.type_tag() == TypeTag::InlineString
    }

    // Safety: Must be a string
    unsafe fn as_string_unchecked(&self) -> &IString {
        self.unchecked_cast_ref()
    }

    // Safety: Must be a string
    unsafe fn as_string_unchecked_mut(&mut self) -> &mut IString {
        self.unchecked_cast_mut()
    }

    /// Gets a reference to this value as an [`IString`].
    /// Returns `None` if it's not a string.
    #[must_use]
    pub fn as_string(&self) -> Option<&IString> {
        if self.is_string() {
            // Safety: IString is a `#[repr(transparent)]` wrapper around IValue
            Some(unsafe { self.as_string_unchecked() })
        } else {
            None
        }
    }

    /// Gets a mutable reference to this value as an [`IString`].
    /// Returns `None` if it's not a string.
    pub fn as_string_mut(&mut self) -> Option<&mut IString> {
        if self.is_string() {
            // Safety: IString is a `#[repr(transparent)]` wrapper around IValue
            Some(unsafe { self.as_string_unchecked_mut() })
        } else {
            None
        }
    }

    /// Converts this value to an [`IString`].
    ///
    /// # Errors
    ///
    /// Returns `Err(self)` if it's not a string.
    pub fn into_string(self) -> Result<IString, IValue> {
        if self.is_string() {
            Ok(IString(self))
        } else {
            Err(self)
        }
    }

    // # Array methods
    /// Returns `true` if this is an array.
    #[must_use]
    pub fn is_array(&self) -> bool {
        self.type_tag() == TypeTag::ArrayOrFalse && self.is_ptr()
    }

    // Safety: Must be an array
    unsafe fn as_array_unchecked(&self) -> &IArray {
        self.unchecked_cast_ref()
    }

    // Safety: Must be an array
    unsafe fn as_array_unchecked_mut(&mut self) -> &mut IArray {
        self.unchecked_cast_mut()
    }

    /// Gets a reference to this value as an [`IArray`].
    /// Returns `None` if it's not an array.
    #[must_use]
    pub fn as_array(&self) -> Option<&IArray> {
        if self.is_array() {
            // Safety: IArray is a `#[repr(transparent)]` wrapper around IValue
            Some(unsafe { self.as_array_unchecked() })
        } else {
            None
        }
    }

    /// Gets a mutable reference to this value as an [`IArray`].
    /// Returns `None` if it's not an array.
    pub fn as_array_mut(&mut self) -> Option<&mut IArray> {
        if self.is_array() {
            // Safety: IArray is a `#[repr(transparent)]` wrapper around IValue
            Some(unsafe { self.as_array_unchecked_mut() })
        } else {
            None
        }
    }

    /// Converts this value to an [`IArray`].
    ///
    /// # Errors
    ///
    /// Returns `Err(self)` if it's not an array.
    pub fn into_array(self) -> Result<IArray, IValue> {
        if self.is_array() {
            Ok(IArray(self))
        } else {
            Err(self)
        }
    }

    // # Object methods
    /// Returns `true` if this is an object.
    #[must_use]
    pub fn is_object(&self) -> bool {
        self.type_tag() == TypeTag::ObjectOrTrue && self.is_ptr()
    }

    // Safety: Must be an array
    unsafe fn as_object_unchecked(&self) -> &IObject {
        self.unchecked_cast_ref()
    }

    // Safety: Must be an array
    unsafe fn as_object_unchecked_mut(&mut self) -> &mut IObject {
        self.unchecked_cast_mut()
    }

    /// Gets a reference to this value as an [`IObject`].
    /// Returns `None` if it's not an object.
    #[must_use]
    pub fn as_object(&self) -> Option<&IObject> {
        if self.is_object() {
            // Safety: IObject is a `#[repr(transparent)]` wrapper around IValue
            Some(unsafe { self.as_object_unchecked() })
        } else {
            None
        }
    }

    /// Gets a mutable reference to this value as an [`IObject`].
    /// Returns `None` if it's not an object.
    pub fn as_object_mut(&mut self) -> Option<&mut IObject> {
        if self.is_object() {
            // Safety: IObject is a `#[repr(transparent)]` wrapper around IValue
            Some(unsafe { self.as_object_unchecked_mut() })
        } else {
            None
        }
    }

    /// Converts this value to an [`IObject`].
    ///
    /// # Errors
    ///
    /// Returns `Err(self)` if it's not an object.
    pub fn into_object(self) -> Result<IObject, IValue> {
        if self.is_object() {
            Ok(IObject(self))
        } else {
            Err(self)
        }
    }
}

impl Clone for IValue {
    fn clone(&self) -> Self {
        use ValueType::*;
        match self.type_() {
            // Inline types can be trivially copied
            Null | Bool => Self { ptr: self.ptr },
            // Safety: We checked the type
            Array => unsafe { self.as_array_unchecked() }.clone_impl(),
            Object => unsafe { self.as_object_unchecked() }.clone_impl(),
            String => unsafe { self.as_string_unchecked() }.clone_impl(),
            Number => unsafe { self.as_number_unchecked() }.clone_impl(),
        }
    }
}

impl Drop for IValue {
    fn drop(&mut self) {
        use ValueType::*;
        match self.type_() {
            // Inline types can be trivially dropped
            Null | Bool => {}
            // Safety: We checked the type
            Array => unsafe { self.as_array_unchecked_mut() }.drop_impl(),
            Object => unsafe { self.as_object_unchecked_mut() }.drop_impl(),
            String => unsafe { self.as_string_unchecked_mut() }.drop_impl(),
            Number => unsafe { self.as_number_unchecked_mut() }.drop_impl(),
        }
    }
}

impl Hash for IValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use ValueType::*;
        match self.type_() {
            // Inline and interned types can be trivially hashed
            Null | Bool | String => self.ptr.hash(state),
            // Safety: We checked the type
            Array => unsafe { self.as_array_unchecked() }.hash(state),
            // Safety: We checked the type
            Object => unsafe { self.as_object_unchecked() }.hash(state),
            // Safety: We checked the type
            Number => unsafe { self.as_number_unchecked() }.hash(state),
        }
    }
}

impl PartialEq for IValue {
    fn eq(&self, other: &Self) -> bool {
        use ValueType::*;
        let (t1, t2) = (self.type_(), other.type_());
        if t1 == t2 {
            // Safety: Only methods for the appropriate type are called
            unsafe {
                match t1 {
                    // Inline and interned types can be trivially compared
                    Null | Bool => self.ptr == other.ptr,
                    String => self.as_string_unchecked() == other.as_string_unchecked(),
                    Number => self.as_number_unchecked() == other.as_number_unchecked(),
                    Array => self.as_array_unchecked() == other.as_array_unchecked(),
                    Object => self.as_object_unchecked() == other.as_object_unchecked(),
                }
            }
        } else {
            false
        }
    }
}

impl Eq for IValue {}
impl PartialOrd for IValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use ValueType::*;
        let (t1, t2) = (self.type_(), other.type_());
        if t1 == t2 {
            // Safety: Only methods for the appropriate type are called
            unsafe {
                match t1 {
                    // Inline and interned types can be trivially compared
                    Null => Some(Ordering::Equal),
                    Bool => self.is_true().partial_cmp(&other.is_true()),
                    String => self
                        .as_string_unchecked()
                        .partial_cmp(other.as_string_unchecked()),
                    Number => self
                        .as_number_unchecked()
                        .partial_cmp(other.as_number_unchecked()),
                    Array => self
                        .as_array_unchecked()
                        .partial_cmp(other.as_array_unchecked()),
                    Object => None,
                }
            }
        } else {
            t1.partial_cmp(&t2)
        }
    }
}

mod private {
    #[doc(hidden)]
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for &str {}
    impl Sealed for &super::IString {}
    impl<T: Sealed> Sealed for &T {}
}

/// Trait which abstracts over the various number and string types
/// which can be used to index into an [`IValue`].
pub trait ValueIndex: private::Sealed + Copy {
    #[doc(hidden)]
    fn index_into(self, v: &IValue) -> Option<&IValue>;

    #[doc(hidden)]
    fn index_into_mut(self, v: &mut IValue) -> Option<&mut IValue>;

    #[doc(hidden)]
    fn index_or_insert(self, v: &mut IValue) -> &mut IValue;

    #[doc(hidden)]
    fn remove(self, v: &mut IValue) -> Option<IValue>;
}

impl ValueIndex for usize {
    fn index_into(self, v: &IValue) -> Option<&IValue> {
        v.as_array().unwrap().get(self)
    }

    fn index_into_mut(self, v: &mut IValue) -> Option<&mut IValue> {
        v.as_array_mut().unwrap().get_mut(self)
    }

    fn index_or_insert(self, v: &mut IValue) -> &mut IValue {
        self.index_into_mut(v).unwrap()
    }

    fn remove(self, v: &mut IValue) -> Option<IValue> {
        v.as_array_mut().unwrap().remove(self)
    }
}

impl ValueIndex for &str {
    fn index_into(self, v: &IValue) -> Option<&IValue> {
        v.as_object().unwrap().get(&IString::intern(self))
    }

    fn index_into_mut(self, v: &mut IValue) -> Option<&mut IValue> {
        v.as_object_mut().unwrap().get_mut(&IString::intern(self))
    }

    fn index_or_insert(self, v: &mut IValue) -> &mut IValue {
        &mut v.as_object_mut().unwrap()[self]
    }

    fn remove(self, v: &mut IValue) -> Option<IValue> {
        v.as_object_mut().unwrap().remove(self)
    }
}

impl ValueIndex for &IString {
    fn index_into(self, v: &IValue) -> Option<&IValue> {
        v.as_object().unwrap().get(self)
    }

    fn index_into_mut(self, v: &mut IValue) -> Option<&mut IValue> {
        v.as_object_mut().unwrap().get_mut(self)
    }

    fn index_or_insert(self, v: &mut IValue) -> &mut IValue {
        &mut v.as_object_mut().unwrap()[self]
    }

    fn remove(self, v: &mut IValue) -> Option<IValue> {
        v.as_object_mut().unwrap().remove(self)
    }
}

impl<T: ValueIndex> ValueIndex for &T {
    fn index_into(self, v: &IValue) -> Option<&IValue> {
        (*self).index_into(v)
    }

    fn index_into_mut(self, v: &mut IValue) -> Option<&mut IValue> {
        (*self).index_into_mut(v)
    }

    fn index_or_insert(self, v: &mut IValue) -> &mut IValue {
        (*self).index_or_insert(v)
    }

    fn remove(self, v: &mut IValue) -> Option<IValue> {
        (*self).remove(v)
    }
}

impl<I: ValueIndex> Index<I> for IValue {
    type Output = IValue;

    #[inline]
    fn index(&self, index: I) -> &IValue {
        index.index_into(self).unwrap()
    }
}

impl<I: ValueIndex> IndexMut<I> for IValue {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut IValue {
        index.index_or_insert(self)
    }
}

impl Debug for IValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use ValueType::*;
        unsafe {
            // Safety: We checked the type
            match self.type_() {
                // Inline and interned types can be trivially hashed
                Null => f.write_str("null"),
                Bool => Debug::fmt(&self.is_true(), f),
                String => Debug::fmt(self.as_string_unchecked(), f),
                Array => Debug::fmt(self.as_array_unchecked(), f),
                Object => Debug::fmt(self.as_object_unchecked(), f),
                Number => Debug::fmt(self.as_number_unchecked(), f),
            }
        }
    }
}

impl<T: Into<IValue>> From<Option<T>> for IValue {
    fn from(other: Option<T>) -> Self {
        if let Some(v) = other {
            v.into()
        } else {
            Self::NULL
        }
    }
}

impl From<bool> for IValue {
    fn from(other: bool) -> Self {
        if other {
            Self::TRUE
        } else {
            Self::FALSE
        }
    }
}

typed_conversions! {
    INumber: i8, u8, i16, u16, i32, u32, i64, u64, isize, usize;
    IString: String, &String, &mut String, &str, &mut str;
}
typed_conversions_fallible! {
    INumber: f16, bf16, f32, f64;
    IObject:
        HashMap<K, V> where (K: Into<IString>, V: Into<IValue>),
        BTreeMap<K, V> where (K: Into<IString>, V: Into<IValue>);
    IArray:
        Vec<T> where (T: Into<IValue> + crate::array::private::Sealed),
        Vec<i8>, Vec<u8>, Vec<i16>, Vec<u16>, Vec<i32>, Vec<u32>,
        Vec<i64>, Vec<u64>, Vec<isize>, Vec<usize>,
        Vec<f16>, Vec<bf16>, Vec<f32>, Vec<f64>,
        &[T] where (T: Into<IValue> + Clone + crate::array::private::Sealed),
        &[i8], &[u8], &[i16], &[u16], &[i32], &[u32],
        &[i64], &[u64], &[isize], &[usize],
        &[f16], &[bf16], &[f32], &[f64];
}

impl Default for IValue {
    fn default() -> Self {
        Self::NULL
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::TryCollect;

    #[mockalloc::test]
    fn can_use_literal() {
        let x: IValue = ijson!({
            "foo": "bar",
            "x": [],
            "y": ["hi", "there", 1, 2, null, false, true, 63.5],
            "z": [false, {
                "a": null
            }, {}]
        });
        let y: IValue = serde_json::from_str(
            r#"{
                "foo": "bar",
                "x": [],
                "y": ["hi", "there", 1, 2, null, false, true, 63.5],
                "z": [false, {
                    "a": null
                }, {}]
            }"#,
        )
        .unwrap();
        assert_eq!(x, y);
    }

    #[test]
    #[allow(clippy::redundant_clone)]
    fn test_null() {
        let x: IValue = IValue::NULL;
        assert!(x.is_null());
        assert_eq!(x.type_(), ValueType::Null);
        assert!(matches!(x.clone().destructure(), Destructured::Null));
        assert!(matches!(x.clone().destructure_ref(), DestructuredRef::Null));
        assert!(matches!(x.clone().destructure_mut(), DestructuredMut::Null));
        assert_eq!(x.mem_allocated(), 0);
    }

    #[test]
    fn test_bool() {
        for v in [true, false].iter().copied() {
            let mut x = IValue::from(v);
            assert!(x.is_bool());
            assert_eq!(x.type_(), ValueType::Bool);
            assert_eq!(x.to_bool(), Some(v));
            assert!(matches!(x.clone().destructure(), Destructured::Bool(u) if u == v));
            assert!(matches!(x.clone().destructure_ref(), DestructuredRef::Bool(u) if u == v));
            assert!(
                matches!(x.clone().destructure_mut(), DestructuredMut::Bool(u) if u.get() == v)
            );

            if let DestructuredMut::Bool(mut b) = x.destructure_mut() {
                b.set(!v);
            }

            assert_eq!(x.to_bool(), Some(!v));
            assert_eq!(x.mem_allocated(), 0);
        }
    }

    #[mockalloc::test]
    fn test_number() {
        const INLINE_LOWER: i64 = i64::MIN >> TAG_SIZE_BITS;
        const INLINE_UPPER: i64 = i64::MAX >> TAG_SIZE_BITS;
        for range in [-1 << 61, 0i64, 1 << 61].map(|b| b - 100..b + 100) {
            for v in range {
                let mut x = IValue::from(v);
                assert!(x.is_number());
                assert_eq!(x.type_(), ValueType::Number);
                assert_eq!(x.to_i32(), i32::try_from(v).ok());
                assert_eq!(x.to_u32(), u32::try_from(v).ok());
                assert_eq!(x.to_i64(), i64::try_from(v).ok());
                assert_eq!(x.to_u64(), u64::try_from(v).ok());
                assert_eq!(x.to_isize(), isize::try_from(v).ok());
                assert_eq!(x.to_usize(), usize::try_from(v).ok());
                assert_eq!(x.to_f64_lossy(), Some(v as f64));
                assert_eq!(x.to_f32_lossy(), Some(v as f32));
                assert_eq!(x.as_number(), Some(&v.into()));
                assert_eq!(x.as_number_mut(), Some(&mut v.into()));
                assert!(
                    matches!(x.clone().destructure(), Destructured::Number(u) if u == v.into())
                );
                assert!(
                    matches!(x.clone().destructure_ref(), DestructuredRef::Number(u) if *u == v.into())
                );
                assert!(
                    matches!(x.clone().destructure_mut(), DestructuredMut::Number(u) if *u == v.into())
                );
                assert_eq!(
                    x.mem_allocated(),
                    if (INLINE_LOWER..=INLINE_UPPER).contains(&v) {
                        0
                    } else {
                        mem::size_of::<i64>()
                    }
                );
            }
        }
        for range in [0u64, 1 << 61, 1 << 63].map(|b| b..b + 100) {
            for v in range {
                let mut x = IValue::from(v);
                assert!(x.is_number());
                assert_eq!(x.type_(), ValueType::Number);
                assert_eq!(x.to_i32(), i32::try_from(v).ok());
                assert_eq!(x.to_u32(), u32::try_from(v).ok());
                assert_eq!(x.to_i64(), i64::try_from(v).ok());
                assert_eq!(x.to_u64(), u64::try_from(v).ok());
                assert_eq!(x.to_isize(), isize::try_from(v).ok());
                assert_eq!(x.to_usize(), usize::try_from(v).ok());
                assert_eq!(x.to_f64_lossy(), Some(v as f64));
                assert_eq!(x.to_f32_lossy(), Some(v as f32));
                assert_eq!(x.as_number(), Some(&v.into()));
                assert_eq!(x.as_number_mut(), Some(&mut v.into()));
                assert!(
                    matches!(x.clone().destructure(), Destructured::Number(u) if u == v.into())
                );
                assert!(
                    matches!(x.clone().destructure_ref(), DestructuredRef::Number(u) if *u == v.into())
                );
                assert!(
                    matches!(x.clone().destructure_mut(), DestructuredMut::Number(u) if *u == v.into())
                );
                assert_eq!(
                    x.mem_allocated(),
                    if v <= i64::MAX as u64 && (INLINE_LOWER..=INLINE_UPPER).contains(&(v as i64)) {
                        0
                    } else {
                        mem::size_of::<u64>()
                    }
                );
            }
        }
        for range in [-1i64 << 62, 1 << 62].map(|v| v - 100..v + 100) {
            for v in range.map(|v| v as f64 * 1e-30) {
                let mut x = IValue::from(v);
                assert!(x.is_number());
                assert_eq!(x.type_(), ValueType::Number);
                assert_eq!(x.to_f64(), Some(v));
                assert_eq!(x.to_f64_lossy(), Some(v));
                assert_eq!(x.to_f32_lossy(), Some(v as f32));
                assert_eq!(x.as_number(), Some(&INumber::try_from(v).unwrap()));
                assert_eq!(x.as_number_mut(), Some(&mut INumber::try_from(v).unwrap()));
                assert!(
                    matches!(x.clone().destructure(), Destructured::Number(u) if u == INumber::try_from(v).unwrap())
                );
                assert!(
                    matches!(x.clone().destructure_ref(), DestructuredRef::Number(u) if *u == INumber::try_from(v).unwrap())
                );
                assert!(
                    matches!(x.clone().destructure_mut(), DestructuredMut::Number(u) if *u == INumber::try_from(v).unwrap())
                );
                assert_eq!(x.mem_allocated(), mem::size_of::<f64>());
            }
        }
    }

    #[mockalloc::test]
    fn test_string() {
        for v in 0..10 {
            let mut s = IString::intern(&v.to_string());
            let mut x = IValue::from(s.clone());
            assert!(x.is_string());
            assert_eq!(x.type_(), ValueType::String);
            assert_eq!(x.as_string(), Some(&s));
            assert_eq!(x.as_string_mut(), Some(&mut s));
            assert!(matches!(x.clone().destructure(), Destructured::String(u) if u == s));
            assert!(matches!(x.clone().destructure_ref(), DestructuredRef::String(u) if *u == s));
            assert!(matches!(x.clone().destructure_mut(), DestructuredMut::String(u) if *u == s));
            assert_eq!(x.mem_allocated(), 0);
        }

        let s = String::from("foofoofoo");
        let mut x = IValue::from(&s);
        assert!(x.is_string());
        assert_eq!(x.type_(), ValueType::String);
        assert_eq!(x.as_string(), Some(&IString::intern(&s)));
        assert_eq!(x.as_string_mut(), Some(&mut IString::intern(&s)));
        assert!(matches!(x.clone().destructure(), Destructured::String(u) if u == s));
        assert!(matches!(x.clone().destructure_ref(), DestructuredRef::String(u) if *u == s));
        assert!(matches!(x.clone().destructure_mut(), DestructuredMut::String(u) if *u == s));
        assert_eq!(x.mem_allocated(), 24);
    }

    #[mockalloc::test]
    fn test_array() {
        for v in 4..20 {
            let mut a: IArray = (0..v).try_collect().unwrap();
            let mut x = IValue::from(a.clone());
            assert!(x.is_array());
            assert_eq!(x.type_(), ValueType::Array);
            assert_eq!(x.as_array(), Some(&a));
            assert_eq!(x.as_array_mut(), Some(&mut a));
            assert!(matches!(x.clone().destructure(), Destructured::Array(u) if u == a));
            assert!(matches!(x.clone().destructure_ref(), DestructuredRef::Array(u) if *u == a));
            assert!(matches!(x.clone().destructure_mut(), DestructuredMut::Array(u) if *u == a));
            assert_eq!(
                x.mem_allocated(),
                mem::size_of::<usize>()
                    + ((a.capacity() as usize * mem::size_of::<i32>() + 7) & !7)
            );
        }
    }

    #[mockalloc::test]
    fn test_object() {
        for v in 4..20 {
            let mut o: IObject = (0..v).map(|i| (i.to_string(), i)).try_collect().unwrap();
            let mut x = IValue::from(o.clone());
            assert!(x.is_object());
            assert_eq!(x.type_(), ValueType::Object);
            assert_eq!(x.as_object(), Some(&o));
            assert_eq!(x.as_object_mut(), Some(&mut o));
            assert!(matches!(x.clone().destructure(), Destructured::Object(u) if u == o));
            assert!(matches!(x.clone().destructure_ref(), DestructuredRef::Object(u) if *u == o));
            assert!(matches!(x.clone().destructure_mut(), DestructuredMut::Object(u) if *u == o));
            // Layout: packed 8-byte header + KeyValuePair array + (only when cap > 8)
            // a u32 hash table, padded to 8-byte alignment. Small objects carry no table.
            let cap = o.capacity() as usize;
            let table_bytes = if cap > 8 {
                5 * cap / 4 * mem::size_of::<u32>()
            } else {
                0
            };
            let raw = mem::size_of::<u64>()
                + cap * (mem::size_of::<IString>() + mem::size_of::<IValue>())
                + table_bytes;
            assert_eq!(
                x.mem_allocated(),
                o.iter()
                    .map(|(k, v)| k.mem_allocated() + v.mem_allocated())
                    .sum::<usize>()
                    + ((raw + 7) & !7)
            );
        }
    }

    #[mockalloc::test]
    fn test_into_object_for_object() {
        let o: IObject = (0..10).map(|i| (i.to_string(), i)).try_collect().unwrap();
        let x = IValue::from(o.clone());

        assert_eq!(x.into_object(), Ok(o));
    }
}

#[cfg(test)]
mod tests_defrag {
    use std::alloc::{alloc, dealloc, Layout};

    use super::*;

    struct DummyDefragAllocator;

    impl DefragAllocator for DummyDefragAllocator {
        unsafe fn realloc_ptr<T>(&mut self, ptr: *mut T, layout: Layout) -> *mut T {
            let new_ptr = self.alloc(layout).cast::<T>();
            std::ptr::copy_nonoverlapping(ptr.cast::<u8>(), new_ptr.cast::<u8>(), layout.size());
            self.free(ptr, layout);
            new_ptr
        }

        /// Allocate memory for defrag
        unsafe fn alloc(&mut self, layout: Layout) -> *mut u8 {
            alloc(layout)
        }

        /// Free memory for defrag
        unsafe fn free<T>(&mut self, ptr: *mut T, layout: Layout) {
            dealloc(ptr as *mut u8, layout);
        }
    }

    fn test_defrag_generic(val: IValue) {
        let defrag_val = val.clone();
        crate::reinit_shared_string_cache();
        let defrag_val = defrag_val.defrag(&mut DummyDefragAllocator);
        assert_eq!(val, defrag_val);
    }

    #[test]
    fn test_defrag_null() {
        test_defrag_generic(ijson!(null));
    }

    #[test]
    fn test_defrag_bool() {
        test_defrag_generic(ijson!(true));
        test_defrag_generic(ijson!(false));
    }

    #[test]
    fn test_defrag_number() {
        test_defrag_generic(ijson!(1));
        test_defrag_generic(ijson!(1000000000));
        test_defrag_generic(ijson!(-1000000000));
        test_defrag_generic(ijson!(1.11111111));
        test_defrag_generic(ijson!(-1.11111111));
    }

    #[test]
    fn test_defrag_string() {
        test_defrag_generic(ijson!("test"));
        test_defrag_generic(ijson!(""));
    }

    #[test]
    fn test_defrag_array() {
        test_defrag_generic(ijson!([1, 2, "bar"]));
    }

    #[test]
    fn test_defrag_array_of_numbers() {
        test_defrag_generic(ijson!([1, 2, 3]));
    }

    #[test]
    fn test_defrag_empty_array_of_numbers() {
        test_defrag_generic(ijson!([]));
    }

    #[test]
    fn test_defrag_object() {
        test_defrag_generic(ijson!({"foo": "bar"}));
    }

    #[test]
    fn test_defrag_empty_object() {
        test_defrag_generic(ijson!({}));
    }

    #[test]
    fn test_defrag_complex() {
        test_defrag_generic(ijson!([
            {"foo": "bar"}, 1, 2, 3, null, true, false, {"test":[1,2,null, true, false, 3, {"foo":[1, "bar"]}]}
        ]));
    }
}
