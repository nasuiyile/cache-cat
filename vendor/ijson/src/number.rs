//! Functionality relating to the JSON number type
#![allow(clippy::float_cmp)]

use half::{bf16, f16};
use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Debug, Formatter};
use std::hash::Hash;

use crate::{
    value::{TypeTag, ALIGNMENT, TAG_SIZE_BITS},
    Defrag, DefragAllocator, IValue,
};

#[repr(usize)]
#[derive(Copy, Clone, PartialEq, Eq)]
enum NumberType {
    F64 = TypeTag::F64 as usize,
    I64 = TypeTag::I64 as usize,
    U64 = TypeTag::U64 as usize,
    Inline = TypeTag::InlineInt as usize,
}

impl From<usize> for NumberType {
    fn from(other: usize) -> Self {
        // Safety: `% ALIGNMENT` can only return valid variants
        unsafe { std::mem::transmute(other % ALIGNMENT) }
    }
}
impl From<NumberType> for TypeTag {
    fn from(tag: NumberType) -> Self {
        // Safety: all NumberType variants are equal to their corresponding TypeTags
        unsafe { std::mem::transmute(tag as usize) }
    }
}

fn can_represent_as_f64(x: u64) -> bool {
    x.leading_zeros() + x.trailing_zeros() >= 11
}
fn can_represent_as_f32(x: u64) -> bool {
    x.leading_zeros() + x.trailing_zeros() >= 40
}

fn cmp_i64_to_f64(a: i64, b: f64) -> Ordering {
    if a < 0 {
        cmp_u64_to_f64(a.wrapping_neg() as u64, -b).reverse()
    } else {
        cmp_u64_to_f64(a as u64, b)
    }
}
fn cmp_u64_to_f64(a: u64, b: f64) -> Ordering {
    if can_represent_as_f64(a) {
        // If we can represent as an f64, we can just cast and compare
        (a as f64).partial_cmp(&b).unwrap()
    } else if b <= (0x0020_0000_0000_0000_u64 as f64) {
        // If the floating point number is less than all non-representable
        // integers, and our integer is non-representable, then we know
        // the integer is greater.
        Ordering::Greater
    } else if b >= u64::MAX as f64 {
        // If the floating point number is larger than the largest u64, then
        // the integer is smaller.
        Ordering::Less
    } else {
        // The remaining floating point values can be losslessly converted to u64.
        a.cmp(&(b as u64))
    }
}

// Range of a 61-bit signed integer.
const INLINE_LOWER: i64 = i64::MIN >> TAG_SIZE_BITS;
const INLINE_UPPER: i64 = i64::MAX >> TAG_SIZE_BITS;

/// The `INumber` type represents a JSON number. It is decoupled from any specific
/// representation, and internally uses several. There is no way to determine the
/// internal representation: instead the caller is expected to convert the number
/// using one of the fallible `to_xxx` functions and handle the cases where the
/// number does not convert to the desired type.
///
/// Special floating point values (eg. NaN, Infinity, etc.) cannot be stored within
/// an `INumber`.
///
/// Whilst `INumber` does not consider `2.0` and `2` to be different numbers (ie.
/// they will compare equal) it does allow you to distinguish them using the
/// method `INumber::has_decimal_point()`. That said, calling `to_i32` on
/// `2.0` will succeed with the value `2`.
///
/// Currently `INumber` can store any number representable with an `f64`, `i64` or
/// `u64`. It is expected that in the future it will be further expanded to store
/// integers and possibly decimals to arbitrary precision, but that is not currently
/// the case.
///
/// Any number representable with an `i8` or a `u8` can be stored in an `INumber`
/// without a heap allocation (so JSON byte arrays are relatively efficient).
/// Integers up to 24 bits can be stored with a 4-byte heap allocation.
#[repr(transparent)]
#[derive(Clone)]
pub struct INumber(pub(crate) IValue);

value_subtype_impls!(INumber, into_number, as_number, as_number_mut);

impl INumber {
    fn layout(tag: NumberType) -> Layout {
        match tag {
            NumberType::I64 => Layout::new::<i64>(),
            NumberType::U64 => Layout::new::<u64>(),
            NumberType::F64 => Layout::new::<f64>(),
            // Inline numbers are stored in the pointer itself, so no layout is needed
            NumberType::Inline => unreachable!(),
        }
    }

    /// Returns the number zero (without a decimal point). Does not allocate.
    #[must_use]
    pub fn zero() -> Self {
        // Safety: 0 is in the inline range
        unsafe { Self::new_inline(0) }
    }
    /// Returns the number one (without a decimal point). Does not allocate.
    #[must_use]
    pub fn one() -> Self {
        // Safety: 1 is in the inline range
        unsafe { Self::new_inline(1) }
    }
    // Safety: Value must be in the range INLINE_LOWER..=INLINE_UPPER
    unsafe fn new_inline(value: i64) -> Self {
        INumber(IValue::new_ptr(
            (value << TAG_SIZE_BITS) as *mut u8,
            TypeTag::InlineInt,
        ))
    }
    fn new_ptr(tag: NumberType) -> Self {
        unsafe {
            let layout = Self::layout(tag);
            let ptr = alloc(layout);
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            INumber(IValue::new_ptr(ptr, tag.into()))
        }
    }

    fn type_tag(&self) -> NumberType {
        self.0.ptr_usize().into()
    }
    fn is_inline(&self) -> bool {
        self.type_tag() == NumberType::Inline
    }

    fn new_i64(value: i64) -> Self {
        if (INLINE_LOWER..=INLINE_UPPER).contains(&value) {
            // Safety: We know this is in the inline range
            unsafe { Self::new_inline(value) }
        } else {
            let mut res = Self::new_ptr(NumberType::I64);
            // Safety: We know this is an i64 because we just created it
            unsafe {
                res.ptr_mut().cast::<i64>().write(value);
            }
            res
        }
    }
    fn new_u64(value: u64) -> Self {
        if let Ok(val) = i64::try_from(value) {
            Self::new_i64(val)
        } else {
            let mut res = Self::new_ptr(NumberType::U64);
            // Safety: We know this is a u64 because we just created it
            unsafe {
                res.ptr_mut().write(value);
            }
            res
        }
    }
    fn new_f64(value: f64) -> Self {
        let mut res = Self::new_ptr(NumberType::F64);
        // Safety: We know this is an f64 because we just created it
        unsafe {
            res.ptr_mut().cast::<f64>().write(value);
        }
        res
    }

    pub(crate) fn clone_impl(&self) -> IValue {
        // Safety: We only call methods appropriate for the matched type
        unsafe {
            match self.type_tag() {
                NumberType::Inline => self.0.raw_copy(),
                NumberType::I64 => Self::new_i64(*self.i64_unchecked()).0,
                NumberType::U64 => Self::new_u64(*self.u64_unchecked()).0,
                NumberType::F64 => Self::new_f64(*self.f64_unchecked()).0,
            }
        }
    }
    pub(crate) fn drop_impl(&mut self) {
        if !self.is_inline() {
            unsafe {
                dealloc(self.0.ptr(), Self::layout(self.type_tag()));
            }
        }
    }

    unsafe fn ptr_mut(&mut self) -> *mut u64 {
        self.0.ptr().cast()
    }
    unsafe fn ptr(&self) -> *const u64 {
        self.0.ptr().cast()
    }
    unsafe fn inline_int_unchecked(&self) -> i64 {
        self.0.ptr_usize() as i64 >> TAG_SIZE_BITS
    }
    unsafe fn i64_unchecked(&self) -> &i64 {
        &*self.ptr().cast()
    }
    unsafe fn u64_unchecked(&self) -> &u64 {
        &*self.ptr()
    }
    unsafe fn f64_unchecked(&self) -> &f64 {
        &*self.ptr().cast()
    }
    // Currently unused, but may be useful in the future
    // unsafe fn i64_unchecked_mut(&mut self) -> &mut i64 {
    //     &mut *self.ptr_mut().cast()
    // }
    // unsafe fn u64_unchecked_mut(&mut self) -> &mut u64 {
    //     &mut *self.ptr_mut()
    // }
    // unsafe fn f64_unchecked_mut(&mut self) -> &mut f64 {
    //     &mut *self.ptr_mut().cast()
    // }

    /// Converts this number to an i64 if it can be represented exactly.
    #[must_use]
    pub fn to_i64(&self) -> Option<i64> {
        // Safety: We only call methods appropriate for the type
        unsafe {
            match self.type_tag() {
                NumberType::Inline => Some(self.inline_int_unchecked()),
                NumberType::I64 => Some(*self.i64_unchecked()),
                // all u64 values are in the range [i64::MAX, u64::MAX)
                NumberType::U64 => None,
                NumberType::F64 => {
                    let v = *self.f64_unchecked();
                    if v.fract() == 0.0 && i64::MIN as f64 <= v && v < i64::MAX as f64 {
                        Some(v as i64)
                    } else {
                        None
                    }
                }
            }
        }
    }
    /// Converts this number to an f64 if it can be represented exactly.
    #[must_use]
    pub fn to_u64(&self) -> Option<u64> {
        // Safety: We only call methods appropriate for the type
        unsafe {
            match self.type_tag() {
                NumberType::Inline => u64::try_from(self.inline_int_unchecked()).ok(),
                NumberType::I64 => u64::try_from(*self.i64_unchecked()).ok(),
                NumberType::U64 => Some(*self.u64_unchecked()),
                NumberType::F64 => {
                    let v = *self.f64_unchecked();
                    if v.fract() == 0.0 && 0.0 <= v && v < u64::MAX as f64 {
                        Some(v as u64)
                    } else {
                        None
                    }
                }
            }
        }
    }
    /// Converts this number to an isize if it can be represented exactly.
    #[must_use]
    pub fn to_isize(&self) -> Option<isize> {
        self.to_i64().map(|v| v as _)
    }
    /// Converts this number to a usize if it can be represented exactly.
    #[must_use]
    pub fn to_usize(&self) -> Option<usize> {
        self.to_u64().map(|v| v as _)
    }
    /// Converts this number to an i8 if it can be represented exactly.
    #[must_use]
    pub fn to_i8(&self) -> Option<i8> {
        self.to_i64().and_then(|x| x.try_into().ok())
    }
    /// Converts this number to a u8 if it can be represented exactly.
    #[must_use]
    pub fn to_u8(&self) -> Option<u8> {
        self.to_u64().and_then(|x| x.try_into().ok())
    }
    /// Converts this number to an i16 if it can be represented exactly.
    #[must_use]
    pub fn to_i16(&self) -> Option<i16> {
        self.to_i64().and_then(|x| x.try_into().ok())
    }
    /// Converts this number to a u16 if it can be represented exactly.
    #[must_use]
    pub fn to_u16(&self) -> Option<u16> {
        self.to_u64().and_then(|x| x.try_into().ok())
    }
    /// Converts this number to an i32 if it can be represented exactly.
    #[must_use]
    pub fn to_i32(&self) -> Option<i32> {
        self.to_i64().and_then(|x| x.try_into().ok())
    }
    /// Converts this number to a u32 if it can be represented exactly.
    #[must_use]
    pub fn to_u32(&self) -> Option<u32> {
        self.to_u64().and_then(|x| x.try_into().ok())
    }
    /// This allows distinguishing between `1.0` and `1` in the original JSON.
    /// Numeric operations will otherwise treat these two values as equivalent.
    #[must_use]
    pub fn has_decimal_point(&self) -> bool {
        self.type_tag() == NumberType::F64
    }
    /// Converts this number to an f64, potentially losing precision in the process.
    #[must_use]
    pub fn to_f64_lossy(&self) -> f64 {
        unsafe {
            match self.type_tag() {
                NumberType::Inline => self.inline_int_unchecked() as f64,
                NumberType::I64 => *self.i64_unchecked() as f64,
                NumberType::U64 => *self.u64_unchecked() as f64,
                NumberType::F64 => *self.f64_unchecked(),
            }
        }
    }
    /// Converts this number to an f64 if it can be represented exactly.
    #[must_use]
    pub fn to_f64(&self) -> Option<f64> {
        // Safety: We only call methods appropriate for the type
        unsafe {
            match self.type_tag() {
                NumberType::Inline => {
                    let v = self.inline_int_unchecked();
                    let can_represent = if v < 0 {
                        can_represent_as_f64(v.wrapping_neg() as u64)
                    } else {
                        can_represent_as_f64(v as u64)
                    };
                    if can_represent {
                        Some(v as f64)
                    } else {
                        None
                    }
                }
                NumberType::I64 => {
                    let v = *self.i64_unchecked();
                    let can_represent = if v < 0 {
                        can_represent_as_f64(v.wrapping_neg() as u64)
                    } else {
                        can_represent_as_f64(v as u64)
                    };
                    if can_represent {
                        Some(v as f64)
                    } else {
                        None
                    }
                }
                NumberType::U64 => {
                    let v = *self.u64_unchecked();
                    if can_represent_as_f64(v) {
                        Some(v as f64)
                    } else {
                        None
                    }
                }
                NumberType::F64 => Some(*self.f64_unchecked()),
            }
        }
    }
    /// Converts this number to an f32, potentially losing precision in the process.
    #[must_use]
    pub fn to_f32_lossy(&self) -> f32 {
        self.to_f64_lossy() as f32
    }
    /// Converts this number to an f32 if it can be represented exactly.
    #[must_use]
    pub fn to_f32(&self) -> Option<f32> {
        // Safety: We only call methods appropriate for the type
        unsafe {
            match self.type_tag() {
                NumberType::Inline => {
                    let v = self.inline_int_unchecked();
                    let can_represent = if v < 0 {
                        can_represent_as_f32(v.wrapping_neg() as u64)
                    } else {
                        can_represent_as_f32(v as u64)
                    };
                    if can_represent {
                        Some(v as f32)
                    } else {
                        None
                    }
                }
                NumberType::I64 => {
                    let v = *self.i64_unchecked();
                    let can_represent = if v < 0 {
                        can_represent_as_f32(v.wrapping_neg() as u64)
                    } else {
                        can_represent_as_f32(v as u64)
                    };
                    if can_represent {
                        Some(v as f32)
                    } else {
                        None
                    }
                }
                NumberType::U64 => {
                    let v = *self.u64_unchecked();
                    if can_represent_as_f32(v) {
                        Some(v as f32)
                    } else {
                        None
                    }
                }
                NumberType::F64 => {
                    let v = *self.f64_unchecked();
                    let u = v as f32;
                    if v == f64::from(u) {
                        Some(u)
                    } else {
                        None
                    }
                }
            }
        }
    }
    /// Converts this number to an f16, potentially losing precision in the process.
    #[must_use]
    pub fn to_f16_lossy(&self) -> f16 {
        f16::from_f32(self.to_f32_lossy())
    }
    /// Converts this number to an f16 if it can be represented exactly.
    #[must_use]
    pub fn to_f16(&self) -> Option<f16> {
        self.to_f32().and_then(|v| {
            let u = f16::from_f32(v);
            if v == f32::from(u) {
                Some(u)
            } else {
                None
            }
        })
    }
    /// Converts this number to a bf16, potentially losing precision in the process.
    #[must_use]
    pub fn to_bf16_lossy(&self) -> bf16 {
        bf16::from_f32(self.to_f32_lossy())
    }
    /// Converts this number to a bf16 if it can be represented exactly.
    #[must_use]
    pub fn to_bf16(&self) -> Option<bf16> {
        self.to_f32().and_then(|v| {
            let u = bf16::from_f32(v);
            if v == f32::from(u) {
                Some(u)
            } else {
                None
            }
        })
    }

    fn cmp_impl(&self, other: &Self) -> Ordering {
        if self.type_tag() == other.type_tag() {
            // Safety: we know type tags are the same
            unsafe { self.cmp_homogenous_tags(other) }
        } else {
            // Safety: we know type tags are different
            unsafe { self.cmp_heterogenous_tags(other) }
        }
    }
    // Safety: type tags must be the same
    unsafe fn cmp_homogenous_tags(&self, other: &Self) -> Ordering {
        use NumberType::*;
        // Safety: We only call methods appropriate for the matched type
        match self.type_tag() {
            Inline => self
                .inline_int_unchecked()
                .cmp(&other.inline_int_unchecked()),
            I64 => self.i64_unchecked().cmp(other.i64_unchecked()),
            U64 => self.u64_unchecked().cmp(other.u64_unchecked()),
            F64 => self
                .f64_unchecked()
                .partial_cmp(other.f64_unchecked())
                .unwrap(),
        }
    }
    // Safety: type tags must be different
    unsafe fn cmp_heterogenous_tags(&self, other: &Self) -> Ordering {
        use NumberType::*;
        // Safety: We only call methods appropriate for the matched type
        match (self.type_tag(), other.type_tag()) {
            (Inline, I64) => self.inline_int_unchecked().cmp(&*other.i64_unchecked()),
            // all inline values are in the range [-2^61, 2^61)
            (Inline, U64) => Ordering::Less,
            (Inline, F64) => cmp_i64_to_f64(self.inline_int_unchecked(), *other.f64_unchecked()),

            // all u64 values are in the range [i64::MAX, u64::MAX)
            (I64, U64) => Ordering::Less,
            (I64, F64) => cmp_i64_to_f64(*self.i64_unchecked(), *other.f64_unchecked()),

            (U64, F64) => cmp_u64_to_f64(*self.u64_unchecked(), *other.f64_unchecked()),

            // Non-number types do not exist for INumbers. All cases covered here are inverse of the above.
            _ => other.cmp_heterogenous_tags(&self).reverse(),
        }
    }

    pub(crate) fn mem_allocated(&self) -> usize {
        if self.is_inline() {
            0
        } else {
            Self::layout(self.type_tag()).size()
        }
    }
}

impl Hash for INumber {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Some(v) = self.to_i64() {
            v.hash(state);
        } else if let Some(v) = self.to_u64() {
            v.hash(state);
        } else if let Some(v) = self.to_f64() {
            let bits = if v == 0.0 {
                0 // this accounts for +0.0 and -0.0
            } else {
                v.to_bits()
            };
            bits.hash(state);
        }
    }
}

impl From<u64> for INumber {
    fn from(v: u64) -> Self {
        Self::new_u64(v)
    }
}
impl From<u32> for INumber {
    fn from(v: u32) -> Self {
        // Safety: All u32s are in the inline range
        unsafe { Self::new_inline(v as i64) }
    }
}
impl From<u16> for INumber {
    fn from(v: u16) -> Self {
        // Safety: All u16s are in the inline range
        unsafe { Self::new_inline(v as i64) }
    }
}
impl From<u8> for INumber {
    fn from(v: u8) -> Self {
        // Safety: All u8s are in the inline range
        unsafe { Self::new_inline(v as i64) }
    }
}
impl From<usize> for INumber {
    fn from(v: usize) -> Self {
        Self::new_u64(v as u64)
    }
}
impl From<i64> for INumber {
    fn from(v: i64) -> Self {
        Self::new_i64(v)
    }
}
impl From<i32> for INumber {
    fn from(v: i32) -> Self {
        // Safety: All i32s are in the inline range
        unsafe { Self::new_inline(v as i64) }
    }
}
impl From<i16> for INumber {
    fn from(v: i16) -> Self {
        // Safety: All i16s are in the inline range
        unsafe { Self::new_inline(v as i64) }
    }
}
impl From<i8> for INumber {
    fn from(v: i8) -> Self {
        // Safety: All i8s are in the inline range
        unsafe { Self::new_inline(v as i64) }
    }
}
impl From<isize> for INumber {
    fn from(v: isize) -> Self {
        Self::new_i64(v as i64)
    }
}
impl TryFrom<f64> for INumber {
    type Error = ();
    fn try_from(v: f64) -> Result<Self, ()> {
        if v.is_finite() {
            Ok(Self::new_f64(v))
        } else {
            Err(())
        }
    }
}
impl TryFrom<f32> for INumber {
    type Error = ();
    fn try_from(v: f32) -> Result<Self, ()> {
        if v.is_finite() {
            Ok(Self::new_f64(v as f64))
        } else {
            Err(())
        }
    }
}
impl TryFrom<f16> for INumber {
    type Error = ();
    fn try_from(v: f16) -> Result<Self, ()> {
        if v.is_finite() {
            Ok(Self::new_f64(f64::from(v)))
        } else {
            Err(())
        }
    }
}
impl TryFrom<bf16> for INumber {
    type Error = ();
    fn try_from(v: bf16) -> Result<Self, ()> {
        if v.is_finite() {
            Ok(Self::new_f64(f64::from(v)))
        } else {
            Err(())
        }
    }
}

impl PartialEq for INumber {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for INumber {}
impl Ord for INumber {
    fn cmp(&self, other: &Self) -> Ordering {
        // Fast path, pointers are equal
        if self.0.raw_eq(&other.0) {
            Ordering::Equal
        } else {
            self.cmp_impl(other)
        }
    }
}
impl PartialOrd for INumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Debug for INumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(v) = self.to_i64() {
            Debug::fmt(&v, f)
        } else if let Some(v) = self.to_u64() {
            Debug::fmt(&v, f)
        } else if let Some(v) = self.to_f64() {
            Debug::fmt(&v, f)
        } else {
            unreachable!()
        }
    }
}

impl Default for INumber {
    fn default() -> Self {
        Self::zero()
    }
}

impl<A: DefragAllocator> Defrag<A> for INumber {
    fn defrag(mut self, defrag_allocator: &mut A) -> Self {
        match self.type_tag() {
            NumberType::Inline => self,
            t => {
                let layout = Self::layout(t);
                unsafe {
                    let ptr = self.ptr_mut();
                    self.0
                        .set_ptr(defrag_allocator.realloc_ptr(ptr.cast(), layout));
                }
                self
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mockalloc::test]
    fn can_create() {
        let x = INumber::zero();
        let y: INumber = (0.0).try_into().unwrap();

        assert_eq!(x, y);
        assert!(!x.has_decimal_point());
        assert!(y.has_decimal_point());
        assert_eq!(x.to_i32(), Some(0));
        assert_eq!(y.to_i32(), Some(0));
        assert!(INumber::try_from(f32::INFINITY).is_err());
        assert!(INumber::try_from(f64::INFINITY).is_err());
        assert!(INumber::try_from(f32::NEG_INFINITY).is_err());
        assert!(INumber::try_from(f64::NEG_INFINITY).is_err());
        assert!(INumber::try_from(f32::NAN).is_err());
        assert!(INumber::try_from(f64::NAN).is_err());
    }

    #[mockalloc::test]
    fn can_store_various_numbers() {
        let x: INumber = 256.into();
        assert_eq!(x.to_i64(), Some(256));
        assert_eq!(x.to_u64(), Some(256));
        assert_eq!(x.to_f64(), Some(256.0));

        let x: INumber = 0x1000000.into();
        assert_eq!(x.to_i64(), Some(0x1000000));
        assert_eq!(x.to_u64(), Some(0x1000000));
        assert_eq!(x.to_f64(), Some(16_777_216.0));

        let x: INumber = i64::MIN.into();
        assert_eq!(x.to_i64(), Some(i64::MIN));
        assert_eq!(x.to_u64(), None);
        assert_eq!(x.to_f64(), Some(-9_223_372_036_854_775_808.0));

        let x: INumber = (i64::MIN as f64).try_into().unwrap();
        assert_eq!(x.to_i64(), Some(i64::MIN));
        assert_eq!(x.to_u64(), None);
        assert_eq!(x.to_f64(), Some(-9_223_372_036_854_775_808.0));

        let x: INumber = i64::MAX.into();
        assert_eq!(x.to_i64(), Some(i64::MAX));
        assert_eq!(x.to_u64(), Some(i64::MAX as u64));
        assert_eq!(x.to_f64(), None);

        let x: INumber = u64::MAX.into();
        assert_eq!(x.to_i64(), None);
        assert_eq!(x.to_u64(), Some(u64::MAX));
        assert_eq!(x.to_f64(), None);

        let x: INumber = 13369629.into();
        assert_eq!(x.to_i64(), Some(13_369_629));
        assert_eq!(x.to_u64(), Some(13_369_629));
        assert_eq!(x.to_f64(), Some(13_369_629.0));

        let x: INumber = 0x800000.into();
        assert_eq!(x.to_i64(), Some(0x800000));
        assert_eq!(x.to_u64(), Some(0x800000));

        let x: INumber = (-0x800000).into();
        assert_eq!(x.to_i64(), Some(-0x800000));
        assert_eq!(x.to_u64(), None);

        let x: INumber = 0x7FFFFF.into();
        assert_eq!(x.to_i64(), Some(0x7FFFFF));
        assert_eq!(x.to_u64(), Some(0x7FFFFF));

        let x: INumber = (-0x7FFFFF).into();
        assert_eq!(x.to_i64(), Some(-0x7FFFFF));
        assert_eq!(x.to_u64(), None);
    }

    #[mockalloc::test]
    fn can_compare_various_numbers() {
        assert!(INumber::from(1) < INumber::try_from(1.5).unwrap());
        assert!(INumber::from(2) > INumber::try_from(1.5).unwrap());
        assert!(INumber::from(-2) < INumber::try_from(1.5).unwrap());
        assert!(INumber::from(-2) < INumber::try_from(-1.5).unwrap());
        assert!(INumber::from(-2) == INumber::try_from(-2.0).unwrap());
        assert!(INumber::try_from(-1.5).unwrap() > INumber::from(-2));
        assert!(INumber::try_from(1e30).unwrap() > INumber::from(u64::MAX));
        assert!(INumber::try_from(1e30).unwrap() > INumber::from(i64::MAX));
        assert!(INumber::try_from(-1e30).unwrap() < INumber::from(i64::MIN));
        assert!(INumber::try_from(-1e30).unwrap() < INumber::from(i64::MIN));
        assert!(INumber::try_from(99_999_999_000.0).unwrap() < INumber::from(99_999_999_001_u64));
    }
}
