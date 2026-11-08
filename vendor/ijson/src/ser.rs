use half::{bf16, f16};
use serde::{
    ser::{
        Error as _, Impossible, SerializeMap, SerializeSeq, SerializeStruct,
        SerializeStructVariant, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
    },
    Serialize, Serializer,
};
use serde_json::error::Error;

use crate::{
    array::{ArraySliceRef, TryCollect},
    DestructuredRef, IArray, INumber, IObject, IString, IValue,
};

/// Rounds an f64 to the given number of significant decimal digits.
#[inline]
fn round_to_sig_digits(val: f64, sig_digits: u32) -> f64 {
    // how many digits are left of the decimal point, minus one
    // OR: what power of 10 is this number closest to? (e.g. 100 -> 10^2, 0.001 -> 10^-3)
    let order_of_magnitude = val.abs().log10().floor() as i32;
    // Multiplier that shifts the desired significant digits into the integer part,
    // so that f64::round() can snap to the nearest integer and discard the rest.
    // e.g. for val=3.14, order_of_magnitude = 0, sig_digits=2: scale=10, so 3.14*10=31.4 → round→31 → 31/10=3.1
    let scale = 10f64.powi(sig_digits as i32 - 1 - order_of_magnitude);
    (val * scale).round() / scale
}

/// Finds an f64 value that, when formatted by ryu's f64 algorithm, produces
/// the shortest decimal string that still round-trips through the target
/// half-precision type (f16 or bf16).
///
/// ryu only supports f32/f64, and serde has no `serialize_f16`. Since f16/bf16
/// have far fewer distinct values than f32, there exist shorter representations
/// that uniquely identify the half value. For example, f16(0.3) = 0.300048828125,
/// and "0.3" parsed as f16 gives back the same bits — so "0.3" is valid.
///
/// The approach: try rounding to increasing significant digits until the
/// rounded value round-trips through the type. Then return that f64
/// value, so that `serialize_f64` (via ryu) reproduces it.
fn find_shortest_roundtrip_f64(f64_val: f64, roundtrips: impl Fn(f64) -> bool) -> f64 {
    if !f64_val.is_finite() || f64_val.fract() == 0.0 {
        return f64_val;
    }
    // With our usage(F16/BF16), the loop will need only ~4 iterations, since max significant digits needed is ~4
    // Example: f16(3.14159) stores 3.140625
    //   sig_digits=1 → 3.0  → f16(3.0)=3.0 ≠ 3.140625  ❌
    //   sig_digits=2 → 3.1  → f16(3.1)=3.099.. ≠ 3.140625  ❌
    //   sig_digits=3 → 3.14 → f16(3.14)=3.140625  ✅ → returns 3.14
    for sig_digits in 1..=5u32 {
        let rounded = round_to_sig_digits(f64_val, sig_digits);
        if roundtrips(rounded) {
            return rounded;
        }
    }
    f64_val
}

impl Serialize for IValue {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.destructure_ref() {
            DestructuredRef::Null => serializer.serialize_unit(),
            DestructuredRef::Bool(b) => serializer.serialize_bool(b),
            DestructuredRef::Number(n) => n.serialize(serializer),
            DestructuredRef::String(s) => s.serialize(serializer),
            DestructuredRef::Array(v) => v.serialize(serializer),
            DestructuredRef::Object(o) => o.serialize(serializer),
        }
    }
}

impl Serialize for INumber {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.has_decimal_point() {
            serializer.serialize_f64(self.to_f64().unwrap())
        } else if let Some(v) = self.to_i64() {
            serializer.serialize_i64(v)
        } else {
            serializer.serialize_u64(self.to_u64().unwrap())
        }
    }
}

impl Serialize for IString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Serialize for IArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.as_slice() {
            // Serialize typed float arrays with the shortest representation that
            // round-trips through the stored precision. Without this, all floats
            // would be promoted to f64 via INumber, and ryu's f64 algorithm would
            // emit unnecessarily long strings (e.g. "0.3" stored as f32 would
            // serialize as "0.30000001192092896" instead of "0.3").
            //
            // F32: serialize directly as f32 so ryu uses its f32 algorithm.
            // F16/BF16: ryu has no f16 mode and serde has no serialize_f16, so we
            // find the shortest decimal that round-trips through the half type and
            // pass the corresponding f64 value to serialize_f64.
            ArraySliceRef::F32(slice) => {
                let mut s = serializer.serialize_seq(Some(slice.len()))?;
                for &v in slice {
                    s.serialize_element(&v)?;
                }
                s.end()
            }
            ArraySliceRef::F16(slice) => {
                let mut s = serializer.serialize_seq(Some(slice.len()))?;
                for &v in slice {
                    let f64_val = f64::from(v);
                    let shortest = find_shortest_roundtrip_f64(f64_val, |p| f16::from_f64(p) == v);
                    s.serialize_element(&shortest)?;
                }
                s.end()
            }
            ArraySliceRef::BF16(slice) => {
                let mut s = serializer.serialize_seq(Some(slice.len()))?;
                for &v in slice {
                    let f64_val = f64::from(v);
                    let shortest = find_shortest_roundtrip_f64(f64_val, |p| bf16::from_f64(p) == v);
                    s.serialize_element(&shortest)?;
                }
                s.end()
            }
            _ => {
                let mut s = serializer.serialize_seq(Some(self.len() as usize))?;
                for v in self {
                    s.serialize_element(&v)?;
                }
                s.end()
            }
        }
    }
}

impl Serialize for IObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut m = serializer.serialize_map(Some(self.len() as usize))?;
        for (k, v) in self {
            m.serialize_entry(k, v)?;
        }
        m.end()
    }
}

pub struct ValueSerializer;

impl Serializer for ValueSerializer {
    type Ok = IValue;
    type Error = Error;

    type SerializeSeq = SerializeArray;
    type SerializeTuple = SerializeArray;
    type SerializeTupleStruct = SerializeArray;
    type SerializeTupleVariant = SerializeArrayVariant;
    type SerializeMap = SerializeObject;
    type SerializeStruct = SerializeObject;
    type SerializeStructVariant = SerializeObjectVariant;

    #[inline]
    fn serialize_bool(self, value: bool) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_i8(self, value: i8) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_i16(self, value: i16) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_i32(self, value: i32) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    fn serialize_i64(self, value: i64) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_u8(self, value: u8) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_u16(self, value: u16) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_u32(self, value: u32) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_u64(self, value: u64) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_f32(self, value: f32) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_f64(self, value: f64) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    #[inline]
    fn serialize_char(self, value: char) -> Result<IValue, Self::Error> {
        let mut buffer = [0_u8; 4];
        Ok(value.encode_utf8(&mut buffer).into())
    }

    #[inline]
    fn serialize_str(self, value: &str) -> Result<IValue, Self::Error> {
        Ok(value.into())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<IValue, Self::Error> {
        value
            .iter()
            .copied()
            .try_collect::<IArray>()
            .map(Into::into)
            .map_err(|_| Error::custom("Failed to allocate array"))
    }

    #[inline]
    fn serialize_unit(self) -> Result<IValue, Self::Error> {
        Ok(IValue::NULL)
    }

    #[inline]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<IValue, Self::Error> {
        self.serialize_unit()
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<IValue, Self::Error> {
        self.serialize_str(variant)
    }

    #[inline]
    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<IValue, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<IValue, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let mut obj = IObject::new();
        obj.insert(variant, value.serialize(self)?)
            .map_err(|e| Error::custom(e.to_string()))?;
        Ok(obj.into())
    }

    #[inline]
    fn serialize_none(self) -> Result<IValue, Self::Error> {
        self.serialize_unit()
    }

    #[inline]
    fn serialize_some<T>(self, value: &T) -> Result<IValue, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SerializeArray {
            array: IArray::with_capacity(len.unwrap_or(0))
                .map_err(|_| Error::custom("Failed to allocate array"))?,
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SerializeArrayVariant {
            name: variant.into(),
            array: IArray::with_capacity(len)
                .map_err(|_| Error::custom("Failed to allocate array"))?,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(SerializeObject {
            object: IObject::with_capacity(len.unwrap_or(0))
                .map_err(|e| Error::custom(e.to_string()))?,
            next_key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(SerializeObjectVariant {
            name: variant.into(),
            object: IObject::with_capacity(len).map_err(|e| Error::custom(e.to_string()))?,
        })
    }
}

pub struct SerializeArray {
    array: IArray,
}

pub struct SerializeArrayVariant {
    name: IString,
    array: IArray,
}

pub struct SerializeObject {
    object: IObject,
    next_key: Option<IString>,
}

pub struct SerializeObjectVariant {
    name: IString,
    object: IObject,
}

impl SerializeSeq for SerializeArray {
    type Ok = IValue;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.array
            .push(value.serialize(ValueSerializer)?)
            .map_err(|_| Error::custom("Failed to push to array"))?;
        Ok(())
    }

    fn end(self) -> Result<IValue, Self::Error> {
        Ok(self.array.into())
    }
}

impl SerializeTuple for SerializeArray {
    type Ok = IValue;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<IValue, Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleStruct for SerializeArray {
    type Ok = IValue;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<IValue, Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleVariant for SerializeArrayVariant {
    type Ok = IValue;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.array
            .push(value.serialize(ValueSerializer)?)
            .map_err(|_| Error::custom("Failed to push to array"))?;
        Ok(())
    }

    fn end(self) -> Result<IValue, Self::Error> {
        let mut object = IObject::new();
        object
            .insert(self.name, self.array)
            .map_err(|e| Error::custom(e.to_string()))?;

        Ok(object.into())
    }
}

impl SerializeMap for SerializeObject {
    type Ok = IValue;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.next_key = Some(key.serialize(ObjectKeySerializer)?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // Panic because this indicates a bug in the program rather than an
        // expected failure.
        let key = self
            .next_key
            .take()
            .expect("serialize_value called before serialize_key");
        self.object
            .insert(key, value.serialize(ValueSerializer)?)
            .map_err(|e| Error::custom(e.to_string()))?;
        Ok(())
    }

    fn end(self) -> Result<IValue, Self::Error> {
        Ok(self.object.into())
    }
}

struct ObjectKeySerializer;

fn key_must_be_a_string() -> Error {
    Error::custom("Object key must be a string")
}

impl Serializer for ObjectKeySerializer {
    type Ok = IString;
    type Error = Error;

    type SerializeSeq = Impossible<IString, Error>;
    type SerializeTuple = Impossible<IString, Error>;
    type SerializeTupleStruct = Impossible<IString, Error>;
    type SerializeTupleVariant = Impossible<IString, Error>;
    type SerializeMap = Impossible<IString, Error>;
    type SerializeStruct = Impossible<IString, Error>;
    type SerializeStructVariant = Impossible<IString, Error>;

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<IString, Self::Error> {
        Ok(variant.into())
    }

    #[inline]
    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<IString, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_bool(self, _value: bool) -> Result<IString, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_i8(self, value: i8) -> Result<IString, Self::Error> {
        Ok(value.to_string().into())
    }

    fn serialize_i16(self, value: i16) -> Result<IString, Self::Error> {
        Ok(value.to_string().into())
    }

    fn serialize_i32(self, value: i32) -> Result<IString, Self::Error> {
        Ok(value.to_string().into())
    }

    fn serialize_i64(self, value: i64) -> Result<IString, Self::Error> {
        Ok(value.to_string().into())
    }

    fn serialize_u8(self, value: u8) -> Result<IString, Self::Error> {
        Ok(value.to_string().into())
    }

    fn serialize_u16(self, value: u16) -> Result<IString, Self::Error> {
        Ok(value.to_string().into())
    }

    fn serialize_u32(self, value: u32) -> Result<IString, Self::Error> {
        Ok(value.to_string().into())
    }

    fn serialize_u64(self, value: u64) -> Result<IString, Self::Error> {
        Ok(value.to_string().into())
    }

    fn serialize_f32(self, _value: f32) -> Result<IString, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_f64(self, _value: f64) -> Result<IString, Self::Error> {
        Err(key_must_be_a_string())
    }

    #[inline]
    fn serialize_char(self, value: char) -> Result<IString, Self::Error> {
        let mut buffer = [0_u8; 4];
        Ok(value.encode_utf8(&mut buffer).into())
    }

    #[inline]
    fn serialize_str(self, value: &str) -> Result<IString, Self::Error> {
        Ok(value.into())
    }

    fn serialize_bytes(self, _value: &[u8]) -> Result<IString, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_unit(self) -> Result<IString, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<IString, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<IString, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(key_must_be_a_string())
    }

    fn serialize_none(self) -> Result<IString, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_some<T>(self, _value: &T) -> Result<IString, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(key_must_be_a_string())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(key_must_be_a_string())
    }
}

impl SerializeStruct for SerializeObject {
    type Ok = IValue;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeMap::serialize_entry(self, key, value)
    }

    fn end(self) -> Result<IValue, Self::Error> {
        SerializeMap::end(self)
    }
}

impl SerializeStructVariant for SerializeObjectVariant {
    type Ok = IValue;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.object
            .insert(key, value.serialize(ValueSerializer)?)
            .map_err(|e| Error::custom(e.to_string()))?;
        Ok(())
    }

    fn end(self) -> Result<IValue, Self::Error> {
        let mut object = IObject::new();
        object
            .insert(self.name, self.object)
            .map_err(|e| Error::custom(e.to_string()))?;
        Ok(object.into())
    }
}

/// Converts an arbitrary type to an [`IValue`] using that type's [`serde::Serialize`]
/// implementation.
/// # Errors
///
/// Will return `Error` if `value` fails to serialize.
pub fn to_value<T>(value: T) -> Result<IValue, Error>
where
    T: Serialize,
{
    value.serialize(ValueSerializer)
}

#[cfg(test)]
mod tests {
    use crate::array::{ArraySliceRef, FloatType};
    use crate::{FPHAConfig, IArray, IValue, IValueDeserSeed};

    #[test]
    fn test_f32_array_serialization_preserves_short_representation() {
        let mut arr = IArray::new();
        arr.push_with_fp_type(IValue::from(0.3), FloatType::F32)
            .unwrap();
        assert!(matches!(arr.as_slice(), ArraySliceRef::F32(_)));

        let json = serde_json::to_string(&arr).unwrap();
        assert_eq!(
            json, "[0.3]",
            "F32 array should serialize 0.3 as '0.3', not with extra f64 precision digits"
        );
    }

    #[test]
    fn test_f64_array_serialization_preserves_short_representation() {
        let mut arr = IArray::new();
        arr.push_with_fp_type(IValue::from(0.3), FloatType::F64)
            .unwrap();
        assert!(matches!(arr.as_slice(), ArraySliceRef::F64(_)));

        let json = serde_json::to_string(&arr).unwrap();
        assert_eq!(json, "[0.3]");
    }

    #[test]
    fn test_f16_array_serialization_preserves_short_representation() {
        let mut arr = IArray::new();
        arr.push_with_fp_type(IValue::from(1.5), FloatType::F16)
            .unwrap();
        assert_eq!(serde_json::to_string(&arr).unwrap(), "[1.5]");

        let mut arr2 = IArray::new();
        arr2.push_with_fp_type(IValue::from(0.3), FloatType::F16)
            .unwrap();
        assert_eq!(
            serde_json::to_string(&arr2).unwrap(),
            "[0.3]",
            "F16 array should serialize 0.3 as '0.3', not '0.30004883' or '0.300048828125'"
        );
    }

    #[test]
    fn test_bf16_array_serialization_preserves_short_representation() {
        let mut arr = IArray::new();
        arr.push_with_fp_type(IValue::from(1.5), FloatType::BF16)
            .unwrap();
        assert_eq!(serde_json::to_string(&arr).unwrap(), "[1.5]");

        let mut arr2 = IArray::new();
        arr2.push_with_fp_type(IValue::from(0.3), FloatType::BF16)
            .unwrap();
        assert_eq!(
            serde_json::to_string(&arr2).unwrap(),
            "[0.3]",
            "BF16 array should serialize 0.3 as '0.3'"
        );
    }

    #[test]
    fn test_typed_float_array_serialization_roundtrip() {
        let input = "[0.3,0.1,0.7,1.0,2.5,100.0]";
        let fp_types = [
            FloatType::F16,
            FloatType::BF16,
            FloatType::F32,
            FloatType::F64,
        ];

        let jsons: Vec<String> = fp_types
            .iter()
            .map(|&fp_type| {
                let seed = IValueDeserSeed::new(Some(FPHAConfig::new_with_type(fp_type)));
                let mut de = serde_json::Deserializer::from_str(input);
                let arr = serde::de::DeserializeSeed::deserialize(seed, &mut de)
                    .unwrap()
                    .into_array()
                    .unwrap();
                let json_out = serde_json::to_string(&arr).unwrap();
                assert_eq!(
                    json_out, input,
                    "{fp_type} round-trip should preserve the original JSON string"
                );
                json_out
            })
            .collect();

        for pair in jsons.windows(2) {
            assert_eq!(
                pair[0], pair[1],
                "all float types should produce identical JSON"
            );
        }
    }

    #[test]
    fn test_f16_precision_loss_produces_different_but_short_representation() {
        // Values with more significant digits than f16 can represent (~3.3 digits).
        // The stored f16 value differs from the original, so the serialized string
        // must differ too — but it should still be the shortest string that
        // round-trips through f16.
        let cases: &[(&str, &str)] = &[
            ("3.14159", "3.14"), // pi truncated: f16 stores 3.140625
            ("42.42", "42.4"),   // f16 stores 42.40625
            ("12.345", "12.34"), // f16 stores 12.34375
            ("0.5678", "0.568"), // f16 stores 0.56787109375
        ];

        for &(input, expected_f16) in cases {
            let json_input = format!("[{input}]");

            let f16_arr: IArray = {
                let seed = IValueDeserSeed::new(Some(FPHAConfig::new_with_type(FloatType::F16)));
                let mut de = serde_json::Deserializer::from_str(&json_input);
                serde::de::DeserializeSeed::deserialize(seed, &mut de)
                    .unwrap()
                    .into_array()
                    .unwrap()
            };
            let f16_json = serde_json::to_string(&f16_arr).unwrap();
            assert_eq!(
                f16_json,
                format!("[{expected_f16}]"),
                "F16 of {input}: should serialize as shortest f16 representation"
            );

            // Same values through F32 should preserve the original (enough precision)
            let f32_arr: IArray = {
                let seed = IValueDeserSeed::new(Some(FPHAConfig::new_with_type(FloatType::F32)));
                let mut de = serde_json::Deserializer::from_str(&json_input);
                serde::de::DeserializeSeed::deserialize(seed, &mut de)
                    .unwrap()
                    .into_array()
                    .unwrap()
            };
            let f32_json = serde_json::to_string(&f32_arr).unwrap();
            assert_eq!(
                f32_json, json_input,
                "F32 of {input}: should preserve the original representation"
            );
        }
    }

    #[test]
    fn test_negative_float_array_serialization() {
        let input = "[-0.3,-0.1,-1.0,-2.5,-100.0]";
        let fp_types = [
            FloatType::F16,
            FloatType::BF16,
            FloatType::F32,
            FloatType::F64,
        ];

        for &fp_type in &fp_types {
            let seed = IValueDeserSeed::new(Some(FPHAConfig::new_with_type(fp_type)));
            let mut de = serde_json::Deserializer::from_str(input);
            let arr = serde::de::DeserializeSeed::deserialize(seed, &mut de)
                .unwrap()
                .into_array()
                .unwrap();
            let json_out = serde_json::to_string(&arr).unwrap();
            assert_eq!(
                json_out, input,
                "{fp_type} negative round-trip should preserve the original JSON string"
            );
        }
    }

    #[test]
    fn test_negative_f16_precision_loss_produces_short_representation() {
        let cases: &[(&str, &str)] = &[
            ("-3.14159", "-3.14"),
            ("-42.42", "-42.4"),
            ("-0.5678", "-0.568"),
        ];

        for &(input, expected_f16) in cases {
            let json_input = format!("[{input}]");
            let seed = IValueDeserSeed::new(Some(FPHAConfig::new_with_type(FloatType::F16)));
            let mut de = serde_json::Deserializer::from_str(&json_input);
            let arr = serde::de::DeserializeSeed::deserialize(seed, &mut de)
                .unwrap()
                .into_array()
                .unwrap();
            let json_out = serde_json::to_string(&arr).unwrap();
            assert_eq!(
                json_out,
                format!("[{expected_f16}]"),
                "F16 of {input}: negative should serialize as shortest f16 representation"
            );
        }
    }
}
