//! Compact Redis JSON documents and their self-describing snapshot encoding.
//!
//! The live tree is Redis's `ijson::IValue`; no second JSON text/DOM is retained.
//! Its normal serde implementation needs `deserialize_any`, which bincode cannot
//! provide. The binary adapter below streams explicit tags and payloads instead,
//! preserving typed arrays without allocating an intermediate serialization tree.

use half::{bf16, f16};
use ijson::array::ArraySliceRef;
use ijson::{DestructuredRef, FloatType, IArray, IObject, IString, IValue, TryExtend};
use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq, SerializeTuple};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::marker::PhantomData;

/// Maximum nesting accepted by snapshot restoration. Ordinary JSON parsing also
/// retains serde_json's recursion limit; commands must not create deeper trees.
pub const MAX_JSON_DEPTH: usize = 128;
const MAX_PREALLOC: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonDocument {
    value: IValue,
}

impl JsonDocument {
    pub fn parse(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        Self::parse_with_float_type(bytes, None)
    }

    pub fn parse_with_float_type(
        bytes: &[u8],
        float_type: Option<FloatType>,
    ) -> Result<Self, serde_json::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let seed = ijson::IValueDeserSeed::new(float_type.map(ijson::FPHAConfig::new_with_type));
        let value = seed.deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(Self { value })
    }

    pub fn from_value(value: IValue) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &IValue {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut IValue {
        &mut self.value
    }

    pub fn into_value(self) -> IValue {
        self.value
    }

    /// Reachable allocations, excluding this handle. Shared strings may be
    /// counted more than once; the process-wide intern table and allocator
    /// rounding/fragmentation are not included. This is not an RSS measurement.
    pub fn estimated_heap_usage(&self) -> usize {
        self.value.mem_allocated()
    }
}

impl Serialize for JsonDocument {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            self.value.serialize(serializer)
        } else {
            SnapshotValue(&self.value).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for JsonDocument {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = if deserializer.is_human_readable() {
            IValue::deserialize(deserializer)?
        } else {
            ValueSeed { depth: 0 }.deserialize(deserializer)?
        };
        Ok(Self { value })
    }
}

struct SnapshotValue<'a>(&'a IValue);

impl Serialize for SnapshotValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(2)?;
        macro_rules! payload {
            ($tag:expr, $value:expr) => {{
                tuple.serialize_element(&($tag as u8))?;
                tuple.serialize_element(&$value)?;
            }};
        }
        match self.0.destructure_ref() {
            DestructuredRef::Null => payload!(0, ()),
            DestructuredRef::Bool(value) => payload!(1, value),
            DestructuredRef::Number(value) => {
                if value.has_decimal_point() {
                    payload!(4, value.to_f64().expect("finite JSON number"));
                } else if let Some(value) = value.to_i64() {
                    payload!(2, value);
                } else {
                    payload!(3, value.to_u64().expect("unsigned JSON number"));
                }
            }
            DestructuredRef::String(value) => payload!(5, value.as_str()),
            DestructuredRef::Object(value) => payload!(7, SnapshotObject(value)),
            DestructuredRef::Array(value) => match value.as_slice() {
                ArraySliceRef::Heterogeneous(values) => payload!(6, SnapshotArray(values)),
                ArraySliceRef::I8(values) => payload!(8, values),
                ArraySliceRef::U8(values) => payload!(9, values),
                ArraySliceRef::I16(values) => payload!(10, values),
                ArraySliceRef::U16(values) => payload!(11, values),
                ArraySliceRef::I32(values) => payload!(12, values),
                ArraySliceRef::U32(values) => payload!(13, values),
                ArraySliceRef::I64(values) => payload!(14, values),
                ArraySliceRef::U64(values) => payload!(15, values),
                ArraySliceRef::F32(values) => payload!(16, values),
                ArraySliceRef::F64(values) => payload!(17, values),
                ArraySliceRef::F16(values) => payload!(18, HalfArray(values)),
                ArraySliceRef::BF16(values) => payload!(19, HalfArray(values)),
            },
        }
        tuple.end()
    }
}

struct SnapshotArray<'a>(&'a [IValue]);

impl Serialize for SnapshotArray<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            seq.serialize_element(&SnapshotValue(value))?;
        }
        seq.end()
    }
}

struct SnapshotObject<'a>(&'a IObject);

impl Serialize for SnapshotObject<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len() as usize))?;
        for (key, value) in self.0 {
            map.serialize_entry(key.as_str(), &SnapshotValue(value))?;
        }
        map.end()
    }
}

trait HalfBits {
    fn bits(&self) -> u16;
}
impl HalfBits for f16 {
    fn bits(&self) -> u16 {
        self.to_bits()
    }
}
impl HalfBits for bf16 {
    fn bits(&self) -> u16 {
        self.to_bits()
    }
}
struct HalfArray<'a, T>(&'a [T]);
impl<T: HalfBits> Serialize for HalfArray<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            seq.serialize_element(&value.bits())?;
        }
        seq.end()
    }
}

#[derive(Clone, Copy)]
struct ValueSeed {
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for ValueSeed {
    type Value = IValue;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<IValue, D::Error> {
        if self.depth > MAX_JSON_DEPTH {
            return Err(D::Error::custom("JSON snapshot nesting limit exceeded"));
        }
        deserializer.deserialize_tuple(2, self)
    }
}

impl<'de> Visitor<'de> for ValueSeed {
    type Value = IValue;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a tagged JSON snapshot value")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<IValue, A::Error> {
        let tag: u8 = seq
            .next_element()?
            .ok_or_else(|| A::Error::custom("missing JSON tag"))?;
        macro_rules! next {
            ($ty:ty) => {
                seq.next_element::<$ty>()?
                    .ok_or_else(|| A::Error::custom("missing JSON payload"))?
            };
        }
        macro_rules! typed {
            ($ty:ty) => {
                seq.next_element_seed(TypedArraySeed::<$ty>(PhantomData))?
                    .ok_or_else(|| A::Error::custom("missing JSON array"))?
                    .into()
            };
        }
        Ok(match tag {
            0 => {
                next!(());
                IValue::NULL
            }
            1 => next!(bool).into(),
            2 => next!(i64).into(),
            3 => next!(u64).into(),
            4 => {
                let value = next!(f64);
                if !value.is_finite() {
                    return Err(A::Error::custom("non-finite JSON number"));
                }
                value.into()
            }
            5 => next!(IString).into(),
            6 => seq
                .next_element_seed(ValuesSeed {
                    depth: self.depth + 1,
                })?
                .ok_or_else(|| A::Error::custom("missing JSON array"))?
                .into(),
            7 => seq
                .next_element_seed(ObjectSeed {
                    depth: self.depth + 1,
                })?
                .ok_or_else(|| A::Error::custom("missing JSON object"))?
                .into(),
            8 => typed!(i8),
            9 => typed!(u8),
            10 => typed!(i16),
            11 => typed!(u16),
            12 => typed!(i32),
            13 => typed!(u32),
            14 => typed!(i64),
            15 => typed!(u64),
            16 => typed!(f32),
            17 => typed!(f64),
            18 => typed!(HalfF16),
            19 => typed!(HalfBf16),
            _ => return Err(A::Error::custom("invalid JSON snapshot tag")),
        })
    }
}

struct ValuesSeed {
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for ValuesSeed {
    type Value = IArray;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<IArray, D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for ValuesSeed {
    type Value = IArray;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON array values")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<IArray, A::Error> {
        let mut array = IArray::with_capacity(seq.size_hint().unwrap_or(0).min(MAX_PREALLOC))
            .map_err(A::Error::custom)?;
        while let Some(value) = seq.next_element_seed(ValueSeed { depth: self.depth })? {
            array.push_heterogeneous(value).map_err(A::Error::custom)?;
        }
        Ok(array)
    }
}

struct ObjectSeed {
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for ObjectSeed {
    type Value = IObject;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<IObject, D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for ObjectSeed {
    type Value = IObject;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON object fields")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<IObject, A::Error> {
        let mut object = IObject::with_capacity(map.size_hint().unwrap_or(0).min(MAX_PREALLOC))
            .map_err(A::Error::custom)?;
        while let Some(key) = map.next_key::<IString>()? {
            let value = map.next_value_seed(ValueSeed { depth: self.depth })?;
            if object
                .insert(key, value)
                .map_err(A::Error::custom)?
                .is_some()
            {
                return Err(A::Error::custom("duplicate JSON snapshot field"));
            }
        }
        Ok(object)
    }
}

// Deserialize directly into the final compact array. No temporary Vec of numbers
// and no per-element JSON nodes survive this step.
trait ArrayElement: for<'de> Deserialize<'de> {
    type Stored;
    fn stored(self) -> Option<Self::Stored>;
}
macro_rules! array_integers {
    ($($ty:ty),*) => { $(impl ArrayElement for $ty {
        type Stored = Self;
        fn stored(self) -> Option<Self> { Some(self) }
    })* };
}
array_integers!(i8, u8, i16, u16, i32, u32, i64, u64);
macro_rules! array_floats {
    ($($ty:ty),*) => { $(impl ArrayElement for $ty {
        type Stored = Self;
        fn stored(self) -> Option<Self> { self.is_finite().then_some(self) }
    })* };
}
array_floats!(f32, f64);

#[derive(Deserialize)]
struct HalfF16(u16);
impl ArrayElement for HalfF16 {
    type Stored = f16;
    fn stored(self) -> Option<f16> {
        let value = f16::from_bits(self.0);
        value.is_finite().then_some(value)
    }
}
#[derive(Deserialize)]
struct HalfBf16(u16);
impl ArrayElement for HalfBf16 {
    type Stored = bf16;
    fn stored(self) -> Option<bf16> {
        let value = bf16::from_bits(self.0);
        value.is_finite().then_some(value)
    }
}

struct TypedArraySeed<T>(PhantomData<T>);
impl<'de, T: ArrayElement> DeserializeSeed<'de> for TypedArraySeed<T>
where
    IArray: TryExtend<T::Stored>,
{
    type Value = IArray;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<IArray, D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de, T: ArrayElement> Visitor<'de> for TypedArraySeed<T>
where
    IArray: TryExtend<T::Stored>,
{
    type Value = IArray;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a compact numeric JSON array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<IArray, A::Error> {
        let mut array = IArray::new();
        array
            .try_extend(std::iter::empty::<T::Stored>())
            .map_err(A::Error::custom)?;
        array
            .reserve(seq.size_hint().unwrap_or(0).min(MAX_PREALLOC))
            .map_err(A::Error::custom)?;
        while let Some(value) = seq.next_element::<T>()? {
            let value = value
                .stored()
                .ok_or_else(|| A::Error::custom("non-finite JSON array value"))?;
            array
                .try_extend(std::iter::once(value))
                .map_err(A::Error::custom)?;
        }
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_document_snapshot_preserves_scalars_arrays_and_object_order() {
        for input in [
            r#"{"z":null,"a":true,"float":-0.0,"int":-9223372036854775808,"uint":18446744073709551615}"#,
            r#"{"small":[1,2,255],"signed":[-129,0,32767],"float":[0.3,1.25],"mixed":[1,"x",{"long_field":"shared long string"}]}"#,
        ] {
            let value = JsonDocument::parse(input.as_bytes()).unwrap();
            let encoded = bincode2::serialize(&value).unwrap();
            let restored: JsonDocument = bincode2::deserialize(&encoded).unwrap();
            assert_eq!(
                serde_json::to_string(&value).unwrap(),
                serde_json::to_string(&restored).unwrap()
            );
            assert_eq!(encoded, bincode2::serialize(&restored).unwrap());
        }
    }

    #[test]
    fn json_document_snapshot_preserves_explicit_float_array_precision() {
        for float_type in [
            FloatType::F16,
            FloatType::BF16,
            FloatType::F32,
            FloatType::F64,
        ] {
            let value =
                JsonDocument::parse_with_float_type(b"[0.3,-0.0,1.23456]", Some(float_type))
                    .unwrap();
            let encoded = bincode2::serialize(&value).unwrap();
            let restored: JsonDocument = bincode2::deserialize(&encoded).unwrap();
            assert_eq!(
                value.value().as_array().unwrap().as_slice().type_tag(),
                restored.value().as_array().unwrap().as_slice().type_tag()
            );
            assert_eq!(encoded, bincode2::serialize(&restored).unwrap());
        }
    }

    #[test]
    fn json_document_snapshot_keeps_heterogeneous_numeric_arrays() {
        let mut array = IArray::new();
        array.push_heterogeneous(1.into()).unwrap();
        array.push_heterogeneous(2.into()).unwrap();
        let value = JsonDocument::from_value(array.into());
        let encoded = bincode2::serialize(&value).unwrap();
        let restored: JsonDocument = bincode2::deserialize(&encoded).unwrap();
        assert!(
            restored
                .value()
                .as_array()
                .unwrap()
                .as_slice()
                .is_heterogeneous()
        );
        assert_eq!(encoded, bincode2::serialize(&restored).unwrap());
    }

    #[test]
    fn json_document_rejects_trailing_json_and_invalid_snapshot_values() {
        assert!(JsonDocument::parse(b"{} true").is_err());
        assert!(bincode2::deserialize::<JsonDocument>(&[255]).is_err());
        let mut nan = vec![4];
        nan.extend_from_slice(&f64::NAN.to_le_bytes());
        assert!(bincode2::deserialize::<JsonDocument>(&nan).is_err());
        let mut nested = Vec::new();
        for _ in 0..MAX_JSON_DEPTH + 2 {
            nested.push(6);
            nested.extend_from_slice(&1u64.to_le_bytes());
        }
        nested.push(0);
        assert!(bincode2::deserialize::<JsonDocument>(&nested).is_err());
    }

    #[test]
    fn json_document_matches_redis_nesting_boundary() {
        // RedisJSON's nest_object(depth) fixture counts the scalar leaf as a
        // level: depth 128 therefore contains 127 objects, not 128 objects.
        let nested =
            |depth: usize| format!("{}0{}", "{\"a\":".repeat(depth - 1), "}".repeat(depth - 1));
        let valid = JsonDocument::parse(nested(128).as_bytes()).unwrap();
        let encoded = bincode2::serialize(&valid).unwrap();
        let restored: JsonDocument = bincode2::deserialize(&encoded).unwrap();
        assert_eq!(encoded, bincode2::serialize(&restored).unwrap());
        assert!(
            JsonDocument::parse(nested(129).as_bytes())
                .unwrap_err()
                .to_string()
                .contains("recursion limit exceeded")
        );
        // Parentheses inside strings do not consume parser recursion depth.
        let text = serde_json::to_vec(&"[{}]".repeat(1_000)).unwrap();
        assert!(JsonDocument::parse(&text).is_ok());
    }

    #[test]
    fn json_document_is_compact_and_its_clone_is_isolated() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<JsonDocument>();
        assert_eq!(size_of::<JsonDocument>(), size_of::<usize>());
        assert_eq!(
            JsonDocument::parse(br#""abcdefg""#)
                .unwrap()
                .estimated_heap_usage(),
            0
        );
        let original =
            JsonDocument::parse(br#"{"a":{"value":1},"long_field":"shared long string"}"#).unwrap();
        let mut copied = original.clone();
        *copied
            .value_mut()
            .as_object_mut()
            .unwrap()
            .get_mut("a")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .get_mut("value")
            .unwrap() = 2.into();
        assert_eq!(original.value()["a"]["value"].to_i64(), Some(1));
        assert_eq!(copied.value()["a"]["value"].to_i64(), Some(2));
    }

    #[test]
    fn json_documents_share_strings_across_concurrent_owners_and_drops() {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..500 {
                        let first = JsonDocument::parse(
                            br#"{"shared_field_name":"shared long value","n":1}"#,
                        )
                        .unwrap();
                        let second = first.clone();
                        assert_eq!(
                            first.value()["shared_field_name"]
                                .as_string()
                                .unwrap()
                                .as_str(),
                            "shared long value"
                        );
                        let encoded = bincode2::serialize(&second).unwrap();
                        let decoded: JsonDocument = bincode2::deserialize(&encoded).unwrap();
                        assert_eq!(first, decoded);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
