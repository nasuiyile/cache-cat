use crate::raft::types::core::response_value::Value;
use half::{bf16, f16};
use ijson::{IArray, IValue, array::ArraySliceMut};
use serde_json::Number;

fn overflow() -> Value {
    Value::error("ERR numeric overflow")
}

fn not_a_number() -> Value {
    Value::error("ERR result is not a number")
}

pub(super) fn increment_value(slot: &mut IValue, by: &Number) -> Result<Number, Value> {
    let current = slot.as_number().ok_or_else(not_a_number)?;
    if !current.has_decimal_point()
        && let (Some(left), Some(right)) = (current.to_i64(), by.as_i64())
    {
        let sum = left.checked_add(right).ok_or_else(overflow)?;
        *slot = IValue::from(sum);
        return Ok(Number::from(sum));
    }
    let sum = current.to_f64_lossy() + by.as_f64().ok_or_else(not_a_number)?;
    let result = Number::from_f64(sum).ok_or_else(not_a_number)?;
    *slot = IValue::from(sum);
    Ok(result)
}

/// A packed integer can use the full signed or unsigned 64-bit range.
fn packed_integer_sum(current: i128, by: &Number) -> Result<Number, Value> {
    if let Some(integer) = by.as_i128() {
        return Number::from_i128(current + integer).ok_or_else(overflow);
    }
    let sum = current as f64 + by.as_f64().ok_or_else(not_a_number)?;
    Number::from_f64(sum).ok_or_else(not_a_number)
}

pub(super) fn increment_element(
    array: &mut IArray,
    index: usize,
    by: &Number,
) -> Result<Number, Value> {
    // The path traversal supplies a live index, including repeated visits after
    // the preceding result promoted the array to a different numeric type.
    macro_rules! integer {
        ($values:expr, $ty:ty) => {{
            let slot = &mut $values[index];
            let result = packed_integer_sum(i128::from(*slot), by)?;
            if let Some(sum) = result.as_i128()
                && let Ok(narrow) = <$ty>::try_from(sum)
            {
                *slot = narrow;
                return Ok(result);
            }
            result
        }};
    }
    macro_rules! float {
        ($values:expr, $narrow:expr, $error:expr) => {{
            let slot = &mut $values[index];
            let sum = f64::from(*slot) + by.as_f64().ok_or_else(not_a_number)?;
            if !sum.is_finite() {
                return Err($error());
            }
            let narrow = ($narrow)(sum);
            if narrow.is_finite() {
                *slot = narrow;
                return Ok(Number::from_f64(f64::from(narrow)).expect("finite narrowed result"));
            }
            Number::from_f64(sum).expect("finite sum")
        }};
    }

    let result = match array.as_mut_slice() {
        ArraySliceMut::Heterogeneous(values) => return increment_value(&mut values[index], by),
        ArraySliceMut::I8(values) => integer!(values, i8),
        ArraySliceMut::U8(values) => integer!(values, u8),
        ArraySliceMut::I16(values) => integer!(values, i16),
        ArraySliceMut::U16(values) => integer!(values, u16),
        ArraySliceMut::I32(values) => integer!(values, i32),
        ArraySliceMut::U32(values) => integer!(values, u32),
        ArraySliceMut::I64(values) => integer!(values, i64),
        ArraySliceMut::U64(values) => integer!(values, u64),
        ArraySliceMut::F16(values) => float!(values, f16::from_f64, not_a_number),
        ArraySliceMut::BF16(values) => float!(values, bf16::from_f64, not_a_number),
        ArraySliceMut::F32(values) => float!(values, |n: f64| n as f32, overflow),
        ArraySliceMut::F64(values) => float!(values, |n: f64| n, overflow),
    };
    let value = if let Some(sum) = result.as_i64() {
        IValue::from(sum)
    } else if let Some(sum) = result.as_u64() {
        IValue::from(sum)
    } else {
        IValue::from(result.as_f64().expect("floating-point sum"))
    };
    // RedisJSON removes and reinserts an element when its type changes. In a
    // singleton array the result therefore determines the new storage type.
    if array.len() == 1 {
        let mut replacement = IArray::new();
        replacement
            .push(value)
            .map_err(|err| Value::error(err.to_string()))?;
        *array = replacement;
    } else {
        array
            .replace(index, value)
            .map_err(|err| Value::error(err.to_string()))?;
    }
    Ok(result)
}
