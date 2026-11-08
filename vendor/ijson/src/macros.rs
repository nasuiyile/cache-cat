macro_rules! value_subtype_impls {
    ($t:ty, $cf:ident, $rcf:ident, $mcf:ident) => {
        impl std::convert::AsRef<crate::IValue> for $t {
            fn as_ref(&self) -> &crate::IValue {
                &self.0
            }
        }
        impl std::convert::AsMut<crate::IValue> for $t {
            fn as_mut(&mut self) -> &mut crate::IValue {
                &mut self.0
            }
        }
        impl std::borrow::Borrow<crate::IValue> for $t {
            fn borrow(&self) -> &crate::IValue {
                &self.0
            }
        }
        impl std::borrow::BorrowMut<crate::IValue> for $t {
            fn borrow_mut(&mut self) -> &mut crate::IValue {
                &mut self.0
            }
        }
        impl std::convert::From<$t> for crate::IValue {
            fn from(other: $t) -> Self {
                other.0
            }
        }
        impl std::convert::TryFrom<crate::IValue> for $t {
            type Error = crate::IValue;
            fn try_from(other: crate::IValue) -> Result<Self, crate::IValue> {
                other.$cf()
            }
        }
        impl<'a> std::convert::TryFrom<&'a crate::IValue> for &'a $t {
            type Error = ();
            fn try_from(other: &'a crate::IValue) -> Result<Self, ()> {
                other.$rcf().ok_or(())
            }
        }
        impl<'a> std::convert::TryFrom<&'a mut crate::IValue> for &'a mut $t {
            type Error = ();
            fn try_from(other: &'a mut crate::IValue) -> Result<Self, ()> {
                other.$mcf().ok_or(())
            }
        }
    };
}

macro_rules! typed_conversions {
    ($(
        $interm:ty: $(
            $src:ty
            $(where ($($gb:tt)*))*
        ),*;
    )*) => {
        $(
            $(
                impl $(<$($gb)*>)* From<$src> for IValue {
                    fn from(other: $src) -> Self {
                        <$interm>::from(other).into()
                    }
                }
            )*
        )*
    }
}
macro_rules! typed_conversions_fallible {
    ($(
        $interm:ty: $(
            $src:ty
            $(where ($($gb:tt)*))*
        ),*;
    )*) => {
        $(
            $(
                impl $(<$($gb)*>)* From<$src> for IValue {
                    fn from(other: $src) -> Self {
                        <$interm>::try_from(other).map(Into::into).unwrap_or(IValue::NULL)
                    }
                }
            )*
        )*
    }
}

/// Construct an [`IValue`] using familiar JSON syntax.
///
/// For example:
/// ```
/// use ijson::{ijson, IValue};
///
/// let _: IValue = ijson!({ "foo": null, "bar": [1, 2, 3, 4] });
/// ```
///
/// [`IValue`]: super::IValue
#[macro_export(local_inner_macros)]
macro_rules! ijson {
    // Hide implementation details from the generated rustdoc.
    ($($json:tt)+) => {
        $crate::ijson_internal!($($json)+)
    };
}

#[macro_export(local_inner_macros)]
#[doc(hidden)]
macro_rules! ijson_internal {
    // Done without trailing comma.
    (@array $array:ident) => {};

    // Done with trailing comma.
    (@array $array:ident ,) => {};

    // Next element is `null`.
    (@array $array:ident , null $($rest:tt)*) => {
        $array.push(ijson_internal!(null)).unwrap();
        ijson_internal!(@array $array $($rest)*)
    };

    // Next element is `true`.
    (@array $array:ident , true $($rest:tt)*) => {
        $array.push(ijson_internal!(true)).unwrap();
        ijson_internal!(@array $array $($rest)*)
    };

    // Next element is `false`.
    (@array $array:ident , false $($rest:tt)*) => {
        $array.push(ijson_internal!(false)).unwrap();
        ijson_internal!(@array $array $($rest)*)
    };

    // Next element is an array.
    (@array $array:ident , [$($arr:tt)*] $($rest:tt)*) => {
        $array.push(ijson_internal!([$($arr)*])).unwrap();
        ijson_internal!(@array $array $($rest)*)
    };

    // Next element is an object.
    (@array $array:ident , {$($obj:tt)*} $($rest:tt)*) => {
        $array.push(ijson_internal!({$($obj)*})).unwrap();
        ijson_internal!(@array $array $($rest)*)
    };

    // Next element is an expression followed by comma.
    (@array $array:ident , $next:expr , $($rest:tt)*) => {
        $array.push(ijson_internal!($next)).unwrap();
        ijson_internal!(@array $array , $($rest)*)
    };

    // Last element is an expression with no trailing comma.
    (@array $array:ident , $last:expr) => {
        $array.push(ijson_internal!($last)).unwrap();
    };

    // Unexpected token after most recent element.
    (@array $array:ident , $unexpected:tt $($rest:tt)*) => {
        ijson_unexpected!($unexpected)
    };

    // Unexpected token after most recent element.
    (@array $array:ident $unexpected:tt $($rest:tt)*) => {
        ijson_unexpected!($unexpected)
    };

    // Done.
    (@object $object:ident () () ()) => {};

    // Insert the current entry followed by trailing comma.
    (@object $object:ident [$($key:tt)+] ($value:expr) , $($rest:tt)*) => {
        $object.insert(($($key)+), $value).unwrap();
        ijson_internal!(@object $object () ($($rest)*) ($($rest)*));
    };

    // Current entry followed by unexpected token.
    (@object $object:ident [$($key:tt)+] ($value:expr) $unexpected:tt $($rest:tt)*) => {
        ijson_unexpected!($unexpected);
    };

    // Insert the last entry without trailing comma.
    (@object $object:ident [$($key:tt)+] ($value:expr)) => {
        $object.insert(($($key)+), $value).unwrap();
    };

    // Next value is `null`.
    (@object $object:ident ($($key:tt)+) (: null $($rest:tt)*) $copy:tt) => {
        ijson_internal!(@object $object [$($key)+] (ijson_internal!(null)) $($rest)*);
    };

    // Next value is `true`.
    (@object $object:ident ($($key:tt)+) (: true $($rest:tt)*) $copy:tt) => {
        ijson_internal!(@object $object [$($key)+] (ijson_internal!(true)) $($rest)*);
    };

    // Next value is `false`.
    (@object $object:ident ($($key:tt)+) (: false $($rest:tt)*) $copy:tt) => {
        ijson_internal!(@object $object [$($key)+] (ijson_internal!(false)) $($rest)*);
    };

    // Next value is an array.
    (@object $object:ident ($($key:tt)+) (: [$($array:tt)*] $($rest:tt)*) $copy:tt) => {
        ijson_internal!(@object $object [$($key)+] (ijson_internal!([$($array)*])) $($rest)*);
    };

    // Next value is a map.
    (@object $object:ident ($($key:tt)+) (: {$($map:tt)*} $($rest:tt)*) $copy:tt) => {
        ijson_internal!(@object $object [$($key)+] (ijson_internal!({$($map)*})) $($rest)*);
    };

    // Next value is an expression followed by comma.
    (@object $object:ident ($($key:tt)+) (: $value:expr , $($rest:tt)*) $copy:tt) => {
        ijson_internal!(@object $object [$($key)+] (ijson_internal!($value)) , $($rest)*);
    };

    // Last value is an expression with no trailing comma.
    (@object $object:ident ($($key:tt)+) (: $value:expr) $copy:tt) => {
        ijson_internal!(@object $object [$($key)+] (ijson_internal!($value)));
    };

    // Missing value for last entry. Trigger a reasonable error message.
    (@object $object:ident ($($key:tt)+) (:) $copy:tt) => {
        // "unexpected end of macro invocation"
        ijson_internal!();
    };

    // Missing colon and value for last entry. Trigger a reasonable error
    // message.
    (@object $object:ident ($($key:tt)+) () $copy:tt) => {
        // "unexpected end of macro invocation"
        ijson_internal!();
    };

    // Misplaced colon. Trigger a reasonable error message.
    (@object $object:ident () (: $($rest:tt)*) ($colon:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `:`".
        ijson_unexpected!($colon);
    };

    // Found a comma inside a key. Trigger a reasonable error message.
    (@object $object:ident ($($key:tt)*) (, $($rest:tt)*) ($comma:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `,`".
        ijson_unexpected!($comma);
    };

    // Key is fully parenthesized. This avoids clippy double_parens false
    // positives because the parenthesization may be necessary here.
    (@object $object:ident () (($key:expr) : $($rest:tt)*) $copy:tt) => {
        ijson_internal!(@object $object ($key) (: $($rest)*) (: $($rest)*));
    };

    // Refuse to absorb colon token into key expression.
    (@object $object:ident ($($key:tt)*) (: $($unexpected:tt)+) $copy:tt) => {
        ijson_expect_expr_comma!($($unexpected)+);
    };

    // Munch a token into the current key.
    (@object $object:ident ($($key:tt)*) ($tt:tt $($rest:tt)*) $copy:tt) => {
        ijson_internal!(@object $object ($($key)* $tt) ($($rest)*) ($($rest)*));
    };

    //////////////////////////////////////////////////////////////////////////
    // The main implementation.
    //
    // Must be invoked as: ijson_internal!($($json)+)
    //////////////////////////////////////////////////////////////////////////

    (null) => {
        $crate::IValue::NULL
    };

    (true) => {
        $crate::IValue::TRUE
    };

    (false) => {
        $crate::IValue::FALSE
    };

    ([]) => {
        $crate::IValue::from($crate::IArray::new())
    };

    ([ $($tt:tt)+ ]) => {
        $crate::IValue::from({
            let mut array = $crate::IArray::new();
            ijson_internal!(@array array , $($tt)+);
            array
        })
    };

    ({}) => {
        $crate::IValue::from($crate::IObject::new())
    };

    ({ $($tt:tt)+ }) => {
        $crate::IValue::from({
            let mut object = $crate::IObject::new();
            ijson_internal!(@object object () ($($tt)+) ($($tt)+));
            object
        })
    };

    // Any Serialize type: numbers, strings, struct literals, variables etc.
    // Must be below every other rule.
    ($other:expr) => {
        $crate::to_value(&$other).unwrap()
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! ijson_unexpected {
    () => {};
}

#[macro_export]
#[doc(hidden)]
macro_rules! ijson_expect_expr_comma {
    ($e:expr , $($tt:tt)*) => {};
}
