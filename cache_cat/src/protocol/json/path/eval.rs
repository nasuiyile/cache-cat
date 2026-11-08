use super::{Expr, Node, Op, array_node, normalize_index, select_segments};
use ijson::{IArray, IValue};
use std::{cell::RefCell, cmp::Ordering};

/// Bounded to the last pattern at each expression site. A constant regex is
/// compiled once per parsed command, not once for every matching array element.
#[derive(Debug, Default)]
pub(super) struct RegexCache(RefCell<Option<(String, bool, Option<regex::Regex>)>>);

impl RegexCache {
    fn matches(&self, text: &IValue, pattern: &IValue, full: bool) -> bool {
        let (Some(text), Some(pattern)) = (text.as_string(), pattern.as_string()) else {
            return false;
        };
        let mut cache = self.0.borrow_mut();
        if !cache
            .as_ref()
            .is_some_and(|(source, anchored, _)| source == pattern.as_str() && *anchored == full)
        {
            let source = pattern.as_str();
            let compiled = if full {
                format!("\\A(?:{source})\\z")
            } else {
                source.to_owned()
            };
            let regex = regex::RegexBuilder::new(&compiled)
                .size_limit(1024 * 1024)
                .build()
                .ok();
            *cache = Some((source.to_owned(), full, regex));
        }
        cache
            .as_ref()
            .and_then(|(_, _, regex)| regex.as_ref())
            .is_some_and(|regex| regex.is_match(text.as_str()))
    }
}

struct Values<'a> {
    nodes: Vec<Node<'a>>,
    // keys()/append() produce a result sequence, even when it contains one value.
    spread: bool,
    document: bool,
}

impl<'a> Values<'a> {
    fn none() -> Self {
        Self {
            nodes: Vec::new(),
            spread: false,
            document: false,
        }
    }
    fn value(value: IValue) -> Self {
        Self {
            nodes: vec![Node::Owned(value)],
            spread: false,
            document: false,
        }
    }
    fn one(&self) -> Option<&IValue> {
        (self.nodes.len() == 1).then(|| self.nodes[0].as_value())
    }
    fn scalar(node: Node<'a>, document: bool) -> Self {
        Self {
            nodes: vec![node],
            spread: false,
            document,
        }
    }
}

pub(super) fn evaluate<'a>(expr: &'a Expr, root: &'a IValue, current: &'a IValue) -> Vec<Node<'a>> {
    run(expr, root, current).nodes
}

pub(super) fn test(expr: &Expr, root: &IValue, current: &IValue) -> bool {
    match expr {
        Expr::Path {
            root: absolute,
            segments,
        } => !select_segments(if *absolute { root } else { current }, segments, root).is_empty(),
        Expr::Unary(b'!', child) => !test(child, root, current),
        Expr::Binary(Op::And, left, right) => {
            test(left, root, current) && test(right, root, current)
        }
        Expr::Binary(Op::Or, left, right) => {
            test(left, root, current) || test(right, root, current)
        }
        _ => run(expr, root, current)
            .one()
            .and_then(IValue::to_bool)
            .unwrap_or(false),
    }
}

fn run<'a>(expr: &'a Expr, root: &'a IValue, current: &'a IValue) -> Values<'a> {
    match expr {
        Expr::Path {
            root: absolute,
            segments,
        } => Values {
            nodes: select_segments(if *absolute { root } else { current }, segments, root),
            spread: false,
            document: true,
        },
        Expr::Literal(value) => Values {
            nodes: vec![Node::Borrowed(value)],
            spread: false,
            document: false,
        },
        Expr::Unary(b'!', _) | Expr::Binary(Op::And | Op::Or, _, _) => {
            Values::value(test(expr, root, current).into())
        }
        Expr::Unary(op, child) => {
            let values = run(child, root, current);
            let Some(value) = values.one() else {
                return Values::none();
            };
            if *op == b'+' {
                return if value.is_number() {
                    values
                } else {
                    Values::none()
                };
            }
            if value.as_number().is_some_and(|n| !n.has_decimal_point())
                && let Some(integer) = value.to_i64()
                && let Some(negated) = integer.checked_neg()
            {
                return Values::value(negated.into());
            }
            finite(value.to_f64_lossy().map(|number| -number))
        }
        Expr::Binary(op, left, right) => {
            let left = run(left, root, current);
            let right = run(right, root, current);
            if matches!(op, Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Rem) {
                let (Some(left), Some(right)) = (left.one(), right.one()) else {
                    return Values::none();
                };
                arithmetic(*op, left, right)
            } else {
                // Comparisons use existential pairing for multi-node queries.
                let matched = left.nodes.iter().any(|l| {
                    right
                        .nodes
                        .iter()
                        .any(|r| compare(*op, l.as_value(), r.as_value()))
                });
                Values::value(matched.into())
            }
        }
        Expr::Regex(left, right, cache) => {
            let left = run(left, root, current);
            let right = run(right, root, current);
            Values::value(
                left.nodes
                    .iter()
                    .any(|left| {
                        right
                            .nodes
                            .iter()
                            .any(|right| cache.matches(left.as_value(), right.as_value(), false))
                    })
                    .into(),
            )
        }
        Expr::Call(name, args, cache) => function(
            name,
            args.iter().map(|arg| run(arg, root, current)).collect(),
            cache,
        ),
        Expr::Method(receiver, name, args, cache) => {
            let mut values = Vec::with_capacity(args.len() + 1);
            values.push(run(receiver, root, current));
            values.extend(args.iter().map(|arg| run(arg, root, current)));
            function(name, values, cache)
        }
        Expr::Keys(value) => function(
            "keys",
            vec![run(value, root, current)],
            &RegexCache::default(),
        ),
    }
}

fn finite<'a>(value: Option<f64>) -> Values<'a> {
    match value.filter(|value| value.is_finite()) {
        Some(value) => Values::value(value.into()),
        None => Values::none(),
    }
}

fn arithmetic<'a>(op: Op, left: &IValue, right: &IValue) -> Values<'a> {
    if !matches!(op, Op::Div)
        && left.as_number().is_some_and(|n| !n.has_decimal_point())
        && right.as_number().is_some_and(|n| !n.has_decimal_point())
        && let (Some(left), Some(right)) = (left.to_i64(), right.to_i64())
    {
        let result = match op {
            Op::Add => left.checked_add(right),
            Op::Sub => left.checked_sub(right),
            Op::Mul => left.checked_mul(right),
            Op::Rem => left.checked_rem(right),
            _ => None,
        };
        if let Some(result) = result {
            return Values::value(result.into());
        }
    }
    let (Some(left), Some(right)) = (left.to_f64_lossy(), right.to_f64_lossy()) else {
        return Values::none();
    };
    finite(Some(match op {
        Op::Add => left + right,
        Op::Sub => left - right,
        Op::Mul => left * right,
        Op::Div => left / right,
        Op::Rem => left % right,
        _ => unreachable!(),
    }))
}

fn compare(op: Op, left: &IValue, right: &IValue) -> bool {
    match op {
        Op::Eq => equal(left, right),
        Op::Ne => !equal(left, right),
        Op::Lt | Op::Le | Op::Gt | Op::Ge => {
            let order = if let (Some(left), Some(right)) = (left.as_number(), right.as_number()) {
                Some(left.cmp(right))
            } else if let (Some(left), Some(right)) = (left.as_string(), right.as_string()) {
                Some(left.as_str().cmp(right.as_str()))
            } else {
                None
            };
            match op {
                Op::Lt => order == Some(Ordering::Less),
                Op::Le => matches!(order, Some(Ordering::Less | Ordering::Equal)),
                Op::Gt => order == Some(Ordering::Greater),
                Op::Ge => matches!(order, Some(Ordering::Greater | Ordering::Equal)),
                _ => unreachable!(),
            }
        }
        Op::In | Op::Nin => right.as_array().is_some_and(|values| {
            let found = values.iter().any(|value| equal(left, value.as_ref()));
            if matches!(op, Op::In) { found } else { !found }
        }),
        Op::Subset | Op::Any | Op::None => {
            let (Some(left), Some(right)) = (left.as_array(), right.as_array()) else {
                return false;
            };
            let contains = |value: &IValue| right.iter().any(|other| equal(value, other.as_ref()));
            match op {
                Op::Subset => left.iter().all(|value| contains(value.as_ref())),
                Op::Any => left.iter().any(|value| contains(value.as_ref())),
                Op::None => left.iter().all(|value| !contains(value.as_ref())),
                _ => unreachable!(),
            }
        }
        Op::Size => length(left).is_some_and(|len| {
            right
                .as_number()
                .is_some_and(|n| *n == ijson::INumber::from(len as u64))
        }),
        Op::Empty => length(left).is_some_and(|len| right.to_bool() == Some(len == 0)),
        _ => unreachable!(),
    }
}

fn equal(left: &IValue, right: &IValue) -> bool {
    if let (Some(left), Some(right)) = (left.as_array(), right.as_array()) {
        return left.len() == right.len()
            && left
                .iter()
                .zip(right.iter())
                .all(|(l, r)| equal(l.as_ref(), r.as_ref()));
    }
    if let (Some(left), Some(right)) = (left.as_object(), right.as_object()) {
        return left.len() == right.len()
            && left
                .iter()
                .all(|(key, value)| right.get(key).is_some_and(|right| equal(value, right)));
    }
    left == right
}

fn length(value: &IValue) -> Option<usize> {
    if let Some(value) = value.as_string() {
        Some(value.as_str().chars().count())
    } else if let Some(value) = value.as_array() {
        Some(value.len() as usize)
    } else {
        value.as_object().map(|value| value.len() as usize)
    }
}

fn elements<'a>(mut values: Values<'a>) -> Option<Vec<Node<'a>>> {
    if values.spread || values.nodes.len() > 1 {
        return Some(values.nodes);
    }
    match values.nodes.pop()? {
        Node::Borrowed(value) => {
            let array = value.as_array()?;
            Some(
                (0..array.len() as usize)
                    .map(|index| array_node(array, index).unwrap())
                    .collect(),
            )
        }
        Node::Owned(value) => {
            let array = value.as_array()?;
            Some(
                array
                    .iter()
                    .map(|value| Node::Owned(value.into_owned()))
                    .collect(),
            )
        }
    }
}

fn indexed<'a>(mut values: Values<'a>, index: i64) -> Values<'a> {
    // Selecting a document node preserves its origin. Selecting from a literal
    // or synthesized result list produces a literal, which value() must reject.
    let document = values.document && !values.spread;
    if values.spread || values.nodes.len() > 1 {
        return normalize_index(index, values.nodes.len()).map_or_else(Values::none, |index| {
            Values::scalar(values.nodes.swap_remove(index), document)
        });
    }
    match values.nodes.pop() {
        Some(Node::Borrowed(value)) => {
            let Some(array) = value.as_array() else {
                return Values::none();
            };
            normalize_index(index, array.len() as usize).map_or_else(Values::none, |index| {
                Values::scalar(array_node(array, index).expect("in bounds"), document)
            })
        }
        Some(Node::Owned(value)) => {
            let Some(array) = value.as_array() else {
                return Values::none();
            };
            normalize_index(index, array.len() as usize).map_or_else(Values::none, |index| {
                Values::scalar(
                    Node::Owned(array_node(array, index).expect("in bounds").into_owned()),
                    document,
                )
            })
        }
        None => Values::none(),
    }
}

fn visit_numbers(values: &Values<'_>, mut output: impl FnMut(f64)) -> bool {
    if values.spread || values.nodes.len() > 1 {
        for node in &values.nodes {
            let Some(number) = node.as_value().to_f64_lossy() else {
                return false;
            };
            output(number);
        }
        return !values.nodes.is_empty();
    }
    let Some(array) = values.one().and_then(IValue::as_array) else {
        return false;
    };
    for node in array.iter() {
        let Some(number) = node.as_ref().to_f64_lossy() else {
            return false;
        };
        output(number);
    }
    !array.is_empty()
}

fn function<'a>(name: &str, mut args: Vec<Values<'a>>, cache: &RegexCache) -> Values<'a> {
    let arity = match name {
        "concat" => !args.is_empty(),
        "append" | "index" => args.len() == 2,
        "match" | "search" => true,
        _ => args.len() == 1,
    };
    if !arity {
        return Values::none();
    }
    match name {
        "count" => Values::value((args[0].nodes.len() as u64).into()),
        "length" => {
            let arg = &args[0];
            let len = if arg.spread {
                Some(arg.nodes.len())
            } else {
                arg.one().and_then(length)
            };
            len.map_or_else(Values::none, |len| Values::value((len as u64).into()))
        }
        "value" => {
            let mut arg = args.pop().unwrap();
            if arg.nodes.len() == 1 && (arg.document || arg.spread) {
                // Extracting a synthesized singleton yields a literal; calling
                // value() on that literal a second time returns Nothing.
                Values::scalar(arg.nodes.pop().unwrap(), arg.document && !arg.spread)
            } else {
                Values::none()
            }
        }
        "ceiling" | "floor" | "abs" => {
            let Some(value) = args[0].one() else {
                return Values::none();
            };
            if name == "abs"
                && value.as_number().is_some_and(|n| !n.has_decimal_point())
                && let Some(value) = value.to_i64()
                && let Some(value) = value.checked_abs()
            {
                return Values::value(value.into());
            }
            finite(value.to_f64_lossy().map(|value| match name {
                "ceiling" => value.ceil(),
                "floor" => value.floor(),
                _ => value.abs(),
            }))
        }
        "concat" => {
            let mut string = String::new();
            for arg in &args {
                let Some(value) = arg.one().and_then(IValue::as_string) else {
                    return Values::none();
                };
                string.push_str(value.as_str());
            }
            Values::value(string.as_str().into())
        }
        "keys" => {
            let mut result = Vec::new();
            for value in &args[0].nodes {
                if let Some(object) = value.as_value().as_object() {
                    result.extend(object.keys().map(|key| Node::Owned(key.clone().into())));
                }
            }
            Values {
                nodes: result,
                spread: true,
                document: false,
            }
        }
        "append" => {
            let argument = args.pop().unwrap();
            if argument.nodes.is_empty() {
                return Values::none();
            }
            let mut receiver = args.pop().unwrap();
            let mut nodes = if receiver.nodes.len() == 1 && receiver.nodes[0].as_value().is_array()
            {
                elements(receiver).unwrap()
            } else {
                std::mem::take(&mut receiver.nodes)
            };
            let argument = if argument.nodes.len() == 1 {
                argument.nodes.into_iter().next().unwrap()
            } else {
                let mut array = IArray::new();
                for node in argument.nodes {
                    if array.push(node.into_owned()).is_err() {
                        return Values::none();
                    }
                }
                Node::Owned(array.into())
            };
            nodes.push(argument);
            Values {
                nodes,
                spread: true,
                document: false,
            }
        }
        "first" | "last" | "index" => {
            let index = match name {
                "first" => 0,
                "last" => -1,
                _ => {
                    let index = args.pop().unwrap();
                    let Some(index) = index.one().and_then(IValue::to_f64_lossy).filter(|value| {
                        value.is_finite()
                            && *value >= i64::MIN as f64
                            && *value < -(i64::MIN as f64)
                    }) else {
                        return Values::none();
                    };
                    index.trunc() as i64
                }
            };
            indexed(args.pop().unwrap(), index)
        }
        "sum" | "min" | "max" | "avg" | "stddev" => {
            let values = args.pop().unwrap();
            let mut count = 0u64;
            let mut sum = 0.0f64;
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut min = f64::INFINITY;
            let mut max = f64::NEG_INFINITY;
            let valid = visit_numbers(&values, |value| {
                count += 1;
                sum += value;
                min = min.min(value);
                max = max.max(value);
                let delta = value - mean;
                mean += delta / count as f64;
                m2 += delta * (value - mean);
            });
            if !valid || count == 0 {
                return Values::none();
            }
            finite(Some(match name {
                "sum" => sum,
                "min" => min,
                "max" => max,
                "avg" => sum / count as f64,
                _ => (m2 / count as f64).sqrt(),
            }))
        }
        "match" | "search" => Values::value(
            match (
                args.first().and_then(Values::one),
                args.get(1).and_then(Values::one),
            ) {
                (Some(text), Some(pattern)) => cache.matches(text, pattern, name == "match"),
                _ => false,
            }
            .into(),
        ),
        _ => Values::none(),
    }
}
