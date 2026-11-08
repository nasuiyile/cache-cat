//! JSONPath selection over the compact document, without materializing a second DOM.
//!
//! Locations are collected only for mutation. Reads borrow containers and strings;
//! packed numeric array elements are represented by temporary scalar values.

use ijson::{IArray, IValue};
use serde::{Deserialize, Serialize, Serializer};
use std::{collections::HashSet, fmt};

mod eval;
mod parser;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetMode {
    Always,
    Nx,
    Xx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetOutcome {
    pub changed: bool,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathError(pub(crate) String);

impl fmt::Display for PathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PathError {}

#[derive(Debug)]
pub enum Node<'a> {
    Borrowed(&'a IValue),
    Owned(IValue),
}

impl Node<'_> {
    pub fn as_value(&self) -> &IValue {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value,
        }
    }

    fn into_owned(self) -> IValue {
        match self {
            Self::Borrowed(value) => value.clone(),
            Self::Owned(value) => value,
        }
    }
}

impl Serialize for Node<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_value().serialize(serializer)
    }
}

#[derive(Debug)]
pub struct JsonPath {
    legacy: bool,
    expression: Expr,
}

#[derive(Debug)]
enum Segment {
    Field(String),
    Index(i64),
    Wildcard,
    Union(Vec<Segment>),
    Slice(Option<i64>, Option<i64>, u64),
    Recursive(Box<Segment>),
    Filter(Box<Expr>),
}

#[derive(Debug)]
enum Expr {
    Path { root: bool, segments: Vec<Segment> },
    Literal(IValue),
    Unary(u8, Box<Expr>),
    Binary(Op, Box<Expr>, Box<Expr>),
    Regex(Box<Expr>, Box<Expr>, eval::RegexCache),
    Call(String, Vec<Expr>, eval::RegexCache),
    Method(Box<Expr>, String, Vec<Expr>, eval::RegexCache),
    Keys(Box<Expr>),
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Regex,
    And,
    Or,
    In,
    Nin,
    Subset,
    Any,
    None,
    Size,
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Part {
    Field(String),
    Index(usize),
}

impl JsonPath {
    pub fn parse(source: &str) -> Result<Self, PathError> {
        parser::parse(source)
    }

    pub fn is_legacy_source(source: &str) -> bool {
        !parser::is_expression(source.trim())
    }

    /// Projections always return a list, including legacy-normalized expressions.
    pub fn is_legacy(&self) -> bool {
        self.legacy && !self.is_projection()
    }

    pub fn is_legacy_syntax(&self) -> bool {
        self.legacy
    }

    pub fn is_projection(&self) -> bool {
        !matches!(self.expression, Expr::Path { .. })
    }

    pub fn is_root(&self) -> bool {
        matches!(&self.expression, Expr::Path { segments, .. } if segments.is_empty())
    }

    pub fn select<'a>(&'a self, root: &'a IValue) -> Vec<Node<'a>> {
        eval::evaluate(&self.expression, root, root)
    }

    pub fn set(
        &self,
        root: &mut IValue,
        replacement: &IValue,
        mode: SetMode,
    ) -> Result<SetOutcome, PathError> {
        let Expr::Path { segments, .. } = &self.expression else {
            return Err(PathError(
                "path must identify document nodes, not a projection".into(),
            ));
        };
        if segments.is_empty() {
            if mode == SetMode::Nx {
                return Err(PathError("path must end with an object key".into()));
            }
            *root = replacement.clone();
            return Ok(SetOutcome {
                changed: true,
                complete: true,
            });
        }

        if mode == SetMode::Nx
            && !segments
                .iter()
                .all(|s| matches!(s, Segment::Field(_) | Segment::Index(_)))
        {
            return Err(PathError("wrong static path".into()));
        }

        let mut locations = Vec::new();
        walk(
            Node::Borrowed(root),
            segments,
            root,
            &mut Vec::new(),
            &mut |_, path| {
                locations.push(path.to_vec());
            },
        );
        if !locations.is_empty() {
            if mode == SetMode::Nx {
                return Ok(SetOutcome {
                    changed: false,
                    complete: false,
                });
            }
            // A matched ancestor supersedes all descendant replacements.
            let mut seen = HashSet::with_capacity(locations.len());
            locations.retain(|path| seen.insert(path.clone()));
            locations.retain(|path| !(0..path.len()).any(|len| seen.contains(&path[..len])));
            let height = subtree_depth(replacement);
            let mut changed = false;
            let mut complete = true;
            for path in &locations {
                if path.len() + height >= crate::raft::types::core::structure::json::MAX_JSON_DEPTH
                {
                    // Redis applies valid matches even when another match would
                    // exceed the depth limit. The command returns null, but its
                    // successful writes must still enter the snapshot delta.
                    complete = false;
                    continue;
                }
                let success = replace(root, path, replacement)?;
                changed |= success;
                complete &= success;
            }
            return Ok(SetOutcome { changed, complete });
        }
        if mode == SetMode::Xx {
            return Ok(SetOutcome {
                changed: false,
                complete: false,
            });
        }

        // Only a static path can create a missing final object member. Never
        // synthesize intermediate objects or append beyond an array's bounds.
        if !segments
            .iter()
            .all(|s| matches!(s, Segment::Field(_) | Segment::Index(_)))
        {
            return Err(PathError("wrong static path".into()));
        }
        let (last, prefix) = segments.split_last().expect("non-root path");
        let mut parents = Vec::new();
        walk(
            Node::Borrowed(root),
            prefix,
            root,
            &mut Vec::new(),
            &mut |_, path| {
                parents.push(path.to_vec());
            },
        );
        let Some(parent_path) = parents.first() else {
            return Ok(SetOutcome {
                changed: false,
                complete: false,
            });
        };
        if parent_path.len() + 1 + subtree_depth(replacement)
            >= crate::raft::types::core::structure::json::MAX_JSON_DEPTH
        {
            return Ok(SetOutcome {
                changed: false,
                complete: false,
            });
        }
        let Some(parent) = get_mut(root, parent_path) else {
            return Ok(SetOutcome {
                changed: false,
                complete: false,
            });
        };
        match last {
            Segment::Field(field) => {
                let Some(object) = parent.as_object_mut() else {
                    return Err(PathError("path parent is not an object".into()));
                };
                object
                    .insert(field.as_str(), replacement.clone())
                    .map_err(|e| PathError(e.to_string()))?;
                Ok(SetOutcome {
                    changed: true,
                    complete: true,
                })
            }
            Segment::Index(_) => Err(PathError(
                "array index out of range or path parent is not an array".into(),
            )),
            _ => unreachable!(),
        }
    }
}

fn subtree_depth(value: &IValue) -> usize {
    if let Some(object) = value.as_object() {
        object
            .values()
            .map(|child| 1 + subtree_depth(child))
            .max()
            .unwrap_or(0)
    } else if let Some(array) = value.as_array() {
        if let Some(values) = array.as_slice_of::<IValue>() {
            values
                .iter()
                .map(|child| 1 + subtree_depth(child))
                .max()
                .unwrap_or(0)
        } else {
            usize::from(!array.is_empty())
        }
    } else {
        0
    }
}

fn get_mut<'a>(value: &'a mut IValue, path: &[Part]) -> Option<&'a mut IValue> {
    let mut current = value;
    for part in path {
        current = match part {
            Part::Field(key) => current.as_object_mut()?.get_mut(key.as_str())?,
            Part::Index(index) => current.as_array_mut()?.get_mut(*index)?,
        };
    }
    Some(current)
}

fn replace(root: &mut IValue, path: &[Part], replacement: &IValue) -> Result<bool, PathError> {
    let Some((last, parent)) = path.split_last() else {
        *root = replacement.clone();
        return Ok(true);
    };
    let Some(parent) = get_mut(root, parent) else {
        return Ok(false);
    };
    match last {
        Part::Field(key) => {
            let Some(target) = parent.as_object_mut().and_then(|o| o.get_mut(key.as_str())) else {
                return Ok(false);
            };
            *target = replacement.clone();
            Ok(true)
        }
        Part::Index(index) => {
            let Some(array) = parent.as_array_mut() else {
                return Ok(false);
            };
            array
                .replace(*index, replacement.clone())
                .map(|old| old.is_some())
                .map_err(|e| PathError(e.to_string()))
        }
    }
}

fn array_node(array: &IArray, index: usize) -> Option<Node<'_>> {
    use ijson::array::ArraySliceRef::*;
    match array.as_slice() {
        Heterogeneous(values) => values.get(index).map(Node::Borrowed),
        I8(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        U8(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        I16(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        U16(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        I32(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        U32(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        I64(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        U64(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        F16(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        BF16(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        F32(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
        F64(values) => values.get(index).map(|v| Node::Owned(IValue::from(*v))),
    }
}

fn normalize_index(index: i64, length: usize) -> Option<usize> {
    let length = length as i64;
    let index = if index < 0 {
        length.checked_add(index)?
    } else {
        index
    };
    (index >= 0 && index < length).then_some(index as usize)
}

enum Edge<'a> {
    Field(&'a str),
    Index(usize),
}

impl Edge<'_> {
    fn owned(self) -> Part {
        match self {
            Self::Field(field) => Part::Field(field.to_owned()),
            Self::Index(index) => Part::Index(index),
        }
    }
}

fn children<'a>(value: &'a IValue, mut f: impl FnMut(Node<'a>, Edge<'a>)) {
    if let Some(object) = value.as_object() {
        // ijson iterates its dense field array, whose order is independent of
        // hash seeds. Preserve it rather than sorting ordinary JSON queries.
        for (key, child) in object.iter() {
            f(Node::Borrowed(child), Edge::Field(key.as_str()));
        }
    } else if let Some(array) = value.as_array() {
        for index in 0..array.len() as usize {
            f(
                array_node(array, index).expect("in bounds"),
                Edge::Index(index),
            );
        }
    }
}

fn walk<'a>(
    node: Node<'a>,
    segments: &[Segment],
    root: &'a IValue,
    path: &mut Vec<Part>,
    output: &mut dyn FnMut(Node<'a>, &[Part]),
) {
    let Some((first, rest)) = segments.split_first() else {
        output(node, path);
        return;
    };
    let Node::Borrowed(value) = node else { return };
    match first {
        Segment::Recursive(inner) => {
            apply_segment(value, inner, root, &mut |child, edge| {
                path.push(edge.owned());
                walk(child, rest, root, path, output);
                path.pop();
            });
            children(value, |child, part| {
                path.push(part.owned());
                walk(child, segments, root, path, output);
                path.pop();
            });
        }
        _ => apply_segment(value, first, root, &mut |child, edge| {
            path.push(edge.owned());
            walk(child, rest, root, path, output);
            path.pop();
        }),
    }
}

fn apply_segment<'a>(
    value: &'a IValue,
    segment: &Segment,
    root: &'a IValue,
    output: &mut dyn FnMut(Node<'a>, Edge<'a>),
) {
    match segment {
        Segment::Field(field) => {
            if let Some((field, child)) = value
                .as_object()
                .and_then(|o| o.get_key_value(field.as_str()))
            {
                output(Node::Borrowed(child), Edge::Field(field.as_str()));
            }
        }
        Segment::Index(index) => {
            if let Some(array) = value.as_array()
                && let Some(index) = normalize_index(*index, array.len() as usize)
            {
                output(
                    array_node(array, index).expect("in bounds"),
                    Edge::Index(index),
                );
            }
        }
        Segment::Wildcard => children(value, output),
        Segment::Union(selectors) => {
            for selector in selectors {
                apply_segment(value, selector, root, output);
            }
        }
        Segment::Slice(start, end, step) => {
            if let Some(array) = value.as_array() {
                let length = i64::from(array.len());
                let bound = |index: i64| {
                    if index < 0 {
                        length.saturating_add(index).max(0)
                    } else {
                        index.min(length)
                    }
                };
                let mut index = start.map(bound).unwrap_or(0);
                let end = end.map(bound).unwrap_or(length);
                while index < end {
                    output(
                        array_node(array, index as usize).expect("in bounds"),
                        Edge::Index(index as usize),
                    );
                    let Some(next) = index.checked_add((*step).min(i64::MAX as u64) as i64) else {
                        break;
                    };
                    index = next;
                }
            }
        }
        Segment::Filter(expression) => children(value, |child, part| {
            if eval::test(expression, root, child.as_value()) {
                output(child, part);
            }
        }),
        Segment::Recursive(_) => unreachable!("recursive selectors handled by walk"),
    }
}

fn select_segments<'a>(value: &'a IValue, segments: &[Segment], root: &'a IValue) -> Vec<Node<'a>> {
    let mut result = Vec::new();
    walk_read(Node::Borrowed(value), segments, root, &mut |node| {
        result.push(node)
    });
    result
}

fn walk_read<'a>(
    node: Node<'a>,
    segments: &[Segment],
    root: &'a IValue,
    output: &mut dyn FnMut(Node<'a>),
) {
    let Some((first, rest)) = segments.split_first() else {
        output(node);
        return;
    };
    let Node::Borrowed(value) = node else { return };
    if let Segment::Recursive(inner) = first {
        apply_segment(value, inner, root, &mut |child, _| {
            walk_read(child, rest, root, output)
        });
        children(value, |child, _| walk_read(child, segments, root, output));
    } else {
        apply_segment(value, first, root, &mut |child, _| {
            walk_read(child, rest, root, output)
        });
    }
}

#[cfg(test)]
mod tests;
