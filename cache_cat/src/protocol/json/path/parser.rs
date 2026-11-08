use super::{Expr, JsonPath, Op, PathError, Segment};
use ijson::{IArray, IObject, IValue};

const MAX_NESTING: usize = 128;

pub(super) fn parse(source: &str) -> Result<JsonPath, PathError> {
    let source = source.trim();
    if source.is_empty() {
        return Err(PathError("invalid empty path".into()));
    }
    let legacy = !is_expression(source);
    let normalized;
    let input = if legacy {
        normalized = if source == "." {
            "$".to_owned()
        } else if source.starts_with('.') || source.starts_with('[') {
            format!("${source}")
        } else {
            format!("$.{source}")
        };
        normalized.as_str()
    } else {
        source
    };
    let mut parser = Parser {
        input,
        pos: 0,
        depth: 0,
    };
    let expression = parser.expression(4)?;
    parser.space();
    if parser.pos != input.len() {
        return Err(parser.error());
    }
    if !arithmetic_expression(&expression) {
        return Err(parser.error());
    }
    Ok(JsonPath { legacy, expression })
}

fn arithmetic_expression(expression: &Expr) -> bool {
    match expression {
        Expr::Unary(b'!', _) | Expr::Regex(_, _, _) => false,
        Expr::Unary(_, child) | Expr::Keys(child) => arithmetic_expression(child),
        Expr::Binary(op, left, right) => {
            matches!(op, Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Rem)
                && arithmetic_expression(left)
                && arithmetic_expression(right)
        }
        Expr::Call(_, args, _) => args.iter().all(arithmetic_expression),
        Expr::Method(receiver, _, args, _) => {
            arithmetic_expression(receiver) && args.iter().all(arithmetic_expression)
        }
        Expr::Path { .. } | Expr::Literal(_) => true,
    }
}

fn root_prefix(source: &str) -> bool {
    source == "$"
        || source.starts_with("$.")
        || source.starts_with("$[")
        || source.starts_with("$~")
        || source.starts_with("$ ")
}

pub(super) fn is_expression(source: &str) -> bool {
    if root_prefix(source) || source.starts_with('(') {
        return true;
    }
    if let Some(rest) = source.strip_prefix(['+', '-']) {
        let rest = rest.trim_start();
        if root_prefix(rest) || rest.starts_with('(') {
            return true;
        }
    }
    let end = source
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(source.len());
    end > 0
        && source.as_bytes()[0].is_ascii_alphabetic()
        && source[end..].trim_start().starts_with('(')
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
    depth: usize,
}

impl Parser<'_> {
    fn error(&self) -> PathError {
        PathError(format!("invalid JSONPath at position {}", self.pos))
    }
    fn bytes(&self) -> &[u8] {
        self.input.as_bytes()
    }
    fn peek(&self) -> Option<u8> {
        self.bytes().get(self.pos).copied()
    }
    fn space(&mut self) {
        while self.peek() == Some(b' ') {
            self.pos += 1;
        }
    }
    fn consume(&mut self, token: &str) -> bool {
        self.space();
        if self.input[self.pos..].starts_with(token) {
            self.pos += token.len();
            true
        } else {
            false
        }
    }
    fn expect(&mut self, token: &str) -> Result<(), PathError> {
        if self.consume(token) {
            Ok(())
        } else {
            Err(self.error())
        }
    }

    fn expression(&mut self, min_precedence: u8) -> Result<Expr, PathError> {
        if self.depth >= MAX_NESTING {
            return Err(PathError("JSONPath nesting limit exceeded".into()));
        }
        self.depth += 1;
        let result = self.expression_inner(min_precedence);
        self.depth -= 1;
        result
    }

    fn expression_inner(&mut self, min_precedence: u8) -> Result<Expr, PathError> {
        self.space();
        let mut lhs = match self.peek() {
            Some(op @ (b'!' | b'-' | b'+')) => {
                self.pos += 1;
                Expr::Unary(
                    op,
                    Box::new(self.expression(if op == b'!' { 3 } else { 6 })?),
                )
            }
            _ => self.primary()?,
        };
        let mut operators = 0;
        loop {
            let saved = self.pos;
            let Some((op, precedence)) = self.operator() else {
                break;
            };
            if precedence < min_precedence {
                self.pos = saved;
                break;
            }
            operators += 1;
            if operators >= MAX_NESTING {
                return Err(PathError("JSONPath expression depth limit exceeded".into()));
            }
            let rhs = self.expression(precedence + 1)?;
            lhs = if matches!(op, Op::Regex) {
                Expr::Regex(Box::new(lhs), Box::new(rhs), Default::default())
            } else {
                Expr::Binary(op, Box::new(lhs), Box::new(rhs))
            };
        }
        Ok(lhs)
    }

    fn operator(&mut self) -> Option<(Op, u8)> {
        self.space();
        for (text, op, precedence) in [
            ("||", Op::Or, 1),
            ("&&", Op::And, 2),
            ("==", Op::Eq, 3),
            ("!=", Op::Ne, 3),
            ("<=", Op::Le, 3),
            (">=", Op::Ge, 3),
            ("=~", Op::Regex, 3),
            ("<", Op::Lt, 3),
            (">", Op::Gt, 3),
            ("subsetof", Op::Subset, 3),
            ("noneof", Op::None, 3),
            ("anyof", Op::Any, 3),
            ("sizeof", Op::Size, 3),
            ("size", Op::Size, 3),
            ("empty", Op::Empty, 3),
            ("nin", Op::Nin, 3),
            ("in", Op::In, 3),
            ("+", Op::Add, 4),
            ("-", Op::Sub, 4),
            ("*", Op::Mul, 5),
            ("/", Op::Div, 5),
            ("%", Op::Rem, 5),
        ] {
            if self.input[self.pos..].starts_with(text) {
                let end = self.pos + text.len();
                if text.as_bytes()[0].is_ascii_alphabetic()
                    && self
                        .bytes()
                        .get(end)
                        .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
                {
                    continue;
                }
                self.pos = end;
                return Some((op, precedence));
            }
        }
        None
    }

    fn primary(&mut self) -> Result<Expr, PathError> {
        self.space();
        let mut expression = match self.peek() {
            Some(b'$' | b'@') => {
                let root = self.peek() == Some(b'$');
                self.pos += 1;
                Expr::Path {
                    root,
                    segments: self.path()?,
                }
            }
            Some(b'(') => {
                self.pos += 1;
                let result = self.expression(0)?;
                self.expect(")")?;
                result
            }
            Some(b'\'' | b'"' | b'[' | b'{') => Expr::Literal(self.literal()?),
            Some(b'0'..=b'9') => Expr::Literal(self.number()?),
            Some(b'a'..=b'z' | b'A'..=b'Z' | b'_') => {
                let name = self.identifier()?;
                match name.as_str() {
                    "true" => Expr::Literal(IValue::TRUE),
                    "false" => Expr::Literal(IValue::FALSE),
                    "null" => Expr::Literal(IValue::NULL),
                    _ => Expr::Call(name, self.arguments()?, Default::default()),
                }
            }
            _ => return Err(self.error()),
        };
        if self.consume("~") {
            return Ok(Expr::Keys(Box::new(expression)));
        }
        loop {
            let saved = self.pos;
            if !self.consume(".") {
                break;
            }
            let name = match self.identifier() {
                Ok(name) => name,
                Err(_) => {
                    self.pos = saved;
                    break;
                }
            };
            self.space();
            if self.peek() != Some(b'(') {
                self.pos = saved;
                break;
            }
            expression = Expr::Method(
                Box::new(expression),
                name,
                self.arguments()?,
                Default::default(),
            );
        }
        Ok(expression)
    }

    fn arguments(&mut self) -> Result<Vec<Expr>, PathError> {
        self.expect("(")?;
        let mut args = Vec::new();
        if self.consume(")") {
            return Ok(args);
        }
        loop {
            args.push(self.expression(0)?);
            if self.consume(")") {
                return Ok(args);
            }
            self.expect(",")?;
        }
    }

    fn identifier(&mut self) -> Result<String, PathError> {
        self.space();
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            self.pos += 1;
        }
        if start == self.pos {
            Err(self.error())
        } else {
            Ok(self.input[start..self.pos].to_owned())
        }
    }

    fn field(&mut self) -> Result<String, PathError> {
        let start = self.pos;
        while self.peek().is_some_and(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'-' | b'_' | b'`' | b'+' | b'#' | b'%' | b'$' | b'^' | b'/' | b':'
                )
        }) {
            self.pos += 1;
        }
        if self.pos == start {
            Err(self.error())
        } else {
            Ok(self.input[start..self.pos].to_owned())
        }
    }

    fn path(&mut self) -> Result<Vec<Segment>, PathError> {
        let mut segments = Vec::new();
        loop {
            self.space();
            let saved = self.pos;
            let segment = match self.peek() {
                Some(b'.') => {
                    self.pos += 1;
                    if self.peek() == Some(b'.') {
                        self.pos += 1;
                        let segment = self.path_segment()?;
                        Segment::Recursive(Box::new(segment))
                    } else {
                        let segment = self.path_segment()?;
                        self.space();
                        if matches!(segment, Segment::Field(_)) && self.peek() == Some(b'(') {
                            self.pos = saved;
                            break;
                        }
                        segment
                    }
                }
                Some(b'[') => self.bracket()?,
                Some(b'*') if segments.is_empty() => {
                    self.pos += 1;
                    Segment::Wildcard
                }
                Some(b) if segments.is_empty() && (b.is_ascii_alphanumeric() || b == b'_') => {
                    let field = self.field()?;
                    if matches!(
                        field.as_str(),
                        "in" | "nin"
                            | "subsetof"
                            | "anyof"
                            | "noneof"
                            | "size"
                            | "sizeof"
                            | "empty"
                    ) {
                        self.pos = saved;
                        break;
                    }
                    Segment::Field(field)
                }
                _ => break,
            };
            if segments.len() >= MAX_NESTING {
                return Err(PathError("JSONPath nesting limit exceeded".into()));
            }
            segments.push(segment);
        }
        Ok(segments)
    }

    fn path_segment(&mut self) -> Result<Segment, PathError> {
        self.space();
        match self.peek() {
            Some(b'*') => {
                self.pos += 1;
                Ok(Segment::Wildcard)
            }
            Some(b'[') => self.bracket(),
            _ => self.field().map(Segment::Field),
        }
    }

    fn bracket(&mut self) -> Result<Segment, PathError> {
        self.expect("[")?;
        if self.consume("*") {
            self.expect("]")?;
            return Ok(Segment::Wildcard);
        }
        if self.consume("?") {
            let expression = self.expression(0)?;
            self.expect("]")?;
            return Ok(Segment::Filter(Box::new(expression)));
        }
        self.space();
        if matches!(self.peek(), Some(b'\'' | b'"')) {
            let mut fields = vec![Segment::Field(self.string()?)];
            while self.consume(",") {
                fields.push(Segment::Field(self.string()?));
            }
            self.expect("]")?;
            return Ok(if fields.len() == 1 {
                fields.pop().unwrap()
            } else {
                Segment::Union(fields)
            });
        }
        let start = if self.peek() == Some(b':') {
            None
        } else {
            Some(self.integer()?)
        };
        if self.consume(":") {
            self.space();
            let end = if matches!(self.peek(), Some(b':' | b']')) {
                None
            } else {
                Some(self.integer()?)
            };
            let step = if self.consume(":") {
                if self.consume("]") {
                    return Ok(Segment::Slice(start, end, 1));
                }
                let step = self.integer()?;
                if step <= 0 {
                    return Err(self.error());
                }
                step as u64
            } else {
                1
            };
            self.expect("]")?;
            return Ok(Segment::Slice(start, end, step));
        }
        let mut indexes = vec![Segment::Index(start.ok_or_else(|| self.error())?)];
        while self.consume(",") {
            indexes.push(Segment::Index(self.integer()?));
        }
        self.expect("]")?;
        Ok(if indexes.len() == 1 {
            indexes.pop().unwrap()
        } else {
            Segment::Union(indexes)
        })
    }

    fn integer(&mut self) -> Result<i64, PathError> {
        self.space();
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        let digits = self.pos;
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == digits || (self.pos - digits > 1 && self.bytes()[digits] == b'0') {
            return Err(self.error());
        }
        self.input[start..self.pos]
            .parse()
            .map_err(|_| self.error())
    }

    fn number(&mut self) -> Result<IValue, PathError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.peek() == Some(b'.')
            && self
                .bytes()
                .get(self.pos + 1)
                .is_some_and(|b| b.is_ascii_digit())
        {
            self.pos += 1;
            while self.peek().is_some_and(|b| b.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            while self.peek().is_some_and(|b| b.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        serde_json::from_str(&self.input[start..self.pos]).map_err(|_| self.error())
    }

    fn string(&mut self) -> Result<String, PathError> {
        self.space();
        let quote = self
            .peek()
            .filter(|b| matches!(b, b'\'' | b'"'))
            .ok_or_else(|| self.error())?;
        self.pos += 1;
        let mut decoded = String::new();
        loop {
            let ch = self.input[self.pos..]
                .chars()
                .next()
                .ok_or_else(|| self.error())?;
            self.pos += ch.len_utf8();
            if ch as u32 == quote as u32 {
                return Ok(decoded);
            }
            if ch == '\\' {
                let escaped = self.input[self.pos..]
                    .chars()
                    .next()
                    .ok_or_else(|| self.error())?;
                self.pos += escaped.len_utf8();
                if escaped != '\\' && escaped as u32 != quote as u32 {
                    return Err(self.error());
                }
                decoded.push(escaped);
            } else {
                decoded.push(ch);
            }
        }
    }

    fn literal(&mut self) -> Result<IValue, PathError> {
        if self.depth >= MAX_NESTING {
            return Err(PathError("JSONPath nesting limit exceeded".into()));
        }
        self.depth += 1;
        let result = self.literal_inner();
        self.depth -= 1;
        result
    }

    fn literal_inner(&mut self) -> Result<IValue, PathError> {
        self.space();
        match self.peek() {
            Some(b'\'' | b'"') => Ok(IValue::from(self.string()?.as_str())),
            Some(b'[') => {
                self.pos += 1;
                let mut values = IArray::new();
                if self.consume("]") {
                    return Ok(values.into());
                }
                loop {
                    values
                        .push(self.literal()?)
                        .map_err(|e| PathError(e.to_string()))?;
                    if self.consume("]") {
                        return Ok(values.into());
                    }
                    self.expect(",")?;
                }
            }
            Some(b'{') => {
                self.pos += 1;
                let mut values = IObject::new();
                if self.consume("}") {
                    return Ok(values.into());
                }
                loop {
                    let field = self.string()?;
                    self.expect(":")?;
                    values
                        .insert(field.as_str(), self.literal()?)
                        .map_err(|e| PathError(e.to_string()))?;
                    if self.consume("}") {
                        return Ok(values.into());
                    }
                    self.expect(",")?;
                }
            }
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) if self.consume("true") => Ok(IValue::TRUE),
            Some(_) if self.consume("false") => Ok(IValue::FALSE),
            Some(_) if self.consume("null") => Ok(IValue::NULL),
            _ => Err(self.error()),
        }
    }
}
