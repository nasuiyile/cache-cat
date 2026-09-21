#[derive(Debug, Clone)]
pub struct GlobMatcher {
    pattern: Vec<u8>,
}

impl GlobMatcher {
    pub fn new(pattern: &[u8]) -> Self {
        Self {
            pattern: pattern.to_vec(),
        }
    }

    pub fn matches(&self, value: &[u8]) -> bool {
        Self::match_inner(&self.pattern, value, 0, 0)
    }

    fn match_inner(pattern: &[u8], value: &[u8], pi: usize, vi: usize) -> bool {
        if pi == pattern.len() {
            return vi == value.len();
        }

        match pattern[pi] {
            // *
            b'*' => {
                // 匹配空
                if Self::match_inner(pattern, value, pi + 1, vi) {
                    return true;
                }

                // 吃掉一个字符
                if vi < value.len() {
                    return Self::match_inner(pattern, value, pi, vi + 1);
                }

                false
            }

            // ?
            b'?' => {
                if vi < value.len() {
                    Self::match_inner(pattern, value, pi + 1, vi + 1)
                } else {
                    false
                }
            }

            // []
            b'[' => {
                if vi >= value.len() {
                    return false;
                }

                let (matched, end) = Self::match_bracket(pattern, pi, value[vi]);

                if !matched {
                    return false;
                }

                Self::match_inner(pattern, value, end + 1, vi + 1)
            }

            // escape
            b'\\' => {
                if pi + 1 >= pattern.len() {
                    return false;
                }

                if vi < value.len() && pattern[pi + 1] == value[vi] {
                    Self::match_inner(pattern, value, pi + 2, vi + 1)
                } else {
                    false
                }
            }

            c => {
                if vi < value.len() && c == value[vi] {
                    Self::match_inner(pattern, value, pi + 1, vi + 1)
                } else {
                    false
                }
            }
        }
    }

    fn match_bracket(pattern: &[u8], start: usize, value: u8) -> (bool, usize) {
        let mut i = start + 1;

        let mut negate = false;

        if i < pattern.len() && (pattern[i] == b'^' || pattern[i] == b'!') {
            negate = true;
            i += 1;
        }

        let mut matched = false;

        while i < pattern.len() && pattern[i] != b']' {
            // range a-z
            if i + 2 < pattern.len() && pattern[i + 1] == b'-' {
                let begin = pattern[i];
                let end = pattern[i + 2];

                if begin <= value && value <= end {
                    matched = true;
                }

                i += 3;
            } else {
                if pattern[i] == value {
                    matched = true;
                }

                i += 1;
            }
        }

        if negate {
            matched = !matched;
        }

        (matched, i)
    }
}
