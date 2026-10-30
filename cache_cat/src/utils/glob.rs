/// Binary-safe Redis glob matching without recursive wildcard backtracking.
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
        // Redis stringmatchlen only enters its matching loop for a nonempty
        // string. Commands that treat the exact pattern "*" as unconditional
        // (including HSCAN) bypass the matcher themselves.
        if value.is_empty() {
            return self.pattern.is_empty();
        }
        let mut pattern_index = 0;
        let mut value_index = 0;
        let mut star = None;
        while value_index < value.len() {
            if self.pattern.get(pattern_index) == Some(&b'*') {
                while self.pattern.get(pattern_index) == Some(&b'*') {
                    pattern_index += 1;
                }
                if pattern_index == self.pattern.len() {
                    return true;
                }
                star = Some((pattern_index, value_index));
            }
            let (matched, next_pattern) = self.match_one(pattern_index, value[value_index]);
            if matched {
                pattern_index = next_pattern;
                value_index += 1;
            } else if let Some((after_star, retry_at)) = &mut star {
                // Only the most recent star needs backtracking: any earlier
                // star can consume the same extra prefix. This is O(M*N) in
                // the worst case, with constant space and no recursion.
                *retry_at += 1;
                value_index = *retry_at;
                pattern_index = *after_star;
            } else {
                return false;
            }
        }
        while self.pattern.get(pattern_index) == Some(&b'*') {
            pattern_index += 1;
        }
        pattern_index == self.pattern.len()
    }

    fn match_one(&self, index: usize, byte: u8) -> (bool, usize) {
        match self.pattern.get(index) {
            None => (false, index),
            Some(b'?') => (true, index + 1),
            Some(b'[') => self.match_bracket(index, byte),
            Some(b'\\') => match self.pattern.get(index + 1) {
                Some(&literal) => (byte == literal, index + 2),
                None => (byte == b'\\', index + 1),
            },
            Some(&literal) => (byte == literal, index + 1),
        }
    }

    fn match_bracket(&self, start: usize, byte: u8) -> (bool, usize) {
        let mut index = start + 1;
        let negate = self.pattern.get(index) == Some(&b'^');
        index += usize::from(negate);
        let mut matched = false;
        while index < self.pattern.len() {
            match self.pattern[index] {
                b'\\' if index + 1 < self.pattern.len() => {
                    index += 1;
                    matched |= self.pattern[index] == byte;
                }
                b']' => return (matched != negate, index + 1),
                _ if index + 2 < self.pattern.len() && self.pattern[index + 1] == b'-' => {
                    // Redis compares C char values in ranges. Match its signed
                    // byte ordering on the supported x86 Linux/Windows targets.
                    let begin = self.pattern[index] as i8;
                    let end = self.pattern[index + 2] as i8;
                    matched |= (begin.min(end)..=begin.max(end)).contains(&(byte as i8));
                    index += 2;
                }
                literal => matched |= literal == byte,
            }
            index += 1;
        }
        // An unterminated bracket still acts as a character class in Redis.
        (matched != negate, index)
    }
}

#[cfg(test)]
mod tests {
    use super::GlobMatcher;

    #[test]
    fn redis_globs_match_binary_names_and_bracket_edge_cases() {
        let cases: &[(&[u8], &[u8], bool)] = &[
            (b"", b"", true),
            (b"*", b"", false),
            (b"**", b"", false),
            (b"*", b"a\0\xff", true),
            (b"a?\xff", b"a\0\xff", true),
            (b"a*?c", b"abc", true),
            (b"a*?c", b"ac", false),
            (b"a*b*c", b"axybzzc", true),
            (b"a*b*c", b"axybzz", false),
            (b"[a-c]", b"b", true),
            (b"[c-a]", b"b", true),
            (b"[^a]", b"b", true),
            (b"[^a]", b"a", false),
            (b"[!a]", b"!", true),
            (b"[!a]", b"a", true),
            (b"[!a]", b"b", false),
            (b"[\\]]", b"]", true),
            (b"[\\-]", b"-", true),
            (b"[]", b"]", false),
            (b"[^]", b"]", true),
            (b"[ab", b"b", true),
            (b"[", b"[", false),
            (b"[^", b"x", true),
            (b"\\", b"\\", true),
            (b"x\\", b"x\\", true),
            (b"\\*", b"*", true),
            (b"[\xff-\x01]", b"\0", true),
            (b"[\xff-\x01]", b"\x7f", false),
            (b"a*", b"a", true),
            (b"*a", b"ba", true),
            (b"*a", b"b", false),
        ];
        for &(pattern, value, expected) in cases {
            assert_eq!(
                GlobMatcher::new(pattern).matches(value),
                expected,
                "pattern={pattern:?}, value={value:?}"
            );
        }
    }

    #[test]
    fn long_patterns_and_values_do_not_recurse() {
        let value = vec![b'a'; 100_000];
        assert!(GlobMatcher::new(b"*a").matches(&value));
        assert!(!GlobMatcher::new(b"*a*b").matches(&value));
        let mut pattern = b"*a".repeat(1_500);
        pattern.push(b'b');
        assert!(!GlobMatcher::new(&pattern).matches(&vec![b'a'; 1_500]));
    }
}
