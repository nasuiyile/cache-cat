/// 直接存储原始 u64。
///
/// u64::MAX 表示空，其余值表示对应的实际数值。
/// 布局与 u64 相同，占 8 字节。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OptionalU64(u64);

impl OptionalU64 {
    pub const NONE: Self = Self(u64::MAX);

    /// 从原始表示构造：MAX 表示空，其余值保持原样。
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// 构造有值状态。
    ///
    /// # Panics
    /// value == u64::MAX 时 panic，因为 MAX 被保留为空值。
    #[inline]
    pub const fn some(value: u64) -> Self {
        assert!(value != u64::MAX, "u64::MAX is reserved for None");
        Self(value)
    }

    #[inline]
    pub const fn is_none(self) -> bool {
        self.0 == u64::MAX
    }

    #[inline]
    pub const fn is_some(self) -> bool {
        self.0 != u64::MAX
    }

    /// 返回原始表示，包括用于表示空的 MAX。
    #[inline]
    pub const fn into_raw(self) -> u64 {
        self.0
    }

    /// 转换为标准库 Option，方便与其他接口交互。
    #[inline]
    pub const fn get(self) -> Option<u64> {
        if self.is_none() { None } else { Some(self.0) }
    }
}

impl Default for OptionalU64 {
    fn default() -> Self {
        Self::NONE
    }
}
