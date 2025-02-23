// No imports needed for the current implementation since we're only using:
// - Debug trait (automatically in scope from prelude)
// - Basic slice operations (automatically in scope from prelude)

use std::borrow::Cow;

/// A value that can be stored in the database
#[derive(Debug, Clone)]
pub struct Value<'a> {
    /// The raw bytes of the value
    data: Cow<'a, [u8]>,
}

impl<'a> Value<'a> {
    /// Create a new value from a byte slice
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data: Cow::Borrowed(data),
        }
    }

    /// Create a new owned value
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self {
            data: Cow::Owned(data),
        }
    }

    /// Get the raw bytes of the value
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl<'a> AsRef<[u8]> for Value<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> From<&'a [u8]> for Value<'a> {
    fn from(data: &'a [u8]) -> Self {
        Self::new(data)
    }
}

impl<'a> From<Vec<u8>> for Value<'a> {
    fn from(data: Vec<u8>) -> Self {
        Self::from_vec(data)
    }
}
