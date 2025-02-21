// No imports needed for the current implementation since we're only using:
// - Debug trait (automatically in scope from prelude)
// - Basic slice operations (automatically in scope from prelude)

/// Safe wrapper for MDB_val
#[derive(Debug)]
pub struct Value<'a> {
    size: usize,
    data: &'a [u8],
}

impl<'a> Value<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Value {
            size: data.len(),
            data,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}
