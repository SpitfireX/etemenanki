#[derive(Debug, Clone, Copy)]
pub enum Index<'map> {
    Compressed {
        length: usize,
        r: usize,
        sync: &'map [i64],
        data: &'map [u8],
    },

    Uncompressed {
        length: usize,
        pairs: &'map [i64],
    },
}

impl<'map> Index<'map> {
    pub fn compressed_from_parts(n: usize, r: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
        Self::Compressed {
            length: n,
            r,
            sync,
            data,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Index::Compressed { length, .. } => *length,
            Index::Uncompressed { length, .. } => *length,
        }
    }

    pub fn uncompressed_from_parts(n: usize, pairs: &'map [i64]) -> Self {
        Self::Uncompressed { length: n, pairs }
    }
}
