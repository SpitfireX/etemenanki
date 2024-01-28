use std::{num::NonZeroUsize, ops};

use lru::LruCache;
use streaming_iterator::StreamingIterator;

/// Decodes a compressed block and returns it as a contiguous Vec of dimension n*d in row major order.
fn decode_compressed_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
    let mut block = vec![0i64; d * 16];
    let mut offset = 0;

    for i in 0..d {
        for j in 0..16 {
            let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
            block[(j * d) + i] = int; // wonky because conversion from col-major to row-major
            offset += len;
        }
    }

    block
}

/// Decodes a delta compressed block and returns it as a contiguous Vec of dimension n*d in row-major order.
fn decode_delta_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
    let mut delta_block = vec![0i64; d * 16];
    let mut offset = 0;

    for i in 0..d {
        for j in 0..16 {
            let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
            let current = (j * d) + i;
            if j == 0 {
                delta_block[current] = int; // initial seed values
            } else {
                let last = ((j - 1) * d) + i;
                delta_block[current] = delta_block[last] + int;
            }
            offset += len;
        }
    }

    delta_block
}

#[derive(Debug, Clone, Copy)]
pub enum Vector<'map> {
    Uncompressed {
        length: usize,
        width: usize,
        data: &'map [i64],
    },

    Compressed {
        length: usize,
        width: usize,
        sync: &'map [i64],
        data: &'map [u8],
    },

    Delta {
        length: usize,
        width: usize,
        sync: &'map [i64],
        data: &'map [u8],
    },
}

impl<'map> Vector<'map> {
    /// Decodes a whole block and returns a vector of its columns
    fn decode_compressed_block(d: usize, raw_data: &[u8]) -> Vec<[i64; 16]> {
        let mut block = vec![[0i64; 16]; d];

        let mut offset = 0;
        for i in 0..d {
            for j in 0..16 {
                let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
                block[i][j] = int;
                offset += len;
            }
        }

        block
    }

    /// Decodes a whole delta block and returns a vector of its columns
    fn decode_delta_block(d: usize, raw_data: &[u8]) -> Vec<[i64; 16]> {
        let block_delta = Self::decode_compressed_block(d, raw_data);
        let mut block = vec![[0i64; 16]; d];

        for i in 0..d {
            block[i][0] = block_delta[i][0];

            for j in 1..16 {
                block[i][j] = block[i][j - 1] + block_delta[i][j];
            }
        }

        block
    }

    /// Gets the value with `index` < `self.len()`*`self.width()`.
    ///
    /// This always triggers a full block decode on compressed Vectors,
    /// for efficient sequential access use `VectorReader`.
    pub fn get(&self, index: usize) -> Option<i64> {
        if index < self.len() * self.width() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    /// Gets the value with `index` < `self.len()`*`self.width()`.
    /// Panics if index is out of bounds.
    ///
    /// This always triggers a full block decode on compressed Vectors,
    /// for efficient sequential access use `VectorReader`.
    pub fn get_unchecked(&self, index: usize) -> i64 {
        match *self {
            Self::Uncompressed { length: _, width: _, data } => {
                data[index]
            }

            Self::Compressed { length: _, width, sync: _, data: _ } |
            Self::Delta { length: _, width, sync: _, data: _ } => {
                let ri = index / width;
                let ci = index % width;
                self.get_row_unchecked(ri)[ci]  
            }
        }
    }

    /// Gets the column with `index` < `self.len()`.
    ///
    /// This always triggers a full block decode on compressed Vectors,
    /// for efficient sequential access use `VectorReader`.
    pub fn get_row(&self, index: usize) -> Option<VecSlice> {
        if index < self.len() {
            Some(self.get_row_unchecked(index))
        } else {
            None
        }
    }

    /// Gets the column with `index` < `self.len()`.
    /// Panics if index is out of bounds.
    ///
    /// This always triggers a full block decode on compressed Vectors,
    /// for efficient sequential access use `VectorReader`.
    pub fn get_row_unchecked(&self, index: usize) -> VecSlice {
        match *self {
                Self::Uncompressed { length: _, width, data } => {
                    let start = index * width;
                    let end = start + width;
                    VecSlice::Borrowed(&data[start..end])
                }

                Self::Compressed { length: _, width, sync, data } |
                Self::Delta { length: _, width, sync, data } => {
                    let bi = index/16;

                    let offset = sync[bi] as usize;
                    let block = match self {
                        Vector::Uncompressed { .. } => unreachable!("unreachable because of previous match block"),
                        Vector::Compressed { .. } => Self::decode_compressed_block(width, &data[offset..]),
                        Vector::Delta { .. } => Self::decode_delta_block(width, &data[offset..]),
                    };

                    let mut slice = vec![0i64; width];
                    for i in 0..width {
                        slice[i] = block[i][index % 16];
                    }

                    VecSlice::Owned(slice)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Uncompressed { length, .. } => *length,
            Self::Compressed { length, .. } => *length,
            Self::Delta { length, .. } => *length,
        }
    }

    pub fn width(&self) -> usize {
        match self {
            Self::Uncompressed { length: _, width, .. } => *width,
            Self::Compressed { length: _, width,.. } => *width,
            Self::Delta { length: _, width, .. } => *width,
        }
    }

    pub fn delta_from_parts(n: usize, d: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
        Self::Delta {
            length: n,
            width: d,
            sync,
            data,
        }
    }

    pub fn compressed_from_parts(n: usize, d: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
        Self::Compressed {
            length: n,
            width: d,
            sync,
            data,
        }
    }

    pub fn uncompressed_from_parts(n: usize, d: usize, data: &'map [i64]) -> Self {
        Self::Uncompressed {
            length: n,
            width: d,
            data,
        }
    }
}

#[derive(Debug)]
pub struct VectorReader<'map> {
    vector: Vector<'map>,
    last_block: Option<Vec<[i64; 16]>>,
    last_block_index: usize,
    last_row: usize,
    slice_buffer: Vec<i64>,
}

impl<'map> VectorReader<'map> {
    pub fn from_vector(vector: Vector<'map>) -> Self {
        Self {
            vector,
            last_block: None,
            last_block_index: 0,
            last_row: 0,
            slice_buffer: vec![0; vector.width()],
        }
    }

    pub fn get(&mut self, index: usize) -> Option<i64> {
        if index < self.len() * self.width() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&mut self, index: usize) -> i64 {
        match self.vector {
            Vector::Uncompressed { length: _, width: _, data } => {
                data[index]
            }

            Vector::Compressed { length: _, width, .. } |
            Vector::Delta { length: _, width, .. } => {
                let ri = index / width;
                let ci = index % width;
                self.get_row_unchecked(ri)[ci]
            }
        }
    }

    pub fn get_row(&mut self, index: usize) -> Option<&[i64]> {
        if index < self.len() {
            Some(self.get_row_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_row_unchecked(&mut self, index: usize) -> &[i64] {
        match self.vector {
            Vector::Uncompressed { length: _, width, data } => {
                let start = index * width;
                let end = start + width;
                &data[start..end]
            }

            Vector::Compressed { length: _, width, sync, data } |
            Vector::Delta { length: _, width, sync, data } => {
                let bi = index/16;
                if bi > sync.len() {
                    panic!("block index out of range");
                }

                if bi != self.last_block_index || self.last_block == None {
                    let offset = sync[bi] as usize;

                    self.last_block = match self.vector {
                        Vector::Uncompressed { .. } => unreachable!("unreachable because of previous match block"),
                        Vector::Compressed { .. } => Some(Vector::decode_compressed_block(width, &data[offset..])),
                        Vector::Delta { .. } => Some(Vector::decode_delta_block(width, &data[offset..])),
                    };
                    
                    self.last_block_index = bi;
                }

                if let Some(block) = self.last_block.as_ref() {
                    for i in 0..width {
                        self.slice_buffer[i] = block[i][index % 16];
                    }
                } else {
                    panic!("last_block should not be uninitialized");
                }

                &self.slice_buffer
            }
        }
    }

    pub fn len(&self) -> usize {
        self.vector.len()
    }

    pub fn width(&self) -> usize {
        self.vector.width()
    }
}

impl<'map> Iterator for VectorReader<'map> {
    type Item = Vec<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.last_row < self.len() {
            let row = self.get_row_unchecked(self.last_row).to_owned();
            self.last_row += 1;
            Some(row)
        } else {
            None
        }
    }
}

impl<'map> IntoIterator for Vector<'map> {
    type Item = Vec<i64>;
    type IntoIter = VectorReader<'map>;

    fn into_iter(self) -> Self::IntoIter {
        VectorReader::from_vector(self)
    }
}

#[derive(Debug)]
pub enum VecSlice<'map> {
    Borrowed(&'map [i64]),
    Owned(Vec<i64>),
}

impl<'map> ops::Deref for VecSlice<'map> {
    type Target = [i64];

    fn deref(&self) -> &Self::Target {
        match self {
            VecSlice::Borrowed(s) => *s,
            VecSlice::Owned(v) => v,
        }
    }
}

impl<'map> ToOwned for VecSlice<'map> {
    type Owned = VecSlice<'map>;

    fn to_owned(&self) -> <VecSlice<'map> as ToOwned>::Owned {
        match self {
            VecSlice::Borrowed(s) => VecSlice::Owned((*s).to_owned()),
            VecSlice::Owned(v) => VecSlice::Owned(v.clone()),
        }
    }
}

pub struct CachedVector<'map> {
    inner: Vector<'map>,
    cache: LruCache<usize, Vec<i64>>,
}

impl<'map> CachedVector<'map> {
    /// Decodes a compressed block and returns it as a contiguous Vec of dimension n*d in row major order.
    fn decode_compressed_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
        let mut block = vec![0i64; d * 16];
        let mut offset = 0;

        for i in 0..d {
            for j in 0..16 {
                let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
                block[(j * d) + i] = int; // wonky because conversion from col-major to row-major
                offset += len;
            }
        }

        block
    }

    /// Decodes a delta compressed block and returns it as a contiguous Vec of dimension n*d in row-major order.
    fn decode_delta_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
        let mut delta_block = vec![0i64; d * 16];
        let mut offset = 0;

        for i in 0..d {
            for j in 0..16 {
                let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
                let current = (j * d) + i;
                if j == 0 {
                    delta_block[current] = int; // initial seed values
                } else {
                    let last = ((j - 1) * d) + i;
                    delta_block[current] = delta_block[last] + int;
                }
                offset += len;
            }
        }

        delta_block
    }

    fn row_index_to_block_offsets(&self, index: usize) -> (usize, usize, usize) {
        let bi = index / 16;
        let start = (index % 16) * self.width();
        let end = start + self.width();
        (bi, start, end)
    }

    pub fn new(vector: Vector<'map>) -> Self {
        Self {
            inner: vector,
            cache: LruCache::new(NonZeroUsize::new(250).unwrap()),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn width(&self) -> usize {
        self.inner.width()
    }

    pub fn get_row(&mut self, index: usize) -> Option<&[i64]> {
        if index < self.len() {
            Some(self.get_row_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_row_unchecked(&mut self, index: usize) -> &[i64] {
        match self.inner {
            Vector::Uncompressed { length: _, width, data } => {
                let start = index * width;
                let end = start + width;
                &data[start..end]
            }

            Vector::Delta { .. } | Vector::Compressed { .. } => {
                let (bi, start, end) = self.row_index_to_block_offsets(index);

                // decode and cache block if needed
                if !self.cache.contains(&bi) {
                    let block = match self.inner {
                        Vector::Uncompressed { .. } => unreachable!("unreachable because of previous match block"),
                        
                        Vector::Compressed { length: _, width, sync, data } => {
                            let offset = sync[bi] as usize;
                            Self::decode_compressed_block(width, &data[offset..])
                        }

                        Vector::Delta { length: _, width, sync, data } => {
                            let offset = sync[bi] as usize;
                            Self::decode_delta_block(width, &data[offset..])
                        }
                    };

                    self.cache.put(bi, block);
                }

                // return reference into cache
                let block = self.cache
                    .get(&bi)
                    .expect("at this point the block must be cached");
                &block[start..end]
            }
        }
    }

    // Returns Some(row) if the row is immediately available (either uncompressed or already decoded in the cache)
    // else None. This method never decodes any new blocks and doesn't modify the cache's LRU list.
    fn peek_row(&self, index: usize) -> Option<&[i64]> {
        match self.inner {
            Vector::Uncompressed { length: _, width, data } => {
                let start = index * width;
                let end = start + width;
                Some(&data[start..end])
            }

            Vector::Compressed { .. } | Vector::Delta { .. } => {
                let (bi, start, end) = self.row_index_to_block_offsets(index);
                self.cache.peek(&bi)
                    .map(|b| &b[start..end])
            }
        }
    }

    pub fn column_iter(&mut self, column: usize) -> Option<ColumnIter<'_, 'map>> {
        let len = self.len();
        if column < self.width() {
            Some(ColumnIter::new(column, 0, len, self))
        } else {
            None
        }
    }

    pub fn column_iter_from(&mut self, column: usize, start: usize) -> Option<ColumnIter<'_, 'map>> {
        let len = self.len();
        if column < self.width() {
            Some(ColumnIter::new(column, start, len, self))
        } else {
            None
        }
    }

    pub fn column_iter_range(&mut self, column: usize, start: usize, end: usize) -> Option<ColumnIter<'_, 'map>> {
        let len = self.len();
        if column < self.width() && end < len {
            Some(ColumnIter::new(column, start, end, self))
        } else {
            None
        }
    }

    pub fn iter(&mut self) -> RowIter<'_, 'map> {
        let len = self.len();
        RowIter::new(0, len, self)
    }

    pub fn iter_from(&mut self, start: usize) -> RowIter<'_, 'map> {
        let len = self.len();
        RowIter::new(start, len, self)
    }

    pub fn iter_range(&mut self, start: usize, end: usize) -> Option<RowIter<'_, 'map>> {
        let len = self.len();
        if end <= len {
            Some(RowIter::new(start, end, self))
        } else {
            None
        }
    }
}

pub struct ColumnIter<'cv, 'map> {
    cvec: &'cv mut CachedVector<'map>,
    col: usize,
    position: usize,
    end: usize,
}

impl <'cv, 'map> ColumnIter<'cv, 'map> {
    pub fn new(column: usize, start: usize, end: usize, cvec: &'cv mut CachedVector<'map>) -> Self {
        Self {
            cvec,
            col: column,
            position: start,
            end,
        }
    }
}

impl <'cv, 'map> StreamingIterator for ColumnIter<'cv, 'map> {
    type Item = i64;

    fn advance(&mut self) {
        if self.position <= self. end {
            self.cvec.get_row(self.position);
            self.position += 1;
        }
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.position <= self.end {
            let (bi, start, _) = self.cvec.row_index_to_block_offsets(self.position - 1);
            self.cvec
                .cache.peek(&bi)
                .map(|b| &b[start + self.col])
        } else {
            None
        }
    }
}

pub struct RowIter<'cv, 'map> {
    cvec: &'cv mut CachedVector<'map>,
    position: usize,
    end: usize,
}

impl <'cv, 'map> RowIter<'cv, 'map> {
    pub fn new(start: usize, end: usize, cvec: &'cv mut CachedVector<'map>) -> Self {
        Self {
            cvec,
            position: start,
            end
        }
    }
}

impl <'cv, 'map> StreamingIterator for RowIter<'cv, 'map> {
    type Item = [i64];

    fn advance(&mut self) {
        if self.position <= self.end {
            self.cvec.get_row(self.position);
            self.position += 1;
        }
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.position <= self.end {
            self.cvec.peek_row(self.position-1) // -1 because get always gets called after advace
        } else {
            None
        }
    }
}
