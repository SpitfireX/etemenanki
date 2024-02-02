use std::{cell::RefCell, cmp::min, num::NonZeroUsize, ops, rc::Rc};

use lru::LruCache;

#[derive(Debug, Clone, Copy)]
pub enum CompressionType {
    VarInt,
    Delta,
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
    /// Decodes a compressed block and returns it as a contiguous Vec of dimension n*d in row major order.
    pub fn decode_compressed_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
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
    pub fn decode_delta_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
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

    /// Returns a tuple (block_index, row_start, row_end) for a given row index.
    fn row_index_to_block_offsets(width: usize, index: usize) -> (usize, usize, usize) {
        let bi = index / 16;
        let start = (index % 16) * width;
        let end = start + width;
        (bi, start, end)
    }

    /// Gets the value with `index` < `self.len()`*`self.width()`.
    /// Use get_row instead. 
    ///
    /// This always triggers a full block decode on compressed Vectors,
    /// for efficient block cached access use `CachedVector`.
    #[deprecated]
    #[allow(deprecated)]
    pub fn get(&self, index: usize) -> Option<i64> {
        if index < self.len() * self.width() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    /// Gets the value with `index` < `self.len()`*`self.width()`.
    /// Panics if index is out of bounds.
    /// Use get_row_unchecked instead.
    ///
    /// This always triggers a full block decode on compressed Vectors,
    /// for efficient block cached access use `CachedVector`.
    #[deprecated]
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
    /// for efficient block cached access use `CachedVector`.
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
    /// for efficient block cached access use `CachedVector`.
    pub fn get_row_unchecked(&self, index: usize) -> VecSlice {
        match *self {
                Self::Uncompressed { length: _, width, data } => {
                    let start = index * width;
                    let end = start + width;
                    VecSlice::Borrowed(&data[start..end])
                }

                Self::Compressed { length: _, width, sync, data } |
                Self::Delta { length: _, width, sync, data } => {
                    let (bi, start, end) = Vector::row_index_to_block_offsets(width, index);

                    let offset = sync[bi] as usize;
                    let block = match self {
                        Vector::Uncompressed { .. } => unreachable!("unreachable because of previous match block"),
                        Vector::Compressed { .. } => Self::decode_compressed_block(width, &data[offset..]),
                        Vector::Delta { .. } => Self::decode_delta_block(width, &data[offset..]),
                    };

                    VecSlice::Owned(block[start..end].to_owned())
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

#[deprecated]
#[derive(Debug)]
pub struct VectorReader<'map> {
    vector: Vector<'map>,
    last_block: Option<Vec<i64>>,
    last_block_index: usize,
    last_row: usize,
}

#[allow(deprecated)]
impl<'map> VectorReader<'map> {
    pub fn from_vector(vector: Vector<'map>) -> Self {
        Self {
            vector,
            last_block: None,
            last_block_index: 0,
            last_row: 0,
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
                let (bi, start, end) = Vector::row_index_to_block_offsets(width, index);

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
                    &block[start..end]
                } else {
                    panic!("last_block should not be uninitialized");
                }
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

#[allow(deprecated)]
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

#[allow(deprecated)]
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

#[derive(Debug, Clone, Copy)]
pub struct VectorBlock<const D: usize> {
    rows: [[i64; D]; 16],
    length: usize,
}

impl<const D: usize> VectorBlock<D> {
    /// Decodes a compressed block into memory and turns it into row-major canonical representation
    pub fn decode_compressed(data: &[u8], length: usize) -> Self {
        let mut rows = [[0i64; D]; 16];
        let mut offset = 0;

        for i in 0..16 {
            for j in 0..D {
                let (int, len) = ziggurat_varint::decode(&data[offset..]);
                rows[i][j] = int;
                offset += len;
            }
        }

        Self {
            rows,
            length,
        }
    }

    /// Decodes a delta compressed block into memory and turns it into row-major canonical representation
    pub fn decode_delta(data: &[u8], length: usize) -> Self {
        let mut rows = [[0i64; D]; 16];
        let mut offset = 0;

        for i in 0..D {
            for j in 0..16 {
                let (int, len) = ziggurat_varint::decode(&data[offset..]);
                if j == 0 {
                    rows[j][i] = int; // initial seed values
                } else {
                    rows[j][i] = rows[j-1][i] + int;
                }
                offset += len;
            }
        }

        Self {
            rows,
            length,
        }
    }

    pub fn get_row(&self, index: usize) -> Option<[i64; D]> {
        if index < self.length {
            Some(self.get_row_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_row_unchecked(&self, index: usize) -> [i64; D] {
        self.rows[index]
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub const fn width(&self) -> usize {
        D
    } 

    // Returns a slice over all rows
    pub fn rows(&self) -> &[[i64; D]] {
        &self.rows[..self.length]
    }
}

#[derive(Debug)]
pub struct VectorBlockCache<'map, const D: usize> {
    comp_type: CompressionType,
    length: usize,
    sync: &'map [i64],
    data: &'map [u8],
    cache: LruCache<usize, VectorBlock<D>>,
}

impl<'map, const D: usize> VectorBlockCache<'map, D> {
    pub fn new_compressed(length: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
        Self {
            comp_type: CompressionType::VarInt,
            length,
            sync,
            data,
            cache: LruCache::new(NonZeroUsize::new(250).unwrap()),
        }
    }

    pub fn new_delta(length: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
        Self {
            comp_type: CompressionType::Delta,
            length,
            sync,
            data,
            cache: LruCache::new(NonZeroUsize::new(250).unwrap()),
        }
    }

    pub fn get_block(&mut self, block_index: usize) -> Option<&VectorBlock<D>> {
        let Self {comp_type, length, sync, data, cache } = self;
        if block_index < sync.len() {
            if !cache.contains(&block_index) {
                let offset = sync[block_index] as usize;
                let blen = min(*length - (block_index * 16), 16);
                let block = match comp_type {
                    CompressionType::VarInt => VectorBlock::decode_compressed(&data[offset..], blen),
                    CompressionType::Delta => VectorBlock::decode_delta(&data[offset..], blen),
                };

                cache.put(block_index, block);
            }
    
            cache.get(&block_index)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Clone)]
pub enum CachedVector<'map, const D: usize> {
    Uncompressed {
        length: usize,
        data: &'map [i64],
    },

    Compressed {
        blocks: Rc<RefCell<VectorBlockCache<'map, D>>>,
    },
}

impl<'map, const D: usize> CachedVector<'map, D> {
    pub fn new(vector: Vector<'map>) -> Option<Self> {
        if vector.width() == D {
            match vector {
                Vector::Uncompressed { length, width: _, data } => {
                    Some(Self::Uncompressed { length, data })
                }

                Vector::Compressed { length, width: _, sync, data } => {
                    Some(Self::Compressed { 
                        blocks: Rc::new(RefCell::new(VectorBlockCache::new_compressed(length, sync, data))),
                    })
                }

                Vector::Delta { length, width: _, sync, data } => {
                    Some(Self::Compressed { 
                        blocks: Rc::new(RefCell::new(VectorBlockCache::new_delta(length, sync, data))),
                    })
                }
            }
        } else {
            None
        }
    }

    pub fn column_iter(&self, column: usize) -> ColumnIterator<'map, D> {
        ColumnIterator::new(self, 0, self.len(), column).unwrap()
    }

    pub fn column_iter_from(&self, start: usize, column: usize) -> ColumnIterator<'map, D> {
        ColumnIterator::new(self, start, self.len(), column).unwrap()
    }

    pub fn column_iter_range(&self, start: usize, end: usize, column: usize) -> Option<ColumnIterator<'map, D>> {
        ColumnIterator::new(self, start, end, column)
    }

    pub fn column_iter_until(&self, end: usize, column: usize) -> Option<ColumnIterator<'map, D>> {
        ColumnIterator::new(self, 0, end, column)
    }

    pub fn get_row(&self, index: usize) -> Option<[i64; D]> {
        if index < self.len() {
            Some(self.get_row_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_row_unchecked(&self, index: usize) -> [i64; D] {
        match self {
            CachedVector::Uncompressed { length: _, data } => {
                let start = index * D;
                let end = start + D;

                data[start..end].try_into().unwrap()
            },
            CachedVector::Compressed { blocks } => {
                let mut blocks = blocks.borrow_mut();
                let bi = index / 16;
                let block = blocks.get_block(bi).unwrap();

                block.get_row_unchecked(index % 16)
            }
        }
    }

    pub fn iter(&self) -> RowIterator<'map, D> {
        RowIterator::new(self, 0, self.len()).unwrap()
    }

    pub fn iter_from(&self, start: usize) -> RowIterator<'map, D> {
        RowIterator::new(self, start, self.len()).unwrap()
    }

    pub fn iter_range(&self, start: usize, end: usize) -> Option<RowIterator<'map, D>> {
        RowIterator::new(self, start, end)
    }

    pub fn iter_until(&self, end: usize) -> Option<RowIterator<'map, D>> {
        RowIterator::new(self, 0, end)
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Uncompressed { length, .. } => *length,
            Self::Compressed { blocks } => blocks.borrow().len(),
        }
    }

    pub const fn width(&self) -> usize {
        D
    }
}

pub enum RowIterator<'map, const D: usize> {
    Uncompressed {
        data: &'map [i64],
        position: usize,
        end: usize,
    },

    Compressed {
        blocks: Rc<RefCell<VectorBlockCache<'map, D>>>,
        current: VectorBlock<D>,
        position: usize,
        end: usize,
    },
}

impl<'map, const D: usize> RowIterator<'map, D> {
    pub fn new(cvec: &CachedVector<'map, D>, start: usize, end: usize) -> Option<Self> {
        match cvec {
            CachedVector::Uncompressed { length, data } => {
                if end <= *length {
                    Some(Self::Uncompressed { data, position: start, end })
                } else {
                    None
                }
            }

            CachedVector::Compressed { blocks } => {
                if end <= blocks.borrow().len() {
                    let bi = start / 16;
                    let current = *blocks.borrow_mut().get_block(bi).unwrap();

                    Some(Self::Compressed { blocks: blocks.clone(), current, position: start, end })
                } else {
                    None
                }
            }
        }
    }
}

impl<'map, const D: usize> Iterator for RowIterator<'map, D> {
    type Item = [i64; D];

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Uncompressed { data, position, end } => {
                if position < end {
                    let start = *position * D;
                    let end = start + D;
                    *position += 1;
                    Some(data[start..end].try_into().unwrap())
                } else {
                    None
                }
            }

            Self::Compressed { blocks, current, position, end } => {
                if position < end {
                    let i = *position % 16;
                    *position += 1;
                    let value = current.get_row(i);

                    // only go through cache when the next block is needed
                    if value.is_some() && i == 15 {
                        let mut blocks = blocks.borrow_mut();
                        let bi = *position / 16;
                        *current = *blocks.get_block(bi).unwrap();
                    }

                    value
                } else {
                    None
                }
            }
        }
    }
}

pub enum ColumnIterator<'map, const D: usize> {
    Uncompressed {
        data: &'map [i64],
        position: usize,
        end: usize,
        column: usize,
    },

    Compressed {
        blocks: Rc<RefCell<VectorBlockCache<'map, D>>>,
        current: VectorBlock<D>,
        position: usize,
        end: usize,
        column: usize,
    },
}

impl<'map, const D: usize> ColumnIterator<'map, D> {
    pub fn new(cvec: &CachedVector<'map, D>, start: usize, end: usize, column: usize) -> Option<Self> {
        if column >= D{
            return None;
        }
        
        match cvec {
            CachedVector::Uncompressed { length, data } => {
                if end <= *length {
                    Some(Self::Uncompressed { data, position: start, end, column })
                } else {
                    None
                }
            }

            CachedVector::Compressed { blocks } => {
                if end <= blocks.borrow().len() {
                    let bi = start / 16;
                    let current = *blocks.borrow_mut().get_block(bi).unwrap();

                    Some(Self::Compressed { blocks: blocks.clone(), current, position: start, end, column })
                } else {
                    None
                }
            }
        }
    }
}

impl<'map, const D: usize> Iterator for ColumnIterator<'map, D> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Uncompressed { data, position, end, column} => {
                if position < end {
                    let i = (*position * D) + *column;
                    *position += 1;
                    Some(data[i])
                } else {
                    None
                }
            }

            Self::Compressed { blocks, current, position, end, column } => {
                if position < end {
                    let i = *position % 16;
                    *position += 1;
                    let value = current.get_row(i).map(|r| r[*column]);

                    // only go through cache when the next block is needed
                    if value.is_some() && i == 15 {
                        let mut blocks = blocks.borrow_mut();
                        let bi = *position / 16;
                        *current = *blocks.get_block(bi).unwrap();
                    }

                    value
                } else {
                    None
                }
            }
        }
    }
}
