use std::ops;

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
        n_blocks: usize,
        sync: &'map [i64],
        data: &'map [u8],
    },

    Delta {
        length: usize,
        width: usize,
        n_blocks: usize,
        sync: &'map [i64],
        data: &'map [u8],
    },
}

impl<'map> Vector<'map> {
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
    /// This always triggers a full block decode on compressed Vectors
    /// for efficient access use `VectorReader`.
    pub fn get(&self, index: usize) -> i64 {
        match *self {
                Self::Uncompressed { length: _, width: _, data } => {
                    data[index]
                }
    
                Self::Compressed { length: _, width, n_blocks: _, sync: _, data: _ } |
                Self::Delta { length: _, width, n_blocks: _, sync: _, data: _ } => {
                    let ri = index / width;
                    let ci = index % width;
                    self.get_row(ri)[ci]
                }
            }
        }

    /// Gets the column with `index` < `self.len()`.
    ///
    /// This always triggers a full block decode on compressed Vectors
    /// for efficient access use `VectorReader`.
    pub fn get_row(&self, index: usize) -> VecSlice {
        match *self {
                Self::Uncompressed { length: _, width, data } => {
                    VecSlice::Borrowed(&data[index..index+width])
                }
    
                Self::Compressed { length: _, width, n_blocks, sync, data } |
                Self::Delta { length: _, width, n_blocks, sync, data } => {
                    let bi = index/16;
                    if bi > n_blocks {
                        panic!("block index out of range");
                    }
    
                    // offset in sync vector is from start of the component, so we need
                    // to compensate for that by subtracting the len of the sync vector
                    let offset = (sync[bi] as usize) - (n_blocks * 8);
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

    pub fn iter(&self) -> VectorReader {
        self.into_iter()
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
            n_blocks: sync.len(),
            sync,
            data,
        }
    }

    pub fn compressed_from_parts(n: usize, d: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
        Self::Compressed {
            length: n,
            width: d,
            n_blocks: sync.len(),
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

    pub fn get(&mut self, index: usize) -> i64 {
        match self.vector {
            Vector::Uncompressed { length: _, width: _, data } => {
                data[index]
            }

            Vector::Compressed { length: _, width, .. } |
            Vector::Delta { length: _, width, .. } => {
                let ri = index / width;
                let ci = index % width;
                self.get_row(ri)[ci]
            }
        }
    }

    pub fn get_row(&mut self, index: usize) -> &[i64] {
        match self.vector {
            Vector::Uncompressed { length: _, width, data } => {
                &data[index..index+width]
            }

            Vector::Compressed { length: _, width, n_blocks, sync, data } |
            Vector::Delta { length: _, width, n_blocks, sync, data } => {
                let bi = index/16;
                if bi > n_blocks {
                    panic!("block index out of range");
                }

                if bi != self.last_block_index || self.last_block == None {
                    // offset in sync vector is from start of the component, so we need
                    // to compensate for that by subtracting the len of the sync vector
                    let offset = (sync[bi] as usize) - (n_blocks * 8);

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
            let row = self.get_row(self.last_row).to_owned();
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
