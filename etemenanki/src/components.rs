use std::{error, fmt, ops};

use enum_as_inner::EnumAsInner;
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};

use crate::container::RawBomEntry;

#[repr(u16)]
#[derive(Debug, TryFromPrimitive)]
enum ComponentType {
    Blob = 0x0100,
    StringList = 0x0200,
    StringVector = 0x0300,
    Vector = 0x0400,
    VectorComp = 0x0401,
    VectorDelta = 0x0402,
    Set = 0x0501,
    Index = 0x0600,
    IndexComp = 0x0601,
    InvertedIndex = 0x0701,
}

#[derive(Debug, EnumAsInner)]
pub enum Component<'map> {
    Blob(Blob<'map>),
    StringList(StringList<'map>),
    StringVector(StringVector<'map>),
    Vector(Vector<'map>),
    Set(Set<'map>),
    Index(Index<'map>),
    InvertedIndex(InvertedIndex<'map>),
}

impl<'map> Component<'map> {
    pub fn from_raw_parts(be: &RawBomEntry, start_ptr: *const u8) -> Result<Self, ComponentError> {
        let component_type: ComponentType =
            (((be.ctype as u16) << 8) | be.mode as u16).try_into()?;

        Ok(match component_type {
            ComponentType::Blob => {
                let data = unsafe { std::slice::from_raw_parts(start_ptr, be.size as usize) };
                Component::Blob(Blob::from_parts(data))
            }

            ComponentType::StringList => {
                let n = be.param1 as usize;
                let data = unsafe { std::slice::from_raw_parts(start_ptr, be.size as usize) };
                Component::StringList(StringList::from_parts(n, data))
            }

            ComponentType::StringVector => {
                let n = be.param1 as usize;

                // check if offsets array is in bounds
                let len = be.size as usize;
                let len_offsets = (n + 1) * 8;
                if len_offsets > len {
                    Err(ComponentError::OutOfBounds("offsets in StringVector"))?
                } else {
                    unsafe {
                        let offsets = std::slice::from_raw_parts(start_ptr as *const i64, n + 1);
                        let data_ptr = start_ptr.offset(len_offsets as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len - len_offsets);

                        Component::StringVector(StringVector::from_parts(n, offsets, data))
                    }
                }
            }

            ComponentType::Vector => {
                let n = be.param1 as usize;
                let d = be.param2 as usize;
                let data_ptr = start_ptr as *const i64;
                let data = unsafe { std::slice::from_raw_parts(data_ptr, n * d) };
                Component::Vector(Vector::uncompressed_from_parts(n, d, data))
            }

            ComponentType::VectorComp => {
                let n = be.param1 as usize;
                let d = be.param2 as usize;
                let m = ((n - 1) / 16) + 1;

                // check if sync array is in bounds
                let len = be.size as usize;
                let len_sync = m * 8;
                if len_sync > len {
                    Err(ComponentError::OutOfBounds("sync in VectorComp"))?
                } else {
                    unsafe {
                        let sync = std::slice::from_raw_parts(start_ptr as *const i64, m);
                        let data_ptr = start_ptr.offset(len_sync as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len - len_sync);

                        Component::Vector(Vector::compressed_from_parts(n, d, sync, data))
                    }
                }
            }

            ComponentType::VectorDelta => {
                let n = be.param1 as usize;
                let d = be.param2 as usize;
                let m = ((n - 1) / 16) + 1;

                // check if sync array is in bounds
                let len = be.size as usize;
                let len_sync = m * 8;
                if len_sync > len {
                    Err(ComponentError::OutOfBounds("sync in VectorDelta"))?
                } else {
                    unsafe {
                        let sync = std::slice::from_raw_parts(start_ptr as *const i64, m);
                        let data_ptr = start_ptr.offset(len_sync as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len - len_sync);

                        Component::Vector(Vector::delta_from_parts(n, d, sync, data))
                    }
                }
            }

            ComponentType::Set => {
                let n = be.param1 as usize;
                let m = ((n - 1) / 16) + 1;

                // check if sync array is in bounds
                let len = be.size as usize;
                let len_sync = m * 8;
                if len_sync > len {
                    Err(ComponentError::OutOfBounds("sync in Set"))?
                } else {
                    unsafe {
                        let sync = std::slice::from_raw_parts(start_ptr as *const i64, m);
                        let data_ptr = start_ptr.offset(len_sync as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len - len_sync);

                        Component::Set(Set::from_parts(n, sync, data))
                    }
                }
            }

            ComponentType::Index => {
                let n = be.param1 as usize;
                let pairs_ptr = start_ptr as *const i64;
                let pairs = unsafe { std::slice::from_raw_parts(pairs_ptr, n * 2) };
                Component::Index(Index::uncompressed_from_parts(n, pairs))
            }

            ComponentType::IndexComp => {
                let n = be.param1 as usize;
                let r = unsafe { *(start_ptr as *const i64) } as usize;
                let mr = ((r - 1) / 16) + 1;

                // check if sync array is in bounds
                let len = be.size as usize;
                let len_sync = mr * 8 * 2;
                if len_sync > len {
                    Err(ComponentError::OutOfBounds("sync in IndexComp"))?
                } else {
                    unsafe {
                        let sync =
                            std::slice::from_raw_parts(start_ptr.offset(8) as *const i64, mr * 2);
                        let data_ptr = start_ptr.offset((8 + len_sync) as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len - len_sync - 8);

                        Component::Index(Index::compressed_from_parts(n, r, sync, data))
                    }
                }
            }

            ComponentType::InvertedIndex => {
                let k = be.param1 as usize;
                let p = be.param2 as usize;

                // check if typeinfo array is in bounds
                let len = be.size as usize;
                let len_typeinfo = k * 8 * 2;
                if len_typeinfo > len {
                    Err(ComponentError::OutOfBounds("typeinfo in InvertedIndex"))?
                } else {
                    unsafe {
                        let typeinfo = std::slice::from_raw_parts(start_ptr as *const i64, k * 2);
                        let data_ptr = start_ptr.offset((len_typeinfo) as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len - len_typeinfo);

                        Component::InvertedIndex(InvertedIndex::from_parts(k, p, typeinfo, data))
                    }
                }
            }
        })
    }
}

#[derive(Debug)]
pub enum ComponentError {
    InvalidType(u16),
    NullPtr,
    OutOfBounds(&'static str),
}

impl From<TryFromPrimitiveError<ComponentType>> for ComponentError {
    fn from(value: TryFromPrimitiveError<ComponentType>) -> Self {
        ComponentError::InvalidType(value.number)
    }
}

impl fmt::Display for ComponentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidType(t) => write!(f, "invalid container type {}", t),
            Self::NullPtr => write!(f, "given pointer is a null pointer"),
            Self::OutOfBounds(s) => write!(f, "component is out of bounds: {}", s),
        }
    }
}

impl error::Error for ComponentError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Blob<'map> {
    data: &'map [u8],
}

impl<'map> Blob<'map> {
    pub fn from_parts(data: &'map [u8]) -> Self {
        Self { data }
    }
}

impl<'map> std::ops::Deref for Blob<'map> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug)]
pub struct StringList<'map> {
    length: usize,
    data: &'map [u8],
}

impl<'map> StringList<'map> {
    pub fn from_parts(n: usize, data: &'map [u8]) -> Self {
        Self { length: n, data }
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

impl<'map> ops::Deref for StringList<'map> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug)]
pub struct StringVector<'map> {
    length: usize,
    offsets: &'map [i64],
    data: &'map [u8],
}

impl<'map> StringVector<'map> {
    pub fn from_parts(n: usize, offsets: &'map [i64], data: &'map [u8]) -> Self {
        Self {
            length: n,
            offsets,
            data,
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug)]
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
        /// Decodes a whole delta block and returns a vector of its columns
        fn decode_delta_block(d: usize, raw_data: &[u8]) -> Vec<[i64; 16]> {
            let mut block_delta = vec![[0i64; 16]; d];
            let mut block = vec![[0i64; 16]; d];
    
            let mut offset = 0;
            for i in 0..d {
                for j in 0..16 {
                    let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
                    block_delta[i][j] = int;
                    offset += len;
                }
            }
    
            for i in 0..d {
                block[i][0] = block_delta[i][0];
                
                for j in 1..16 {
                    block[i][j] = block[i][j-1] + block_delta[i][j];
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
    
                Self::Compressed { length, width, n_blocks, sync, data } => todo!(),
                
                Self::Delta { length: _, width, n_blocks: _, sync: _, data } => {
                    let ci = index / width;
                    let ri = index % width;
                    self.get_slice(ci)[ri]
                }
            }
        }

        /// Gets the column with `index` < `self.len()`.
        /// 
        /// This always triggers a full block decode on compressed Vectors
        /// for efficient access use `VectorReader`.
        pub fn get_slice(&self, index: usize) -> VecSlice {
            match *self {
                Self::Uncompressed { length: _, width, data } => {
                    VecSlice::Borrowed(&data[index..index+width])
                }
    
                Self::Compressed { length, width, n_blocks, sync, data } => todo!(),
    
                Self::Delta { length: _, width, n_blocks, sync, data } => {
                    let bi = index/16;
                    if bi > n_blocks {
                        panic!("block index out of range");
                    }
    
                    // offset in sync vector is from start of the component, so we need
                    // to compensate for that by subtracting the len of the sync vector
                    let offset = (sync[bi] as usize) - (n_blocks * 8);
                    let block = Self::decode_delta_block(width, &data[offset..]);
                    
                    let mut slice = vec![0i64; width];
                    for i in 0..width {
                        slice[i] = block[i][index % 16];
                    }
    
                    VecSlice::Owned(slice)
                },
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
    slice_buffer: Vec<i64>,
}

impl<'map> VectorReader<'map> {
    pub fn from_vector(vector: Vector<'map>) -> Self {
        let last_block = None;
        let last_block_index = 0;
        let slice_buffer = vec![0; vector.width()];

        Self {
            vector,
            last_block,
            last_block_index,
            slice_buffer,
        }
    }

    pub fn get(&mut self, index: usize) -> i64 {
        match self.vector {
            Vector::Uncompressed { length: _, width: _, data } => {
                data[index]
            }

            Vector::Compressed { length, width, n_blocks, sync, data } => todo!(),
            
            Vector::Delta { length: _, width, n_blocks, sync, data } => {
                let ci = index / width;
                let ri = index % width;
                self.get_slice(ri)[ci]
            }
        }
    }

    pub fn get_slice(&mut self, index: usize) -> &[i64] {
        match self.vector {
            Vector::Uncompressed { length: _, width, data } => {
                &data[index..index+width]
            }

            Vector::Compressed { length, width, n_blocks, sync, data } => todo!(),

            Vector::Delta { length: _, width, n_blocks, sync, data } => {
                let bi = index/16;
                if bi > n_blocks {
                    panic!("block index out of range");
                }

                if bi != self.last_block_index || self.last_block == None {
                    // offset in sync vector is from start of the component, so we need
                    // to compensate for that by subtracting the len of the sync vector
                    let offset = (sync[bi] as usize) - (n_blocks * 8);
                    self.last_block = Some(Vector::decode_delta_block(width, &data[offset..]));
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
            },
        }
    }

    pub fn len(&self) -> usize {
        self.vector.len()
    }

    pub fn width(&self) -> usize {
        self.vector.width()
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

// pub struct VectorIterator<'a> {
//     vec: &'a mut VectorReader<'a>,
//     index: usize,
// }

// impl<'a> Iterator for VectorIterator<'a> {
//     type Item = &'a [i64];

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.index < self.vec.len() {
//             Some(self.vec.get_slice(self.index))
//         } else {
//             None
//         }
//     }
// }

// impl<'a> IntoIterator for &'a mut VectorReader<'a> {
//     type Item = &'a [i64];
//     type IntoIter = VectorIterator<'a>;

//     fn into_iter(self) -> Self::IntoIter {
//         VectorIterator{
//             vec: self,
//             index: 0
//         }
//     }
// }

#[derive(Debug)]
pub struct Set<'map> {
    length: usize,
    sync: &'map [i64],
    data: &'map [u8],
}

impl<'map> Set<'map> {
    pub fn from_parts(n: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
        Self {
            length: n,
            sync,
            data,
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug)]
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
    }
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

#[derive(Debug)]
pub struct InvertedIndex<'map> {
    types: usize,
    jtable_length: usize,
    typeinfo: &'map [i64],
    data: &'map [u8],
}

impl<'map> InvertedIndex<'map> {
    pub fn from_parts(k: usize, p: usize, typeinfo: &'map [i64], data: &'map [u8]) -> Self {
        Self {
            types: k,
            jtable_length: p,
            typeinfo,
            data,
        }
    }

    pub fn n_types(&self) -> usize {
        self.types
    }
}
