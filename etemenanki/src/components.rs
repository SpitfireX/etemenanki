use std::{error, fmt};

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
pub enum Component<'a> {
    Blob(Blob<'a>),
    StringList(StringList<'a>),
    StringVector(StringVector<'a>),
    Vector(Vector<'a>),
    Set(Set<'a>),
    Index(Index<'a>),
    InvertedIndex(InvertedIndex<'a>),
}

impl<'a> Component<'a> {
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
pub struct Blob<'a> {
    data: &'a [u8],
}

impl<'a> Blob<'a> {
    pub fn from_parts(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> std::ops::Deref for Blob<'a> {
    type Target = &'a [u8];

    fn deref(&self) -> &&'a [u8] {
        &self.data
    }
}

#[derive(Debug)]
pub struct StringList<'a> {
    length: usize,
    data: &'a [u8],
}

impl<'a> StringList<'a> {
    pub fn from_parts(n: usize, data: &'a [u8]) -> Self {
        Self { length: n, data }
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug)]
pub struct StringVector<'a> {
    length: usize,
    offsets: &'a [i64],
    data: &'a [u8],
}

impl<'a> StringVector<'a> {
    pub fn from_parts(n: usize, offsets: &'a [i64], data: &'a [u8]) -> Self {
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
pub enum Vector<'a> {
    Uncompressed {
        length: usize,
        width: usize,
        data: &'a [i64],
    },

    Compressed {
        length: usize,
        width: usize,
        n_blocks: usize,
        sync: &'a [i64],
        data: &'a [u8],
    },

    Delta {
        length: usize,
        width: usize,
        n_blocks: usize,
        sync: &'a [i64],
        data: &'a [u8],
    },
}

impl<'a> Vector<'a> {
    pub fn delta_from_parts(n: usize, d: usize, sync: &'a [i64], data: &'a [u8]) -> Self {
        Self::Delta {
            length: n,
            width: d,
            n_blocks: sync.len(),
            sync,
            data,
        }
    }

    pub fn compressed_from_parts(n: usize, d: usize, sync: &'a [i64], data: &'a [u8]) -> Self {
        Self::Compressed {
            length: n,
            width: d,
            n_blocks: sync.len(),
            sync,
            data,
        }
    }

    pub fn uncompressed_from_parts(n: usize, d: usize, data: &'a [i64]) -> Self {
        Self::Uncompressed {
            length: n,
            width: d,
            data,
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
}

#[derive(Debug)]
pub struct Set<'a> {
    length: usize,
    sync: &'a [i64],
    data: &'a [u8],
}

impl<'a> Set<'a> {
    pub fn from_parts(n: usize, sync: &'a [i64], data: &'a [u8]) -> Self {
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
pub enum Index<'a> {
    Compressed {
        length: usize,
        r: usize,
        sync: &'a [i64],
        data: &'a [u8],
    },

    Uncompressed {
        length: usize,
        pairs: &'a [i64],
    }
}

impl<'a> Index<'a> {
    pub fn compressed_from_parts(n: usize, r: usize, sync: &'a [i64], data: &'a [u8]) -> Self {
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

    pub fn uncompressed_from_parts(n: usize, pairs: &'a [i64]) -> Self {
        Self::Uncompressed { length: n, pairs }
    }
}

#[derive(Debug)]
pub struct InvertedIndex<'a> {
    types: usize,
    jtable_length: usize,
    typeinfo: &'a [i64],
    data: &'a [u8],
}

impl<'a> InvertedIndex<'a> {
    pub fn from_parts(k: usize, p: usize, typeinfo: &'a [i64], data: &'a [u8]) -> Self {
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
