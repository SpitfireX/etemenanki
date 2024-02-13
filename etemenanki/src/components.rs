pub mod index;
pub mod inverted_index;
pub mod set;
pub mod string_vector;
pub mod vector;

pub use index::*;
pub use inverted_index::*;
pub use set::*;
pub use string_vector::*;
pub use vector::*;

use std::{error, fmt};

use enum_as_inner::EnumAsInner;
use num_enum::{IntoPrimitive, TryFromPrimitive, TryFromPrimitiveError};

use crate::container::BomEntry;

#[repr(u16)]
#[derive(Debug, IntoPrimitive, TryFromPrimitive)]
pub enum ComponentType {
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

#[derive(Debug, Clone, Copy, EnumAsInner)]
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
    pub fn from_raw_parts(be: &BomEntry, start_ptr: *const u8) -> Result<Self, ComponentError> {
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
                if d == 0 {
                    return Err(ComponentError::InvalidDimension("d must be > 0"));
                }
                let data_ptr = start_ptr as *const i64;
                let data = unsafe { std::slice::from_raw_parts(data_ptr, n * d) };
                Component::Vector(Vector::uncompressed_from_parts(n, d, data))
            }

            ComponentType::VectorComp => {
                let n = be.param1 as usize;
                let d = be.param2 as usize;
                let m = ((n - 1) / 16) + 1;

                if d == 0 {
                    return Err(ComponentError::InvalidDimension("d must be > 0"));
                }

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

                if d == 0 {
                    return Err(ComponentError::InvalidDimension("d must be > 0"));
                }

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
                let p = be.param2 as usize;
                let m = ((n - 1) / 16) + 1;

                if p == 0 {
                    return Err(ComponentError::InvalidDimension("p must be > 0"));
                }

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

                        Component::Set(Set::from_parts(n, p, sync, data))
                    }
                }
            }

            ComponentType::Index => {
                let n = be.param1 as usize;
                let pairs_ptr = start_ptr as *const (i64, i64);
                let pairs = unsafe { std::slice::from_raw_parts(pairs_ptr, n) };
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
                            std::slice::from_raw_parts(start_ptr.offset(8) as *const (i64, usize), mr);
                        let data_ptr = start_ptr.offset((8 + len_sync) as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len - len_sync - 8);

                        Component::Index(Index::compressed_from_parts(n, r, sync, data))
                    }
                }
            }

            ComponentType::InvertedIndex => {
                let k = be.param1 as usize;

                // check if typeinfo array is in bounds
                let len = be.size as usize;
                let len_typeinfo = k * 8 * 2;
                if len_typeinfo > len {
                    Err(ComponentError::OutOfBounds("typeinfo in InvertedIndex"))?
                } else {
                    unsafe {
                        let typeinfo = std::slice::from_raw_parts(start_ptr as *const (i64, i64), k);
                        let data_ptr = start_ptr.offset((len_typeinfo) as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len - len_typeinfo);

                        Component::InvertedIndex(InvertedIndex::from_parts(k, typeinfo, data))
                    }
                }
            }
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ComponentError {
    InvalidType(u16),
    NullPtr,
    OutOfBounds(&'static str),
    InvalidDimension(&'static str),
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
            Self::InvalidDimension(s) => write!(f, "component has invalid dimension: {}", s),
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

#[derive(Debug, Clone, Copy)]
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
        self.data
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StringList<'map> {
    length: usize,
    data: &'map [u8],
}

impl<'map> StringList<'map> {
    pub fn from_parts(n: usize, data: &'map [u8]) -> Self {
        Self { length: n, data }
    }

    pub fn data(&self) -> &'map [u8] {
        self.data
    }

    pub fn len(&self) -> usize {
        self.length
    }
}
