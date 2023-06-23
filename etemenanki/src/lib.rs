use std::{
    collections::HashMap,
    error::{self, Error},
    fmt, mem,
    num::TryFromIntError,
    ops::Range,
    str::{self, Utf8Error},
};

use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};
use uuid::Uuid;

#[cfg(test)]
mod tests;

#[repr(C, packed)]
struct RawHeader {
    magic: [u8; 8],
    version: [u8; 4],
    family: u8,
    class: u8,
    ctype: u8,
    lf: u8,
    uuid: [u8; 36],
    lfeot: [u8; 4],
    allocated: u8,
    used: u8,
    padding: [u8; 6],
    dim1: i64,
    dim2: i64,
    base1_uuid: [u8; 36],
    padding1: [u8; 4],
    base2_uuid: [u8; 36],
}

#[repr(C, packed)]
struct RawBomEntry {
    family: u8,
    ctype: u8,
    mode: u8,
    name: [u8; 13],
    offset: i64,
    size: i64,
    param1: i64,
    param2: i64,
}

#[derive(Debug)]
pub struct Container<'a> {
    pub version: &'a str,
    pub raw_family: char,
    pub raw_class: char,
    pub raw_type: char,
    pub container_type: ContainerType,
    pub uuid: Uuid,
    pub allocated_components: u8,
    pub used_components: u8,
    pub dim1: usize,
    pub dim2: usize,
    pub base1_uuid: Option<Uuid>,
    pub base2_uuid: Option<Uuid>,
    pub components: HashMap<&'a str, Component<'a>>,
}

impl<'a> Container<'a> {
    pub fn from_mmap(mmap: &Mmap) -> Result<Self, ContainerError> {
        let Range { start, end } = mmap.as_ref().as_ptr_range();

        let header = unsafe {
            if start.offset(mem::size_of::<RawHeader>().try_into()?) <= end {
                (start as *const RawHeader)
                    .as_ref()
                    .ok_or(ContainerError::Memory("null pointer"))
            } else {
                Err(ContainerError::Memory("header out of bounds"))
            }
        }?;

        let magic = str::from_utf8(&header.magic)?;
        if !(magic == "Ziggurat") {
            return Err(ContainerError::FormatError("Invalid magic string"));
        }

        let version = str::from_utf8(&header.version[..3])?;
        if !(version == "1.0") {
            return Err(ContainerError::FormatError("Invalid container version"));
        }

        let raw_family = header.family as char;
        let raw_class = header.class as char;
        let raw_type = header.ctype as char;

        let container_type: ContainerType =
            (((header.family as u64) << 16) | ((header.class as u64) << 8) | header.ctype as u64)
                .try_into()?;

        let uuid: Uuid = str::from_utf8(&header.uuid)?.parse()?;

        let base1_uuid: Option<Uuid> = {
            let s = str::from_utf8(&header.base1_uuid)?;
            if s.contains("\0") {
                None
            } else {
                Some(s.parse()?)
            }
        };

        let base2_uuid: Option<Uuid> = {
            let s = str::from_utf8(&header.base2_uuid)?;
            if s.contains("\0") {
                None
            } else {
                Some(s.parse()?)
            }
        };

        let bom = unsafe {
            let bom_ptr = start.offset(160);
            let n = header.allocated as usize;

            if bom_ptr.offset((mem::size_of::<RawBomEntry>() * n).try_into()?) <= end {
                let first_bom = bom_ptr as *const RawBomEntry;
                Ok(std::slice::from_raw_parts(first_bom, n))
            } else {
                Err(ContainerError::Memory("BOM out of bounds"))
            }
        }?;

        let mut components = HashMap::new();

        for be in bom {
            if be.family != 0x01 {
                continue;
            }

            unsafe {
                if start.offset(be.offset as isize) <= end
                    && start.offset((be.offset + be.size) as isize) <= end
                {
                    let name = str::from_utf8(&be.name)?.trim_end_matches("\0");
                    let component =
                        Component::from_raw_parts(be, start.offset(be.offset as isize))?;

                    components.insert(name, component);
                } else {
                    return Err(ContainerError::Memory("component out of bounds"));
                }
            }
        }

        Ok(Container {
            version,
            raw_family,
            raw_class,
            raw_type,
            container_type,
            uuid,
            allocated_components: header.allocated,
            used_components: header.used,
            dim1: header.dim1 as usize,
            dim2: header.dim2 as usize,
            base1_uuid,
            base2_uuid,
            components,
        })
    }
}

#[derive(Debug)]
pub enum ContainerError {
    Memory(&'static str),
    FormatError(&'static str),
    Utf8Error(Utf8Error),
    InvalidType(u64),
    UuidError(uuid::Error),
    ComponentError(ComponentError),
}

impl fmt::Display for ContainerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContainerError::Memory(s) => write!(f, "{}", s),
            ContainerError::FormatError(s) => write!(f, "{}", s),
            ContainerError::Utf8Error(e) => write!(f, "{}", e),
            ContainerError::InvalidType(t) => write!(f, "invalid container type {}", t),
            ContainerError::UuidError(e) => write!(f, "{}", e),
            ContainerError::ComponentError(e) => write!(f, "{}", e),
        }
    }
}

impl error::Error for ContainerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ContainerError::Utf8Error(e) => Some(e),
            ContainerError::UuidError(e) => Some(e),
            ContainerError::ComponentError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<Utf8Error> for ContainerError {
    fn from(value: Utf8Error) -> Self {
        ContainerError::Utf8Error(value)
    }
}

impl From<TryFromPrimitiveError<ContainerType>> for ContainerError {
    fn from(value: TryFromPrimitiveError<ContainerType>) -> Self {
        ContainerError::InvalidType(value.number)
    }
}

impl From<TryFromIntError> for ContainerError {
    fn from(value: TryFromIntError) -> Self {
        ContainerError::Memory("out of bounds")
    }
}

impl From<uuid::Error> for ContainerError {
    fn from(value: uuid::Error) -> Self {
        ContainerError::UuidError(value)
    }
}

impl From<ComponentError> for ContainerError {
    fn from(value: ComponentError) -> Self {
        ContainerError::ComponentError(value)
    }
}

#[repr(u64)]
#[derive(Debug, TryFromPrimitive)]
pub enum ContainerType {
    GraphLayer = 0x5a4c67,              // "ZLg"
    PrimaryLayer = 0x5a4c70,            // "ZLp"
    SegmentationLayer = 0x5a4c73,       // "ZLs"
    TreeLayer = 0x5a4c74,               // "ZLt"
    PlainStringVariable = 0x5a5663,     // "ZVc"
    HashVariable = 0x5a5668,            // "ZVh"
    IntegerVariable = 0x5a5669,         // "ZVi"
    PointerVariable = 0x5a5670,         // "ZVp"
    ExternalPointerVariable = 0x5a5671, // "ZVq"
    SetVariable = 0x5a5673,             // "ZVs"
    IndexedStringVariable = 0x5a5678,   // "ZVx"
}

#[repr(u16)]
#[derive(Debug, TryFromPrimitive)]
enum ComponentType {
    Blob = 0x0100,
    StringList = 0x0200,
    StringVector = 0x0300,
    Vector = 0x0400,
    VectorComp = 0x0401,
    VectorDelta = 0x0402,
    Set = 0x0500,
    Index = 0x0600,
    IndexComp = 0x0601,
    InvertedIndex = 0x0701,
}

#[derive(Debug, EnumAsInner)]
pub enum Component<'a> {
    Blob(Blob),
    StringList(StringList<'a>),
    StringVector(StringVector),
    Vector(Vector<'a>),
    VectorComp(VectorComp<'a>),
    VectorDelta(VectorDelta<'a>),
    Set(Set),
    Index(Index),
    IndexComp(IndexComp<'a>),
    InvertedIndex(InvertedIndex),
}

impl<'a> Component<'a> {
    fn from_raw_parts(be: &RawBomEntry, start_ptr: *const u8) -> Result<Self, ComponentError> {
        let component_type: ComponentType =
            (((be.ctype as u16) << 8) | be.mode as u16).try_into()?;

        Ok(match component_type {
            ComponentType::Blob => todo!(),

            ComponentType::StringList => {
                let data = unsafe { std::slice::from_raw_parts(start_ptr, be.size as usize ) };
                Component::StringList(StringList::from_parts(data))
            }
            
            ComponentType::StringVector => todo!(),

            ComponentType::Vector => {
                let n = be.param1 as usize;
                let d = be.param2 as usize;
                let data_ptr = start_ptr as *const i64;
                let data = unsafe { std::slice::from_raw_parts(data_ptr, n * d) };
                Component::Vector(Vector::from_parts(n, d, data))
            }

            ComponentType::VectorComp => {
                let n = be.param1 as usize;
                let d = be.param2 as usize;
                let m = ((n-1) / 16) + 1;
                
                // check if sync array is in bounds
                let len = be.size as usize;
                let len_sync = m*8;
                if len_sync > len {
                    Err(ComponentError::OutOfBounds)?
                } else {
                    unsafe {
                        let sync = std::slice::from_raw_parts(start_ptr as *const i64, m);
                        let data_ptr = start_ptr.offset(len_sync as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len-len_sync);

                        Component::VectorDelta(VectorDelta::from_parts(n, d, sync, data))
                    }
                }
            }
            
            ComponentType::VectorDelta => {
                let n = be.param1 as usize;
                let d = be.param2 as usize;
                let m = ((n-1) / 16) + 1;
                
                // check if sync array is in bounds
                let len = be.size as usize;
                let len_sync = m*8;
                if len_sync > len {
                    Err(ComponentError::OutOfBounds)?
                } else {
                    unsafe {
                        let sync = std::slice::from_raw_parts(start_ptr as *const i64, m);
                        let data_ptr = start_ptr.offset(len_sync as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len-len_sync);

                        Component::VectorDelta(VectorDelta::from_parts(n, d, sync, data))
                    }
                }
            }

            ComponentType::Set => todo!(),
            ComponentType::Index => todo!(),
            
            ComponentType::IndexComp => {
                let n = be.param1 as usize;
                let r = unsafe { *(start_ptr as *const i64) } as usize;
                let mr = ((r-1) / 16) + 1;

                // check if sync array is in bounds
                let len = be.size as usize;
                let len_sync = mr*8*2;
                if len_sync > len {
                    Err(ComponentError::OutOfBounds)?
                } else {
                    unsafe {
                        let sync = std::slice::from_raw_parts(start_ptr.offset(8) as *const i64, mr*2);
                        let data_ptr = start_ptr.offset((8 + len_sync) as isize);
                        let data = std::slice::from_raw_parts(data_ptr, len-len_sync-8);

                        Component::IndexComp(IndexComp::from_parts(n, r, sync, data))
                    }
                }
            },

            ComponentType::InvertedIndex => todo!(),
        })
    }
}

#[derive(Debug)]
pub enum ComponentError {
    InvalidType(u16),
    NullPtr,
    OutOfBounds,
}

impl From<TryFromPrimitiveError<ComponentType>> for ComponentError {
    fn from(value: TryFromPrimitiveError<ComponentType>) -> Self {
        ComponentError::InvalidType(value.number)
    }
}

impl fmt::Display for ComponentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComponentError::InvalidType(t) => write!(f, "invalid container type {}", t),
            ComponentError::NullPtr => write!(f, "given pointer is a null pointer"),
            ComponentError::OutOfBounds => write!(f, "component is out of bounds"),
        }
    }
}

impl error::Error for ComponentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Blob {}

#[derive(Debug)]
pub struct StringList<'a> {
    data: &'a [u8],
}

impl<'a> StringList<'a> {
    pub fn from_parts(data: &'a [u8]) -> Self {
        Self { data }
    }
}

#[derive(Debug)]
pub struct StringVector {}

#[derive(Debug)]
pub struct Vector<'a> {
    length: usize,
    width: usize,
    data: &'a [i64],
}

impl<'a> Vector<'a> {
    pub fn from_parts(n: usize, d: usize, data: &'a [i64]) -> Self {
        Self {
            length: n,
            width: d,
            data,
        }
    }
}

#[derive(Debug)]
pub struct VectorComp<'a> {
    length: usize,
    width: usize,
    n_blocks: usize,
    sync: &'a [i64],
    data: &'a [u8],
}

impl<'a> VectorComp<'a> {
    pub fn from_parts(n: usize, d: usize, sync: &'a [i64], data: &'a [u8]) -> Self {
        Self {
            length: n,
            width: d,
            n_blocks: sync.len(),
            sync,
            data,
        }
    }
}

#[derive(Debug)]
pub struct VectorDelta<'a> {
    length: usize,
    width: usize,
    n_blocks: usize,
    sync: &'a [i64],
    data: &'a [u8],
}

impl<'a> VectorDelta<'a> {
    pub fn from_parts(n: usize, d: usize, sync: &'a [i64], data: &'a [u8]) -> Self {
        Self {
            length: n,
            width: d,
            n_blocks: sync.len(),
            sync,
            data,
        }
    }
}

#[derive(Debug)]
pub struct Set {}

#[derive(Debug)]
pub struct Index {}

#[derive(Debug)]
pub struct IndexComp<'a> {
    length: usize,
    r: usize,
    sync: &'a [i64],
    data: &'a [u8],
}

impl<'a> IndexComp<'a> {
    pub fn from_parts(n: usize, r: usize, sync: &'a [i64], data: &'a [u8]) -> Self {
        Self {
            length: n,
            r,
            sync,
            data,
        }
    }
}

#[derive(Debug)]
pub struct InvertedIndex {}
