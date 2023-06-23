use std::{
    collections::HashMap,
    error, fmt, mem,
    num::TryFromIntError,
    ops::Range,
    str::{self, Utf8Error},
};

use memmap2::Mmap;
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};
use uuid::Uuid;

use crate::components::{Component, ComponentError};

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

#[repr(C, packed)]
#[derive(Debug)]
pub struct RawHeader {
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
#[derive(Debug)]
pub struct RawBomEntry {
    pub family: u8,
    pub ctype: u8,
    pub mode: u8,
    pub name: [u8; 13],
    pub offset: i64,
    pub size: i64,
    pub param1: i64,
    pub param2: i64,
}

#[derive(Debug)]
pub struct Container<'a> {
    mmap: Mmap,
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
    pub fn from_mmap(mmap: Mmap) -> Result<Self, ContainerError> {
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
            mmap,
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
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
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
    fn from(_value: TryFromIntError) -> Self {
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
