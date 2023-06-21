#![feature(iter_next_chunk)]

use std::{
    error::{self, Error},
    fmt,
    str::{self, Utf8Error},
};

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
struct RawBOMEntry {
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
    // pub components: Vec<Component>,
}

impl<'a> Container<'a> {
    pub fn from_mmap(mmap: &Mmap) -> Result<Self, ContainerFormatError> {
        let header = unsafe { (mmap.as_ref().as_ptr() as *const RawHeader).as_ref() }
            .ok_or(ContainerFormatError::Mmap)?;

        let magic = str::from_utf8(&header.magic)?;
        if !(magic == "Ziggurat") {
            return Err(ContainerFormatError::FormatError("Invalid magic string"));
        }

        let version = str::from_utf8(&header.version[..3])?;
        if !(version == "1.0") {
            return Err(ContainerFormatError::FormatError(
                "Invalid container version",
            ));
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
        })
    }
}

#[derive(Debug)]
pub enum ContainerFormatError {
    Mmap,
    FormatError(&'static str),
    Utf8Error(Utf8Error),
    InvalidType(u64),
    UuidError(uuid::Error),
}

impl fmt::Display for ContainerFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContainerFormatError::Mmap => write!(f, "invalid memory mapping"),
            ContainerFormatError::FormatError(s) => write!(f, "{}", s),
            ContainerFormatError::Utf8Error(e) => write!(f, "{}", e),
            ContainerFormatError::InvalidType(t) => write!(f, "invalid container type {}", t),
            ContainerFormatError::UuidError(e) => write!(f, "{}", e),
        }
    }
}

impl error::Error for ContainerFormatError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ContainerFormatError::Utf8Error(e) => Some(e),
            ContainerFormatError::UuidError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<Utf8Error> for ContainerFormatError {
    fn from(value: Utf8Error) -> Self {
        ContainerFormatError::Utf8Error(value)
    }
}

impl From<TryFromPrimitiveError<ContainerType>> for ContainerFormatError {
    fn from(value: TryFromPrimitiveError<ContainerType>) -> Self {
        ContainerFormatError::InvalidType(value.number)
    }
}

impl From<uuid::Error> for ContainerFormatError {
    fn from(value: uuid::Error) -> Self {
        ContainerFormatError::UuidError(value)
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

#[derive(Debug)]
pub enum Component {}
