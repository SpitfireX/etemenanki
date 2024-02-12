use std::{
    error, fmt, 
    io::Result as IoResult,
    mem,
    num::TryFromIntError,
    ops::Range,
    str::{self, Utf8Error},
};

use memmap2::Mmap;
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};
use uuid::Uuid;

use crate::components::{Component, ComponentError};

#[repr(u64)]
#[derive(Debug, Clone, Copy, TryFromPrimitive, PartialEq)]
pub enum Type {
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
#[derive(Debug, Clone, Copy)]
pub struct RawHeader {
    magic: [u8; 8],
    version: [u8; 3],
    family: u8,
    class: u8,
    ctype: u8,
    allocated: u8,
    used: u8,
    uuid: [u8; 16],
    base1_uuid: [u8; 16],
    base2_uuid: [u8; 16],
    dim1: i64,
    dim2: i64,
    extensions: i64,
    comment: [u8; 72],
}

impl RawHeader {
    pub fn class(&self) -> char {
        self.class as char
    }

    pub fn dim1(&self) -> usize {
        self.dim1 as usize
    }

    pub fn dim2(&self) -> usize {
        self.dim2 as usize
    }

    pub fn container_type(&self) -> Type {
        (((self.family as u64) << 16) | ((self.class as u64) << 8) | self.ctype as u64)
            .try_into().unwrap()
    }

    pub fn uuid(&self) -> Uuid {
        Uuid::from_bytes(self.uuid)
    }

    pub fn base1(&self) -> Option<Uuid> {
        let uuid = Uuid::from_bytes(self.base1_uuid);
        (!uuid.is_nil()).then_some(uuid)
    }

    pub fn base2(&self) -> Option<Uuid> {
        let uuid = Uuid::from_bytes(self.base2_uuid);
        (!uuid.is_nil()).then_some(uuid)
    }

    pub fn comment(&self) -> Option<&str> {
        std::str::from_utf8(&self.comment).ok()
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
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

impl RawBomEntry {
    fn name(&self) -> Option<&str> {
        str::from_utf8(&self.name).ok()
            .map(|s| s.trim_end_matches("\0"))
    }
}

#[derive(Debug)]
pub struct Container<'map> {
    name: String,
    mmap: Mmap,
    header: &'map RawHeader,
    bom: &'map [RawBomEntry]
}

impl<'map> Container<'map> {
    pub fn from_mmap(mmap: Mmap, name: String) -> Result<Self, Error> {
        let Range { start, end } = mmap.as_ref().as_ptr_range();

        // map header
        let header = unsafe {
            if start.offset(mem::size_of::<RawHeader>().try_into()?) <= end {
                (start as *const RawHeader)
                    .as_ref()
                    .ok_or(Error::Memory("null pointer"))
            } else {
                Err(Error::Memory("header out of bounds"))
            }
        }?;

        // check magic
        let magic = str::from_utf8(&header.magic)?;
        if !(magic == "Ziggurat") {
            return Err(Error::FormatError("Invalid magic string"));
        }

        // check version
        let version = str::from_utf8(&header.version)?;
        if !(version == "1.0") {
            return Err(Error::FormatError("Invalid container version"));
        }

        // map BOM and check if its in bounds
        let bom = unsafe {
            let bom_ptr = start.offset(160);
            let n = header.allocated as usize;

            if bom_ptr.offset((mem::size_of::<RawBomEntry>() * n).try_into()?) <= end {
                let first_bom = bom_ptr as *const RawBomEntry;
                Ok(std::slice::from_raw_parts(first_bom, n))
            } else {
                Err(Error::Memory("BOM out of bounds"))
            }
        }?;

        // check if all components are in bounds
        for be in bom {
            if be.family != 0x01 {
                continue;
            }

            unsafe {
                if start.offset(be.offset as isize) > end
                    && start.offset((be.offset + be.size) as isize) > end
                {
                    return Err(Error::Memory("component out of bounds"));
                }
            }
        }

        Ok(Container {
            name,
            mmap,
            header,
            bom,
        })
    }

    pub fn get_component(&self, name: &str) -> Option<Component<'map>> {
        let Range { start, end } = self.mmap.as_ref().as_ptr_range();
        let be = self.bom.iter()
            .find(| be | { be.name().is_some_and(|s| s == name) })?;

        if be.family != 0x01 {
            return None;
        }

        unsafe {
            if start.offset(be.offset as isize) <= end
                && start.offset((be.offset + be.size) as isize) <= end
            {
                let component =
                    Component::from_raw_parts(be, start.offset(be.offset as isize)).ok()?;

                Some(component)
            } else {
                None
            }
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn header(&self) -> &RawHeader {
        &self.header
    }

    pub fn into_raw_parts(self) -> (String, Mmap, &'map RawHeader, &'map [RawBomEntry]) {
        (self.name, self.mmap, self.header, self.bom)
    }

}

#[derive(Debug, Clone)]
pub enum Error {
    Memory(&'static str),
    FormatError(&'static str),
    Utf8Error(Utf8Error),
    InvalidType(u64),
    UuidError(uuid::Error),
    ComponentError(ComponentError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory(s) => write!(f, "{}", s),
            Self::FormatError(s) => write!(f, "{}", s),
            Self::Utf8Error(e) => write!(f, "{}", e),
            Self::InvalidType(t) => write!(f, "invalid container type {}", t),
            Self::UuidError(e) => write!(f, "{}", e),
            Self::ComponentError(e) => write!(f, "{}", e),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Utf8Error(e) => Some(e),
            Self::UuidError(e) => Some(e),
            Self::ComponentError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Error::Utf8Error(value)
    }
}

impl From<TryFromPrimitiveError<Type>> for Error {
    fn from(value: TryFromPrimitiveError<Type>) -> Self {
        Error::InvalidType(value.number)
    }
}

impl From<TryFromIntError> for Error {
    fn from(_value: TryFromIntError) -> Self {
        Error::Memory("out of bounds")
    }
}

impl From<uuid::Error> for Error {
    fn from(value: uuid::Error) -> Self {
        Error::UuidError(value)
    }
}

impl From<ComponentError> for Error {
    fn from(value: ComponentError) -> Self {
        Error::ComponentError(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TryFromError {
    WrongContainerType,
    MissingComponent(&'static str),
    WrongComponentType(&'static str),
    WrongComponentDimensions(&'static str),
    ConsistencyError(&'static str),
}

impl fmt::Display for TryFromError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongContainerType => {
                write!(f, "wrong container type for conversion")
            }
            Self::MissingComponent(s) => {
                write!(f, "missing component {} in source container", s)
            }
            Self::WrongComponentType(s) => {
                write!(f, "component {} has wrong type", s)
            }
            Self::WrongComponentDimensions(s) => {
                write!(f, "component {} has wrong dimensions", s)
            }
            Self::ConsistencyError(s) => {
                write!(f, "consinstency error: {} ", s)
            }
        }
    }
}

impl error::Error for TryFromError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}
