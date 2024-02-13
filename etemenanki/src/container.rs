use std::{
    error, fmt, fs::File, io::{Seek, SeekFrom}, mem, num::TryFromIntError, ops::Range, str::{self, Utf8Error}
};

use memmap2::{Mmap, MmapMut, MmapOptions};
use num_enum::{IntoPrimitive, TryFromPrimitive, TryFromPrimitiveError};
use uuid::Uuid;

use crate::components::{Component, ComponentError, ComponentType};

#[repr(u64)]
#[derive(Debug, Clone, Copy, IntoPrimitive, TryFromPrimitive, PartialEq)]
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
pub struct Header {
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

impl Header {
    pub unsafe fn from_raw_mut(ptr: *mut u8) -> Option<&'static mut Self> {
        (ptr as *mut Header)
            .as_mut()
    }

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
pub struct BomEntry {
    pub family: u8,
    pub ctype: u8,
    pub mode: u8,
    pub name: [u8; 13],
    pub offset: i64,
    pub size: i64,
    pub param1: i64,
    pub param2: i64,
}

impl BomEntry {
    fn name(&self) -> Option<&str> {
        str::from_utf8(&self.name).ok()
            .map(|s| s.trim_end_matches("\0"))
    }
}

#[derive(Debug)]
pub struct Container<'map> {
    name: String,
    mmap: Mmap,
    header: &'map Header,
    bom: &'map [BomEntry]
}

impl<'map> Container<'map> {
    pub fn from_mmap(mmap: Mmap, name: String) -> Result<Self, Error> {
        let Range { start, end } = mmap.as_ref().as_ptr_range();

        // map header
        let header = unsafe {
            if start.offset(mem::size_of::<Header>().try_into()?) <= end {
                (start as *const Header)
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

            if bom_ptr.offset((mem::size_of::<BomEntry>() * n).try_into()?) <= end {
                let first_bom = bom_ptr as *const BomEntry;
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

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn into_raw_parts(self) -> (String, Mmap, &'map Header, &'map [BomEntry]) {
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

pub struct ContainerBuilder<'map> {
    file: File,
    mmap: MmapMut,
    name: String,
    header_builder: HeaderBuilder<'map>,
    bom_builder: BomBuilder<'map>,
}

impl<'map> ContainerBuilder<'map> {
    pub fn new_into_file(name: String, file: File, capacity: u8) -> Self {
        // make sure the mmap contains space for the header and 255 BOM entries
        let headerbomsize = mem::size_of::<Header>() + (mem::size_of::<BomEntry>() * capacity as usize);
        file.set_len(headerbomsize as u64).unwrap();

        let mut mmap = unsafe { MmapOptions::new().offset(0).len(headerbomsize).map_mut(&file).unwrap() };
        let header = unsafe { Header::from_raw_mut(mmap.as_mut_ptr()).unwrap() };
        let bom = unsafe { mmap.as_mut_ptr().offset(mem::size_of::<Header>() as isize) as *mut BomEntry };

        Self {
            file,
            mmap,
            name,
            header_builder: HeaderBuilder::new(header).allocated(capacity),
            bom_builder: BomBuilder::new(bom, capacity),
        }
    }

    pub fn edit_header(mut self, f: impl FnOnce(&mut HeaderBuilder) -> ()) -> Self {
        f(&mut self.header_builder);
        self
    }

    pub fn add_component(mut self, name: &str, ctype: ComponentType, f: impl FnOnce(&mut BomEntry, &mut File) -> ()) -> Self {
        let bom_entry = unsafe { self.bom_builder.new_component() };

        let name = name.as_bytes();
        assert!(name.len() < 13, "component name too long");
        assert!(name.iter().all(| char | (0x20..0x80).contains(char)), "component name contains unprintable characters");
        bom_entry.name[..name.len()].copy_from_slice(name);

        let raw: u16 = ctype.into();
        bom_entry.family = 0x01;
        bom_entry.ctype = (raw >> 8) as u8;
        bom_entry.mode = raw as u8;

        let offset = bom_entry.offset;
        self.file.seek(SeekFrom::Start(offset as u64)).unwrap();

        f(bom_entry, &mut self.file);

        assert!(bom_entry.offset == offset, "component offset modified during add_component");

        self
    }

    pub fn build(self) -> Container<'map> {
        let header = self.header_builder.build();
        let bom = self.bom_builder.build();

        header.used = bom.len() as u8;
        assert!(header.used <= header.allocated, "more components used than allocated");
        assert!(header.used as usize == bom.len(), "number of components in BOM inconsistent with header");

        // trim file to minimum
        let mut actualsize = mem::size_of::<Header>() + (mem::size_of::<BomEntry>() * header.allocated as usize);
        if let Some(entry) = bom.last() {
            actualsize += entry.offset as usize + entry.size as usize;
        }
        self.file.set_len(actualsize as u64).unwrap();

        let mmap = unsafe {
            Mmap::map(&self.file).unwrap()
        };

        Container {
            name: self.name,
            mmap,
            header,
            bom,
        }
    }
}

pub struct HeaderBuilder<'map> {
    header: &'map mut Header,
}

impl<'map> HeaderBuilder<'map> {
    pub fn new(header: &'map mut Header) -> Self {
        header.magic = "Ziggurat".as_bytes().try_into().unwrap();
        header.version = "1.0".as_bytes().try_into().unwrap();
        header.family = 0;
        header.class = 0;
        header.ctype = 0;
        header.allocated = 0;
        header.used = 0;
        header.uuid.fill(0);
        header.base1_uuid.fill(0);
        header.base2_uuid.fill(0);
        header.dim1 = 0;
        header.dim2 = 0;
        header.extensions = 0;

        Self {
            header
        }
    }

    fn from_raw(header: &'map mut Header) -> Self {
        Self {
            header
        }
    }

    fn allocated(self, value: u8) -> Self {
        self.header.allocated = value;
        self
    }

    pub fn comment(&mut self, text: &str) -> &mut Self {
        let bytes = text.as_bytes();
        assert!(bytes.len() <= 72, "comment too long");
        self.header.comment[..bytes.len()].copy_from_slice(bytes);
        self
    }

    pub fn ziggurat_type(&mut self, type_enum: Type) -> &mut Self {
        let raw: u64 = type_enum.into();
        self.header.ctype = raw as u8;
        self.header.class = (raw >> 8) as u8;
        self.header.family = (raw >> 16) as u8;
        self
    }

    pub fn family(&mut self, value: char) -> &mut Self {
        self.header.family = value.try_into().unwrap();
        self
    }

    pub fn class(&mut self, value: char) -> &mut Self {
        self.header.class = value.try_into().unwrap();
        self
    }

    pub fn ctype(&mut self, value: char) -> &mut Self {
        self.header.ctype = value.try_into().unwrap();
        self
    }

    pub fn dim1(&mut self, value: usize) -> &mut Self {
        self.header.dim1 = value as i64;
        self
    }

    pub fn dim2(&mut self, value: usize) -> &mut Self {
        self.header.dim2 = value as i64;
        self
    }

    pub fn extensions(&mut self, value: i64) -> &mut Self {
        self.header.extensions = value;
        self
    }

    pub fn base1(&mut self, uuid: Option<Uuid>) -> &mut Self {
        match uuid {
            Some(uuid) => self.header.base1_uuid = uuid.as_u128().to_be_bytes(),
            None => self.header.base1_uuid.fill(0),
        }
        self
    }

    pub fn base2(&mut self, uuid: Option<Uuid>) -> &mut Self {
        match uuid {
            Some(uuid) => self.header.base2_uuid = uuid.as_u128().to_be_bytes(),
            None => self.header.base2_uuid.fill(0),
        }
        self
    }

    pub fn build(self) -> &'map mut Header {
        let header = self.header;

        // Identification triplet needs to be upper/lower case ascii letters
        assert!(header.family > 0x40 && header.family <= 0x5A, "invalid container family");
        assert!(header.class > 0x40 && header.class <= 0x5A, "invalid container class");
        assert!(header.ctype > 0x60 && header.ctype <= 0x7A, "invalid container type");

        if header.uuid().is_nil() {
            let uuid = dbg!(Uuid::new_v4());
            header.uuid = uuid.as_u128().to_be_bytes();
        }
        assert!(!header.uuid().is_nil(), "UUID musn't be zero");

        header
    }
}

pub struct BomBuilder<'map> {
    bom: &'map mut [BomEntry],
    capacity: u8,
}

impl<'map> BomBuilder<'map> {
    pub fn new(bom: *mut BomEntry, capacity: u8) -> Self {
        let bom = unsafe { std::slice::from_raw_parts_mut(bom, 0) };
        Self {
            bom,
            capacity,
        }
    }

    fn align_offset(o: usize) -> usize {
        if o % 8 > 0 {
            o + (8 - (o % 8))
        } else {
            o
        }
    }

    pub unsafe fn new_component(&mut self,) -> &mut BomEntry {
        assert!(self.bom.len() < self.capacity as usize, "new component beyond BOM capacity");

        let new_offset = match self.bom.last() {
            Some(entry) => Self::align_offset(entry.offset as usize + entry.size as usize),
            None => mem::size_of::<Header>() + (mem::size_of::<BomEntry>() * self.capacity as usize),
        };

        
        let length = self.bom.len();
        let ptr = self.bom.as_mut_ptr();
        self.bom = std::slice::from_raw_parts_mut(ptr, length + 1);

        let last = self.bom.last_mut().unwrap();
        last.offset = new_offset as i64;
        last
    }

    pub fn build(self) -> &'map mut [BomEntry] {
        self.bom
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::{BufWriter, Seek, SeekFrom, Write}, mem};

    use crate::components::ComponentType;

    use super::ContainerBuilder;

    #[test]
    fn instantiate_empty() {
        let filename = "/tmp/container.zigtest";
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();

        ContainerBuilder::new_into_file("Test".to_owned(), file, 1)
            .edit_header(| hb | {
                hb.comment("this is a test container :)")
                    .family('X')
                    .class('X')
                    .ctype('x');
            })
            .build();
    }

    #[test]
    fn instantiate_blob() {
        let filename = "/tmp/blob.zigl";
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();

        println!();

        ContainerBuilder::new_into_file("testvar".to_owned(), file, 3)
            .edit_header(| h | {
                h.comment("This is a test container containing some blobs")
                    .family('X')
                    .class('X')
                    .ctype('x');
            })
            .add_component("Blob1", ComponentType::Blob, | bom, file | {
                let buf = "hello, I am the first test blob. have a nice day! :D".as_bytes();
                file.write_all(buf).unwrap();
                bom.size = buf.len() as i64;
                bom.param1 = buf.len() as i64;
                println!("Blob1: {:?}", bom);
            })
            .add_component("Blob2", ComponentType::Blob, | bom, file | {
                let buf = "sup, I'm another test blob. I may fuck your shit up :3".as_bytes();
                file.write_all(buf).unwrap();
                bom.size = buf.len() as i64;
                bom.param1 = buf.len() as i64;
                println!("Blob2: {:?}", bom);
            })
            .add_component("Blob3", ComponentType::Blob, | bom, file | {
                let buf: Vec<u64> = (1..100000).collect();
                let blen = buf.len() * mem::size_of::<u64>();
                let bs = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, blen) };
                file.write_all(bs).unwrap();
                bom.size = blen as i64;
                bom.param1 = blen as i64;
                println!("Blob3: {:?}", bom);
            })
            .build();
    }

    #[test]
    fn howtofile() {
        let filename = "/tmp/filetest.zigtest";
        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();

        file.set_len(100).unwrap();
        file.seek(SeekFrom::Start(100)).unwrap();

        let mut bufw = BufWriter::new(&file);
        bufw.write("hellooooooooo test :D".as_bytes()).unwrap();
        bufw.flush().unwrap();

        // let mut mmap = unsafe { MmapOptions::new().len(100).offset(0).map_mut(&file).unwrap() };
        // let str1 = "hello!".as_bytes();
        // mmap[..str1.len()].copy_from_slice(str1);

        // let mut mmap2 = unsafe { MmapOptions::new().len(100).offset(1000).map_mut(&file).unwrap() };
        // let str2 = "I'm also here :>".as_bytes();
        // mmap2[..str2.len()].copy_from_slice(str2);
    }
}
