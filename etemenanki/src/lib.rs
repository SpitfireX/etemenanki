#![allow(dead_code)]
#![feature(hash_drain_filter)]

use std::{
    collections::{hash_map::Entry, HashMap},
    error,
    ffi::OsStr,
    fmt::{self, Display},
    fs::File,
    io,
    path::{Path, PathBuf},
    rc::Rc,
};

use components::Vector;
use container::{Container, ContainerError, ContainerHeader, ContainerType};
use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

pub mod components;
pub mod container;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct Datastore<'a> {
    pub path: PathBuf,
    pub layers_by_name: HashMap<String, Rc<Layer<'a>>>,
    pub layers_by_uuid: HashMap<Uuid, Rc<Layer<'a>>>,
}

impl<'a> Datastore<'a> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DatastoreError> {
        let path = path.as_ref().to_owned();
        let mut containers = HashMap::new();

        for entry in path.read_dir()? {
            let path = entry?.path();
            let filename = path.file_name().and_then(OsStr::to_str);

            if let Some(filename) = filename {
                if let Some(ext) = path.extension().and_then(OsStr::to_str) {
                    match ext {
                        "zigv" | "zigl" => {
                            let file = File::open(&path)?;
                            let mmap = unsafe { Mmap::map(&file)? };
                            let name = filename
                                .strip_suffix(ext)
                                .unwrap()
                                .strip_suffix(".")
                                .unwrap()
                                .to_owned();
                            let container = Container::from_mmap(mmap, name)?;

                            containers.insert(container.header.uuid, container);
                        }
                        _ => (),
                    }
                }
            }
        }

        let mut layers_by_uuid = HashMap::new();
        let mut layers_by_name = HashMap::new();

        let drainfilter =
            containers.drain_filter(|_, c| ContainerType::PrimaryLayer == c.header.container_type);

        for (uuid, container) in drainfilter {
            let name = container.name.clone();
            let primarylayer: PrimaryLayer = container.try_into()?;
            let layer = Rc::new(Layer::Primary(primarylayer));

            layers_by_uuid.insert(uuid, layer.clone());
            layers_by_name.insert(name, layer);
        }

        Ok(Datastore {
            path,
            layers_by_name,
            layers_by_uuid,
        })
    }
}

#[derive(Debug)]
pub enum DatastoreError {
    IoError(io::Error),
    RawContainerError(ContainerError),
    ContainerInstantiationError(TryFromContainerError),
}

impl fmt::Display for DatastoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatastoreError::IoError(e) => write!(f, "{}", e),
            DatastoreError::RawContainerError(e) => write!(f, "{}", e),
            DatastoreError::ContainerInstantiationError(e) => write!(f, "{}", e),
        }
    }
}

impl error::Error for DatastoreError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            DatastoreError::IoError(e) => Some(e),
            DatastoreError::RawContainerError(e) => Some(e),
            DatastoreError::ContainerInstantiationError(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for DatastoreError {
    fn from(value: std::io::Error) -> Self {
        DatastoreError::IoError(value)
    }
}

impl From<ContainerError> for DatastoreError {
    fn from(value: ContainerError) -> Self {
        DatastoreError::RawContainerError(value)
    }
}

impl From<TryFromContainerError> for DatastoreError {
    fn from(value: TryFromContainerError) -> Self {
        DatastoreError::ContainerInstantiationError(value)
    }
}

#[derive(Debug, EnumAsInner)]
pub enum Layer<'a> {
    Primary(PrimaryLayer<'a>),
}

#[derive(Debug)]
pub enum TryFromContainerError {
    WrongContainerType,
    MissingComponent(&'static str),
    WrongComponentType(&'static str),
}

impl Display for TryFromContainerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryFromContainerError::WrongContainerType => {
                write!(f, "wrong container type for conversion")
            }
            TryFromContainerError::MissingComponent(s) => {
                write!(f, "missing component {} in source container", s)
            }
            TryFromContainerError::WrongComponentType(s) => {
                write!(f, "component {} has wrong type", s)
            }
        }
    }
}

impl error::Error for TryFromContainerError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct PrimaryLayer<'a> {
    mmap: Mmap,
    pub name: String,
    pub header: ContainerHeader<'a>,
    partition: Vector<'a>,
}

impl<'a> TryFrom<Container<'a>> for PrimaryLayer<'a> {
    type Error = TryFromContainerError;

    fn try_from(value: Container<'a>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = value;

        match header.container_type {
            ContainerType::PrimaryLayer => match components.entry("Partition") {
                Entry::Occupied(entry) => {
                    let partition = entry
                        .remove()
                        .into_vector()
                        .map_err(|_| TryFromContainerError::WrongComponentType("Partition"))?;
                    Ok(Self {
                        mmap,
                        name,
                        header,
                        partition,
                    })
                }

                Entry::Vacant(_) => Err(TryFromContainerError::MissingComponent("Partition")),
            },

            _ => Err(TryFromContainerError::WrongContainerType),
        }
    }
}
