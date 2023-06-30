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

use components::{IndexComp, Vector, VectorDelta};
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
            let primarylayer = PrimaryLayer::try_from_container(container)?;
            let layer = Rc::new(Layer::Primary(primarylayer));

            layers_by_uuid.insert(uuid, layer.clone());
            layers_by_name.insert(name, layer);
        }

        while containers
            .values()
            .any(|c| c.header.container_type == ContainerType::SegmentationLayer)
        {
            let has_instantiated_parent = containers.drain_filter(|_, c| {
                c.header.container_type == ContainerType::SegmentationLayer
                    && layers_by_uuid.contains_key(
                        &c.header
                            .base1_uuid
                            .expect("SegmentationLayer without base layer"),
                    )
            });

            let mut temp_by_uuid = Vec::new();
            for (uuid, container) in has_instantiated_parent {
                let name = container.name.clone();
                let base = layers_by_uuid
                    .get(&container.header.base1_uuid.ok_or(
                        DatastoreError::ContainerInstantiationError(
                            TryFromContainerError::ConsistencyError(
                                "secondary layer with no declared base layer",
                            ),
                        ),
                    )?)
                    .ok_or(DatastoreError::ConsistencyError(
                        "secondary layer with base layer not in datastore",
                    ))?;
                let seglayer = SegmentationLayer::try_from_container(container, base.clone())?;
                let layer = Rc::new(Layer::Segmentation(seglayer));

                temp_by_uuid.push((uuid, layer.clone()));
                layers_by_name.insert(name, layer);
            }

            layers_by_uuid.extend(temp_by_uuid);

            // TODO add parent layer to SegmentationLayer
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
    ConsistencyError(&'static str),
}

impl fmt::Display for DatastoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatastoreError::IoError(e) => write!(f, "{}", e),
            DatastoreError::RawContainerError(e) => write!(f, "{}", e),
            DatastoreError::ContainerInstantiationError(e) => write!(f, "{}", e),
            DatastoreError::ConsistencyError(e) => write!(f, "consistency error: {}", e),
        }
    }
}

impl error::Error for DatastoreError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            DatastoreError::IoError(e) => Some(e),
            DatastoreError::RawContainerError(e) => Some(e),
            DatastoreError::ContainerInstantiationError(e) => Some(e),
            _ => None,
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
    Segmentation(SegmentationLayer<'a>),
}

#[derive(Debug)]
pub enum TryFromContainerError {
    WrongContainerType,
    MissingComponent(&'static str),
    WrongComponentType(&'static str),
    WrongComponentDimensions(&'static str),
    ConsistencyError(&'static str),
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
            TryFromContainerError::WrongComponentDimensions(s) => {
                write!(f, "component {} has wrong dimensions", s)
            }
            TryFromContainerError::ConsistencyError(s) => {
                write!(f, "consinstency error: {} ", s)
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

impl<'a> PrimaryLayer<'a> {
    fn try_from_container(container: Container<'a>) -> Result<Self, TryFromContainerError> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            ContainerType::PrimaryLayer => match components.entry("Partition") {
                Entry::Occupied(entry) => {
                    let partition = entry
                        .remove()
                        .into_vector()
                        .map_err(|_| TryFromContainerError::WrongComponentType("Partition"))?;

                    if partition.length < 2 || partition.width != 1 {
                        Err(TryFromContainerError::WrongComponentDimensions("Partition"))
                    } else {
                        Ok(Self {
                            mmap,
                            name,
                            header,
                            partition,
                        })
                    }
                }

                Entry::Vacant(_) => Err(TryFromContainerError::MissingComponent("Partition")),
            },

            _ => Err(TryFromContainerError::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct SegmentationLayer<'a> {
    base: Rc<Layer<'a>>,
    mmap: Mmap,
    pub name: String,
    pub header: ContainerHeader<'a>,
    partition: Vector<'a>,
    range_stream: VectorDelta<'a>,
    start_sort: IndexComp<'a>,
    end_sort: IndexComp<'a>,
}

impl<'a> SegmentationLayer<'a> {
    fn try_from_container(
        container: Container<'a>,
        base: Rc<Layer<'a>>,
    ) -> Result<Self, TryFromContainerError> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            ContainerType::SegmentationLayer => {
                if let None = header.base1_uuid {
                    return Err(TryFromContainerError::ConsistencyError(
                        "SegmentationLayer without base layer",
                    ));
                }

                let partition = match components.entry("Partition") {
                    Entry::Occupied(entry) => entry
                        .remove()
                        .into_vector()
                        .map_err(|_| TryFromContainerError::WrongComponentType("Partition")),

                    Entry::Vacant(_) => Err(TryFromContainerError::MissingComponent("Partition")),
                }?;

                if partition.length < 2 || partition.width != 1 {
                    return Err(TryFromContainerError::WrongComponentDimensions("Partition"));
                }

                let range_stream = match components.entry("RangeStream") {
                    Entry::Occupied(entry) => entry
                        .remove()
                        .into_vector_delta()
                        .map_err(|_| TryFromContainerError::WrongComponentType("RangeStream")),

                    Entry::Vacant(_) => Err(TryFromContainerError::MissingComponent("RangeStream")),
                }?;

                if range_stream.width != 2 {
                    return Err(TryFromContainerError::WrongComponentDimensions(
                        "RangeStream",
                    ));
                }

                let start_sort = match components.entry("StartSort") {
                    Entry::Occupied(entry) => entry
                        .remove()
                        .into_index_comp()
                        .map_err(|_| TryFromContainerError::WrongComponentType("StartSort")),

                    Entry::Vacant(_) => Err(TryFromContainerError::MissingComponent("StartSort")),
                }?;

                let end_sort = match components.entry("EndSort") {
                    Entry::Occupied(entry) => entry
                        .remove()
                        .into_index_comp()
                        .map_err(|_| TryFromContainerError::WrongComponentType("EndSort")),

                    Entry::Vacant(_) => Err(TryFromContainerError::MissingComponent("EndSort")),
                }?;

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    partition,
                    range_stream,
                    start_sort,
                    end_sort,
                })
            }

            _ => Err(TryFromContainerError::WrongContainerType),
        }
    }
}
