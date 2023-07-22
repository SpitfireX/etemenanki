#![allow(dead_code)]
#![feature(hash_drain_filter)]
#![feature(pattern)]

use std::{
    collections::{hash_map, HashMap},
    error,
    ffi::OsStr,
    fmt,
    fs::File,
    io, ops,
    path::{Path, PathBuf},
};

use container::Container;
use memmap2::Mmap;
use uuid::Uuid;

pub mod components;
pub mod container;
pub mod layers;
#[cfg(test)]
mod tests;
pub mod variables;

#[derive(Debug)]
pub struct Datastore<'map> {
    path: PathBuf,
    layers_by_uuid: HashMap<Uuid, layers::Layer<'map>>,
    uuids_by_name: HashMap<String, Uuid>,
}

impl<'map> Datastore<'map> {
    pub fn layer_by_name<S: AsRef<str>>(&self, name: S) -> Option<&layers::Layer> {
        match self.uuids_by_name.get(name.as_ref()) {
            Some(u) => self.layers_by_uuid.get(u),
            None => None,
        }
    }

    pub fn layer_by_uuid(&self, uuid: &Uuid) -> Option<&layers::Layer> {
        self.layers_by_uuid.get(uuid)
    }

    pub fn layer_names(&self) -> hash_map::Keys<String, Uuid> {
        self.uuids_by_name.keys()
    }

    pub fn layer_uuids(&self) -> hash_map::Keys<Uuid, layers::Layer> {
        self.layers_by_uuid.keys()
    }

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
        let mut uuids_by_name = HashMap::new();

        let players = containers
            .drain_filter(|_, c| c.header.container_type == container::Type::PrimaryLayer);

        for (uuid, container) in players {
            let name = container.name.clone();
            let primarylayer = container.try_into()?;
            let layer = layers::Layer::new_primary(primarylayer);

            layers_by_uuid.insert(uuid, layer);
            uuids_by_name.insert(name, uuid);
        }

        while containers
            .values()
            .any(|c| c.header.container_type == container::Type::SegmentationLayer)
        {
            let seglayers = containers
                .drain_filter(|_, c| c.header.container_type == container::Type::SegmentationLayer);

            let mut temp_by_uuid = Vec::new();
            for (uuid, container) in seglayers {
                let name = container.name.clone();

                let seglayer: layers::SegmentationLayer = container.try_into()?;
                if !layers_by_uuid.contains_key(&seglayer.base) {
                    return Err(DatastoreError::ConsistencyError(
                        "secondary layer with base layer not in datastore",
                    ));
                }

                let layer = layers::Layer::new_segmentation(seglayer);

                temp_by_uuid.push((uuid, layer));
                uuids_by_name.insert(name, uuid);
            }

            layers_by_uuid.extend(temp_by_uuid);
        }

        let vars = containers.drain_filter(|_, c| c.header.raw_class == 'V');

        for (_, container) in vars {
            let base = layers_by_uuid
                .get_mut(&container.header.base1_uuid.ok_or(
                    DatastoreError::ContainerInstantiationError(
                        container::TryFromError::ConsistencyError(
                            "variable with no declared base layer",
                        ),
                    ),
                )?)
                .ok_or(DatastoreError::ConsistencyError(
                    "variable with base layer not in datastore",
                ))?;
            let name = container.name.clone();

            let var: variables::Variable = container.try_into()?;
            if let Err(_) = base.add_variable(name, var) {
                return Err(DatastoreError::ConsistencyError(
                    "variable inconsistent with base layer",
                ));
            }
        }

        Ok(Datastore {
            path,
            layers_by_uuid,
            uuids_by_name,
        })
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl<'map> ops::Index<&Uuid> for Datastore<'map> {
    type Output = layers::Layer<'map>;

    fn index(&self, index: &Uuid) -> &Self::Output {
        &self.layers_by_uuid[index]
    }
}

impl<'map> ops::Index<&str> for Datastore<'map> {
    type Output = layers::Layer<'map>;

    fn index(&self, index: &str) -> &Self::Output {
        &self.layers_by_uuid[&self.uuids_by_name[index]]
    }
}

impl<'map> ops::Index<&String> for Datastore<'map> {
    type Output = layers::Layer<'map>;

    fn index(&self, index: &String) -> &Self::Output {
        &self.layers_by_uuid[&self.uuids_by_name[index]]
    }
}

#[derive(Debug)]
pub enum DatastoreError {
    IoError(io::Error),
    RawContainerError(container::Error),
    ContainerInstantiationError(container::TryFromError),
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

impl From<container::Error> for DatastoreError {
    fn from(value: container::Error) -> Self {
        DatastoreError::RawContainerError(value)
    }
}

impl From<container::TryFromError> for DatastoreError {
    fn from(value: container::TryFromError) -> Self {
        DatastoreError::ContainerInstantiationError(value)
    }
}

mod macros {
    macro_rules! check_and_return_component {
        ($components:expr, $name:literal, $type:ident) => {
            match $components.entry($name) {
                std::collections::hash_map::Entry::Occupied(entry) => paste::paste! {
                    entry.remove()
                        .[<into_ $type:snake>]()
                        .map_err(|_| container::TryFromError::WrongComponentType($name))
                },

                std::collections::hash_map::Entry::Vacant(_) => {
                    Err(container::TryFromError::MissingComponent($name))
                }
            }
        };
    }

    macro_rules! get_container_base {
        ($header:expr, $selftype:ident) => {
            match $header.base1_uuid {
                Some(uuid) => uuid,
                None => {
                    return Err(container::TryFromError::ConsistencyError(concat!(
                        stringify!($selftype),
                        " without base layer"
                    )));
                }
            }
        };
    }

    pub(crate) use check_and_return_component;
    pub(crate) use get_container_base;
}
