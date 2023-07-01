#![allow(dead_code)]
#![feature(hash_drain_filter)]

use std::{
    collections::HashMap,
    error,
    ffi::OsStr,
    fmt::{self, Display},
    fs::File,
    io,
    path::{Path, PathBuf},
};

use components::*;
use container::{Container, ContainerError, ContainerHeader, ContainerType};
use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use paste::paste;
use uuid::Uuid;

pub mod components;
pub mod container;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct Datastore<'a> {
    pub path: PathBuf,
    pub layers_by_uuid: HashMap<Uuid, Layer<'a>>,
    pub uuids_by_name: HashMap<String, Uuid>,
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
        let mut uuids_by_name = HashMap::new();

        let players =
            containers.drain_filter(|_, c| c.header.container_type == ContainerType::PrimaryLayer);

        for (uuid, container) in players {
            let name = container.name.clone();
            let primarylayer = container.try_into()?;
            let layer = Layer::init_primary(primarylayer);

            layers_by_uuid.insert(uuid, layer);
            uuids_by_name.insert(name, uuid);
        }

        while containers
            .values()
            .any(|c| c.header.container_type == ContainerType::SegmentationLayer)
        {
            let seglayers = containers
                .drain_filter(|_, c| c.header.container_type == ContainerType::SegmentationLayer);

            let mut temp_by_uuid = Vec::new();
            for (uuid, container) in seglayers {
                let name = container.name.clone();

                let seglayer: SegmentationLayer = container.try_into()?;
                if !layers_by_uuid.contains_key(&seglayer.base) {
                    return Err(DatastoreError::ConsistencyError(
                        "secondary layer with base layer not in datastore",
                    ));
                }

                let layer = Layer::init_segmentation(seglayer);

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
                        TryFromContainerError::ConsistencyError(
                            "variable with no declared base layer",
                        ),
                    ),
                )?)
                .ok_or(DatastoreError::ConsistencyError(
                    "variable with base layer not in datastore",
                ))?;
            let name = container.name.clone();

            let var: Variable = container.try_into()?;
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
}

macro_rules! check_and_return_component {
    ($components:expr, $name:literal, $type:ident) => {
        match $components.entry($name) {
            std::collections::hash_map::Entry::Occupied(entry) => paste! {
                entry.remove()
                    .[<into_ $type:snake>]()
                    .map_err(|_| TryFromContainerError::WrongComponentType($name))
            },

            std::collections::hash_map::Entry::Vacant(_) => {
                Err(TryFromContainerError::MissingComponent($name))
            }
        }
    };
}

macro_rules! get_container_base {
    ($header:expr, $selftype:ident) => {
        match $header.base1_uuid {
            Some(uuid) => uuid,
            None => {
                return Err(TryFromContainerError::ConsistencyError(
                    concat!(stringify!($selftype), " without base layer")
                ));
            }
        }
    };
}

#[derive(Debug)]
pub enum Variable<'a> {
    IndexedString(IndexedStringVariable<'a>),
    PlainString(PlainStringVariable<'a>),
    Integer(IntegerVariable<'a>),
    Pointer,
    ExternalPointer,
    Set(SetVariable<'a>),
    Hash,
}

impl<'a> TryFrom<Container<'a>> for Variable<'a> {
    type Error = TryFromContainerError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
        match container.header.container_type {
            ContainerType::IndexedStringVariable => Ok(Self::IndexedString(
                IndexedStringVariable::try_from(container)?,
            )),

            ContainerType::PlainStringVariable => {
                Ok(Self::PlainString(PlainStringVariable::try_from(container)?))
            }

            ContainerType::IntegerVariable => {
                Ok(Self::Integer(IntegerVariable::try_from(container)?))
            }

            ContainerType::PointerVariable => todo!(),

            ContainerType::ExternalPointerVariable => todo!(),

            ContainerType::SetVariable => Ok(Self::Set(SetVariable::try_from(container)?)),

            ContainerType::HashVariable => todo!(),

            _ => Err(TryFromContainerError::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct IndexedStringVariable<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: ContainerHeader<'a>,
    lexicon: StringVector<'a>,
    lex_hash: Index<'a>,
    partition: Vector<'a>,
    lex_id_stream: VectorComp<'a>,
    lex_id_index: InvertedIndex<'a>,
}

impl<'a> IndexedStringVariable<'a> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }

    pub fn n_types(&self) -> usize {
        self.header.dim2
    }
}

impl<'a> TryFrom<Container<'a>> for IndexedStringVariable<'a> {
    type Error = TryFromContainerError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            ContainerType::IndexedStringVariable => {
                let base = get_container_base!(header, PlainStringVariable);
                let n = header.dim1;
                let v = header.dim2;
                
                let lexicon = check_and_return_component!(components, "Lexicon", StringVector)?;
                if lexicon.len() != v {
                    return Err(TryFromContainerError::WrongComponentDimensions("Lexicon"));
                }

                let lex_hash = check_and_return_component!(components, "LexHash", Index)?;
                if lex_hash.len() != v {
                    return Err(TryFromContainerError::WrongComponentDimensions("LexHash"));
                }

                let partition = check_and_return_component!(components, "Partition", Vector)?;
                // consistency gets checked at datastore creation

                let lex_id_stream = check_and_return_component!(components, "LexIDStream", VectorComp)?;
                if lex_id_stream.len() != n || lex_id_stream.width() != 1 {
                    return Err(TryFromContainerError::WrongComponentDimensions("LexIDStream"));
                }

                let lex_id_index = check_and_return_component!(components, "LexIDIndex", InvertedIndex)?;
                if lex_id_index.n_types() != v {
                    return Err(TryFromContainerError::WrongComponentDimensions("LexIDIndex"));
                }

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    lexicon,
                    lex_hash,
                    partition,
                    lex_id_stream,
                    lex_id_index,
                })
            }

            _ => Err(TryFromContainerError::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct PlainStringVariable<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: ContainerHeader<'a>,
    string_data: StringList<'a>,
    offset_stream: VectorDelta<'a>,
    string_hash: IndexComp<'a>,
}

impl<'a> PlainStringVariable<'a> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'a> TryFrom<Container<'a>> for PlainStringVariable<'a> {
    type Error = TryFromContainerError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            ContainerType::PlainStringVariable => {
                let base = get_container_base!(header, PlainStringVariable);
                let n = header.dim1;

                let string_data = check_and_return_component!(components, "StringData", StringList)?;
                if string_data.len() != n {
                    return Err(TryFromContainerError::WrongComponentDimensions("StringData"));
                }

                let offset_stream = check_and_return_component!(components, "OffsetStream", VectorDelta)?;
                if offset_stream.len() != n+1 || offset_stream.width != 1 {
                    return Err(TryFromContainerError::WrongComponentDimensions("OffsetStream"));
                }
                
                let string_hash = check_and_return_component!(components, "StringHash", IndexComp)?;
                if string_hash.len() != n {
                    return Err(TryFromContainerError::WrongComponentDimensions("StringHash"))
                }
                
                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    string_data,
                    offset_stream,
                    string_hash
                })
            }

            _ => Err(TryFromContainerError::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct IntegerVariable<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: ContainerHeader<'a>,
    int_stream: VectorComp<'a>,
    int_sort: IndexComp<'a>,
}

impl<'a> IntegerVariable<'a> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }

    pub fn b(&self) -> usize {
        self.header.dim2
    }
}

impl<'a> TryFrom<Container<'a>> for IntegerVariable<'a> {
    type Error = TryFromContainerError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            ContainerType::IntegerVariable => {
                let base = get_container_base!(header, PlainStringVariable);
                let n = header.dim1;

                let int_stream = check_and_return_component!(components, "IntStream", VectorComp)?;
                if int_stream.len() != n || int_stream.width() != 1 {
                    return Err(TryFromContainerError::WrongComponentDimensions("IntStream"))
                }

                let int_sort = check_and_return_component!(components, "IntSort", IndexComp)?;
                if int_sort.len() != n {
                    return Err(TryFromContainerError::WrongComponentDimensions("IntSort"));
                }

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    int_stream,
                    int_sort,
                })
            }

            _ => Err(TryFromContainerError::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct SetVariable<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: ContainerHeader<'a>,
    lexicon: StringVector<'a>,
    lex_hash: Index<'a>,
    partition: Vector<'a>,
    id_set_stream: Set<'a>,
    id_set_index: InvertedIndex<'a>,
}

impl<'a> SetVariable<'a> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }

    pub fn n_types(&self) -> usize {
        self.header.dim2
    }
}

impl<'a> TryFrom<Container<'a>> for SetVariable<'a> {
    type Error = TryFromContainerError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            ContainerType::SetVariable => {
                let base = get_container_base!(header, PlainStringVariable);
                let n = header.dim1;
                let v = header.dim2;

                let lexicon = check_and_return_component!(components, "Lexicon", StringVector)?;
                if lexicon.len() != v {
                    return Err(TryFromContainerError::WrongComponentDimensions("Lexicon"));
                }

                let lex_hash = check_and_return_component!(components, "LexHash", Index)?;
                if lex_hash.len() != v {
                    return Err(TryFromContainerError::WrongComponentDimensions("LexHash"));
                }

                let partition = check_and_return_component!(components, "Partition", Vector)?;
                // consistency gets checked at datastore creation
                
                let id_set_stream = check_and_return_component!(components, "IDSetStream", Set)?;
                if id_set_stream.len() != n {
                    return Err(TryFromContainerError::WrongComponentDimensions("IDSetStream"));
                }
                
                let id_set_index = check_and_return_component!(components, "IDSetIndex", InvertedIndex)?;
                if id_set_index.n_types() != v {
                    return Err(TryFromContainerError::WrongComponentDimensions("IDSetIndex"));
                }
                
                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    lexicon,
                    lex_hash,
                    partition,
                    id_set_stream,
                    id_set_index,
                })
            }

            _ => Err(TryFromContainerError::WrongContainerType),
        }
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
    Primary(PrimaryLayer<'a>, LayerVariables<'a>),
    Segmentation(SegmentationLayer<'a>, LayerVariables<'a>),
}

impl<'a> Layer<'a> {
    pub fn add_variable(&mut self, name: String, var: Variable<'a>) -> Result<(), Variable> {
        let baselen = self.len();
        let varlen = match &var {
            Variable::IndexedString(v) => v.len(),
            Variable::PlainString(v) => v.len(),
            Variable::Integer(v) => v.len(),
            Variable::Pointer => todo!(),
            Variable::ExternalPointer => todo!(),
            Variable::Set(v) => v.len(),
            Variable::Hash => todo!(),
        };

        if varlen != baselen {
            Err(var)
        } else {
            match self {
                Layer::Primary(_, vars) => vars.add_variable(name, var),
                Layer::Segmentation(_, vars) => vars.add_variable(name, var),
            }
        }
    }

    pub fn init_primary(layer: PrimaryLayer<'a>) -> Self {
        Self::Primary(layer, LayerVariables::new())
    }

    pub fn init_segmentation(layer: SegmentationLayer<'a>) -> Self {
        Self::Segmentation(layer, LayerVariables::new())
    }

    pub fn len(&self) -> usize {
        match &self {
            Layer::Primary(l, _) => l.len(),
            Layer::Segmentation(l, _) => l.len(),
        }
    }
}

#[derive(Debug)]
pub struct LayerVariables<'a> {
    variables: HashMap<String, Variable<'a>>,
}

impl<'a> LayerVariables<'a> {
    pub fn add_variable(&mut self, name: String, var: Variable<'a>) -> Result<(), Variable> {
        if self.variables.contains_key(&name) {
            Err(var)
        } else {
            self.variables.insert(name, var);
            Ok(())
        }
    }

    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }
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
    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'a> TryFrom<Container<'a>> for PrimaryLayer<'a> {
    type Error = TryFromContainerError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            ContainerType::PrimaryLayer => {
                let partition = check_and_return_component!(components, "Partition", Vector)?;

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

            _ => Err(TryFromContainerError::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct SegmentationLayer<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: ContainerHeader<'a>,
    partition: Vector<'a>,
    range_stream: VectorDelta<'a>,
    start_sort: IndexComp<'a>,
    end_sort: IndexComp<'a>,
}

impl<'a> SegmentationLayer<'a> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'a> TryFrom<Container<'a>> for SegmentationLayer<'a> {
    type Error = TryFromContainerError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            ContainerType::SegmentationLayer => {
                let base = get_container_base!(header, SegmentationLayer);

                let partition = check_and_return_component!(components, "Partition", Vector)?;
                if partition.length < 2 || partition.width != 1 {
                    return Err(TryFromContainerError::WrongComponentDimensions("Partition"));
                }

                let range_stream =
                    check_and_return_component!(components, "RangeStream", VectorDelta)?;
                if range_stream.width != 2 {
                    return Err(TryFromContainerError::WrongComponentDimensions(
                        "RangeStream",
                    ));
                }

                let start_sort = check_and_return_component!(components, "StartSort", IndexComp)?;

                let end_sort = check_and_return_component!(components, "EndSort", IndexComp)?;

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
