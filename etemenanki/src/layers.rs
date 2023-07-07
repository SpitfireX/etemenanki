use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use std::collections::{hash_map, HashMap};
use std::ops;

use crate::container::{self, Container};
use crate::macros::{check_and_return_component, get_container_base};
use crate::variables::Variable;
use crate::{components, variables};

#[derive(Debug, EnumAsInner)]
pub enum Layer<'map> {
    Primary(PrimaryLayer<'map>, LayerVariables<'map>),
    Segmentation(SegmentationLayer<'map>, LayerVariables<'map>),
}

impl<'map> Layer<'map> {
    pub fn add_variable(&mut self, name: String, var: Variable<'map>) -> Result<(), Variable> {
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
                Self::Primary(_, vars) => vars.add_variable(name, var),
                Self::Segmentation(_, vars) => vars.add_variable(name, var),
            }
        }
    }

    pub fn variable_by_name<S: AsRef<str>>(&self, name: S) -> Option<&variables::Variable> {
        match self {
            Layer::Primary(_, vars) => vars.variables.get(name.as_ref()),
            Layer::Segmentation(_, vars) => vars.variables.get(name.as_ref()),
        }
    }

    pub fn init_primary(layer: PrimaryLayer<'map>) -> Self {
        Self::Primary(layer, LayerVariables::default())
    }

    pub fn init_segmentation(layer: SegmentationLayer<'map>) -> Self {
        Self::Segmentation(layer, LayerVariables::default())
    }

    pub fn len(&self) -> usize {
        match &self {
            Self::Primary(l, _) => l.len(),
            Self::Segmentation(l, _) => l.len(),
        }
    }

    pub fn variable_names(&self) -> hash_map::Keys<String, variables::Variable> {
        match self {
            Layer::Primary(_, vars) => vars.variables.keys(),
            Layer::Segmentation(_, vars) => vars.variables.keys(),
        }
    }
}

impl<'map, S: AsRef<str>> ops::Index<S> for Layer<'map> {
    type Output = variables::Variable<'map>;

    fn index(&self, index: S) -> &Self::Output {
        match self {
            Layer::Primary(_, vars) => &vars.variables[index.as_ref()],
            Layer::Segmentation(_, vars) => &vars.variables[index.as_ref()],
        }
    }
}

#[derive(Debug, Default)]
pub struct LayerVariables<'map> {
    pub variables: HashMap<String, Variable<'map>>,
}

impl<'map> LayerVariables<'map> {
    pub fn add_variable(&mut self, name: String, var: Variable<'map>) -> Result<(), Variable> {
        if self.variables.contains_key(&name) {
            Err(var)
        } else {
            self.variables.insert(name, var);
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct PrimaryLayer<'map> {
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'map>,
    partition: components::Vector<'map>,
}

impl<'map> PrimaryLayer<'map> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'map> TryFrom<Container<'map>> for PrimaryLayer<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            container::Type::PrimaryLayer => {
                let partition = check_and_return_component!(components, "Partition", Vector)?;

                if partition.len() < 2 || partition.width() != 1 {
                    Err(Self::Error::WrongComponentDimensions("Partition"))
                } else {
                    Ok(Self {
                        mmap,
                        name,
                        header,
                        partition,
                    })
                }
            }

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct SegmentationLayer<'map> {
    pub base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'map>,
    partition: components::Vector<'map>,
    range_stream: components::Vector<'map>,
    start_sort: components::Index<'map>,
    end_sort: components::Index<'map>,
}

impl<'map> SegmentationLayer<'map> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'map> TryFrom<Container<'map>> for SegmentationLayer<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            container::Type::SegmentationLayer => {
                let base = get_container_base!(header, SegmentationLayer);

                let partition = check_and_return_component!(components, "Partition", Vector)?;
                if partition.len() < 2 || partition.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("Partition"));
                }

                let range_stream =
                    check_and_return_component!(components, "RangeStream", Vector)?;
                if range_stream.width() != 2 {
                    return Err(Self::Error::WrongComponentDimensions("RangeStream"));
                }

                let start_sort = check_and_return_component!(components, "StartSort", Index)?;

                let end_sort = check_and_return_component!(components, "EndSort", Index)?;

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

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}
