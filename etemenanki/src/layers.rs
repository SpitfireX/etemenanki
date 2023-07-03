use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use std::collections::HashMap;

use crate::components;
use crate::container::{self, Container};
use crate::macros::{check_and_return_component, get_container_base};
use crate::variables::Variable;

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
                Self::Primary(_, vars) => vars.add_variable(name, var),
                Self::Segmentation(_, vars) => vars.add_variable(name, var),
            }
        }
    }

    pub fn init_primary(layer: PrimaryLayer<'a>) -> Self {
        Self::Primary(layer, LayerVariables::default())
    }

    pub fn init_segmentation(layer: SegmentationLayer<'a>) -> Self {
        Self::Segmentation(layer, LayerVariables::default())
    }

    pub fn len(&self) -> usize {
        match &self {
            Self::Primary(l, _) => l.len(),
            Self::Segmentation(l, _) => l.len(),
        }
    }
}

#[derive(Debug, Default)]
pub struct LayerVariables<'a> {
    pub variables: HashMap<String, Variable<'a>>,
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
}

#[derive(Debug)]
pub struct PrimaryLayer<'a> {
    pub(crate) mmap: Mmap,
    pub name: String,
    pub header: container::Header<'a>,
    pub(crate) partition: components::Vector<'a>,
}

impl<'a> PrimaryLayer<'a> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'a> TryFrom<Container<'a>> for PrimaryLayer<'a> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            container::Type::PrimaryLayer => {
                let partition = check_and_return_component!(components, "Partition", Vector)?;

                if partition.length < 2 || partition.width != 1 {
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
pub struct SegmentationLayer<'a> {
    pub(crate) base: Uuid,
    pub(crate) mmap: Mmap,
    pub name: String,
    pub header: container::Header<'a>,
    pub(crate) partition: components::Vector<'a>,
    pub(crate) range_stream: components::VectorDelta<'a>,
    pub(crate) start_sort: components::IndexComp<'a>,
    pub(crate) end_sort: components::IndexComp<'a>,
}

impl<'a> SegmentationLayer<'a> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'a> TryFrom<Container<'a>> for SegmentationLayer<'a> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
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
                if partition.length < 2 || partition.width != 1 {
                    return Err(Self::Error::WrongComponentDimensions("Partition"));
                }

                let range_stream =
                    check_and_return_component!(components, "RangeStream", VectorDelta)?;
                if range_stream.width != 2 {
                    return Err(Self::Error::WrongComponentDimensions("RangeStream"));
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

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}
