use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use std::collections::{hash_map, HashMap};
use std::ops;

use crate::container::{self, Container};
use crate::macros::{check_and_return_component, get_container_base};
use crate::variables::Variable;
use crate::{components, variables};

#[derive(Debug)]
pub struct LayerData<'map, T>(T, LayerVariables<'map>);

impl<'map, T> ops::Deref for LayerData<'map, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, EnumAsInner)]
pub enum Layer<'map> {
    Primary(LayerData<'map, PrimaryLayer<'map>>),
    Segmentation(LayerData<'map, SegmentationLayer<'map>>),
}

impl<'map> Layer<'map> {
    pub fn add_variable(&mut self, name: String, var: Variable<'map>) -> Result<(), Variable> {
        let varlen = match &var {
            Variable::IndexedString(v) => v.len(),
            Variable::PlainString(v) => v.len(),
            Variable::Integer(v) => v.len(),
            Variable::Pointer => todo!(),
            Variable::ExternalPointer => todo!(),
            Variable::Set(v) => v.len(),
            Variable::Hash => todo!(),
        };

        if varlen != self.len() {
            Err(var)
        } else {
            match self {
                Self::Primary(LayerData(_, vars)) => vars.add_variable(name, var),
                Self::Segmentation(LayerData(_, vars)) => vars.add_variable(name, var),
            }
        }
    }

    pub fn variable_by_name<S: AsRef<str>>(&self, name: S) -> Option<&variables::Variable> {
        match self {
            Layer::Primary(LayerData(_, vars)) => vars.variables.get(name.as_ref()),
            Layer::Segmentation(LayerData(_, vars)) => vars.variables.get(name.as_ref()),
        }
    }

    pub fn init_primary(layer: PrimaryLayer<'map>) -> Self {
        Self::Primary(LayerData(layer, LayerVariables::default()))
    }

    pub fn init_segmentation(layer: SegmentationLayer<'map>) -> Self {
        Self::Segmentation(LayerData(layer, LayerVariables::default()))
    }

    pub fn len(&self) -> usize {
        match &self {
            Self::Primary(LayerData(l, _)) => l.len(),
            Self::Segmentation(LayerData(l, _)) => l.len(),
        }
    }

    pub fn variable_len(&self) -> usize {
        match &self {
            Self::Primary(LayerData(_, var)) => var.len(),
            Self::Segmentation(LayerData(_, var)) => var.len(),
        }
    }

    pub fn variable_names(&self) -> hash_map::Keys<String, variables::Variable> {
        match self {
            Layer::Primary(LayerData(_, vars)) => vars.variables.keys(),
            Layer::Segmentation(LayerData(_, vars)) => vars.variables.keys(),
        }
    }
}

impl<'map, S: AsRef<str>> ops::Index<S> for Layer<'map> {
    type Output = variables::Variable<'map>;

    fn index(&self, index: S) -> &Self::Output {
        match self {
            Layer::Primary(LayerData(_, vars)) => &vars.variables[index.as_ref()],
            Layer::Segmentation(LayerData(_, vars)) => &vars.variables[index.as_ref()],
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

    pub fn len(&self) -> usize {
        self.variables.len()
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
    #[inline]
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
    #[inline]
    pub fn get(&self, index: usize) -> (usize, usize) {
        let row = self.range_stream.get_row(index);
        (row[0] as usize, row[1] as usize)
    }

    #[inline]
    pub fn iter(&self) -> SegmentationLayerIterator {
        self.into_iter()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.header.dim1
    }

    pub fn end_index(&self) -> components::Index {
        self.end_sort
    }

    pub fn start_index(&self) -> components::Index {
        self.start_sort
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

pub struct SegmentationLayerIterator<'map> {
    ranges: components::VectorReader<'map>,
    index: usize,
}

impl<'map> Iterator for SegmentationLayerIterator<'map> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.ranges.len() {
            let row = self.ranges.get_row(self.index);
            self.index += 1;
            Some((row[0] as usize, row[1] as usize))
        } else {
            None
        }
    }
}

impl<'map> IntoIterator for &'map SegmentationLayer<'map> {
    type Item = (usize, usize);
    type IntoIter = SegmentationLayerIterator<'map>;

    fn into_iter(self) -> Self::IntoIter {
        SegmentationLayerIterator {
            ranges: self.range_stream.into_iter(),
            index: 0,
        }
    }
}
