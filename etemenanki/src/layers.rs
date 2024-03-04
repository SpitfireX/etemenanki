use enum_as_inner::EnumAsInner;
use memmap2::{Mmap, MmapOptions};
use uuid::Uuid;

use std::collections::{hash_map, HashMap};
use std::fs::File;
use std::ops;

use crate::components::{CachedIndex, CachedVector, Component, Index, Vector};
use crate::container::{self, Container, ContainerBuilder};
use crate::macros::{check_and_return_component, get_container_base};
use crate::variables::Variable;
use crate::{components, variables};

#[derive(Debug)]
pub struct LayerData<'map, T>(T, LayerVariables<'map>);

impl<'map, T> LayerData<'map, T> {
    pub fn variable_by_name<S: AsRef<str>>(&self, name: S) -> Option<&variables::Variable<'map>> {
        self.1.variables.get(name.as_ref())
    }
}

impl<'map, T> ops::Deref for LayerData<'map, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'map, T, S: AsRef<str>> ops::Index<S> for LayerData<'map, T> {
    type Output = variables::Variable<'map>;

    fn index(&self, index: S) -> &Self::Output {
        &self.1.variables[index.as_ref()]
    }
}

#[derive(Debug, EnumAsInner)]
pub enum Layer<'map> {
    Primary(LayerData<'map, PrimaryLayer<'map>>),
    Segmentation(LayerData<'map, SegmentationLayer<'map>>),
}

impl<'map> Layer<'map> {
    pub fn add_variable(&mut self, name: String, var: Variable<'map>) -> Result<(), Variable<'map>> {
        let varlen = match &var {
            Variable::IndexedString(v) => v.len(),
            Variable::PlainString(v) => v.len(),
            Variable::Integer(v) => v.len(),
            Variable::Pointer(v) => v.len(),
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

    pub fn variable_by_name<S: AsRef<str>>(&self, name: S) -> Option<&variables::Variable<'map>> {
        match self {
            Layer::Primary(LayerData(_, vars)) => vars.variables.get(name.as_ref()),
            Layer::Segmentation(LayerData(_, vars)) => vars.variables.get(name.as_ref()),
        }
    }

    pub fn new_primary(layer: PrimaryLayer<'map>) -> Self {
        Self::Primary(LayerData(layer, LayerVariables::default()))
    }

    pub fn new_segmentation(layer: SegmentationLayer<'map>) -> Self {
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

    pub fn variable_names(&self) -> hash_map::Keys<String, variables::Variable<'map>> {
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
    pub fn add_variable(&mut self, name: String, var: Variable<'map>) -> Result<(), Variable<'map>> {
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
    pub header: &'map container::Header,
}

impl<'map> PrimaryLayer<'map> {
    #[inline]
    pub fn len(&self) -> usize {
        self.header.dim1()
    }
}

impl<'map> TryFrom<Container<'map>> for PrimaryLayer<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let header = *container.header();

        match header.container_type() {
            container::Type::PrimaryLayer => {
                let (name, mmap, header, _) = container.into_raw_parts();
                Ok(Self {
                    mmap,
                    name,
                    header,
                })
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
    pub header: &'map container::Header,
    range_stream: components::CachedVector<'map, 2>,
    start_sort: components::CachedIndex<'map>,
    end_sort: components::CachedIndex<'map>,
}

impl<'map> SegmentationLayer<'map> {
    pub fn contains(&self, range: (usize, usize)) -> bool {
        let (start, end) = range;

        match self.start_sort.get_first(start as i64) {
            None => false,
            Some(i) => end == self.get_unchecked(i as usize).1,
        }
    }

    pub fn contains_end(&self, end: usize) -> bool {
        self.end_sort.contains_key(end as i64)
    }

    pub fn contains_start(&self, start: usize) -> bool {
        self.start_sort.contains_key(start as i64)
    }

    /// Finds the index of the range containing baselayer position `position`
    pub fn find_containing(&self, position: usize) -> Option<usize> {
        let i = match &self.start_sort {

            components::CachedIndex::Compressed { length: _, cache } => {
                let mut cache = cache.borrow_mut();

                let bi = cache.sync_block_position(position as i64);
                let block = cache.get_block(bi).unwrap();

                let vi = match block.keys().binary_search(&(position as i64)) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i-1,
                };

                block.get_position(vi).expect("vi must be in block")
            }
            
            components::CachedIndex::Uncompressed { length: _, pairs } => {
                let i = match pairs.binary_search_by_key(&(position as i64), |(s, _)| *s) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i-1,
                };

                pairs[i].1
            }
        };

        if (i as usize) < self.len() {
            let (start, end) = self.get_unchecked(i as usize);

            if position >= start && end > position {
                return Some(i as usize)
            }
        }
        None
    }

    pub fn get(&self, index: usize) -> Option<(usize, usize)> {
        if index < self.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> (usize, usize) {
        let row = self.range_stream.get_row_unchecked(index);
        (row[0] as usize, row[1] as usize)
    }

    pub fn iter(&self) -> SegmentationLayerIterator<'map> {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.header.dim1()
    }

    pub fn encode_to_file<I>(file: File, values: I, n: usize, name: String, base: Uuid, compressed: bool, comment: &str) -> Self where I: Iterator<Item=(usize, usize)> {
        let vectype = if compressed { components::Type::VectorDelta } else { components::Type::Vector };
        let idxtype = if compressed { components::Type::IndexComp } else { components::Type::Index };
        
        let mut builder = ContainerBuilder::new_into_file(name, file, 3)
            .edit_header(| h | {
                h.comment(comment)
                    .ziggurat_type(container::Type::SegmentationLayer)
                    .dim1(n)
                    .dim2(0)
                    .base1(Some(base));
            })
            .add_component("RangeStream", vectype, | bom_entry, file | {
                unsafe {
                    if compressed {
                        let values = values.map(|(s, e)| [s as i64, e as i64]);
                        Vector::encode_delta_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
                    } else {
                        let values = values.map(|(s, e)| [s as i64, e as i64]).flatten();
                        Vector::encode_uncompressed_to_container_file(values, n, 1, file, bom_entry, bom_entry.offset as u64);
                    }
                }
            });

        let vecbom = *builder.get_component(0);
        let vecmmap = unsafe { MmapOptions::new()
            .offset(vecbom.offset as u64)
            .len(vecbom.size as usize)
            .map(builder.file())
            .unwrap()
        };

        let range_stream = Component::from_raw_parts(&vecbom, vecmmap.as_ptr()).unwrap().into_vector().unwrap();
        let range_stream = CachedVector::<2>::new(range_stream).unwrap();

        builder = builder.add_component("StartSort", idxtype, | bom_entry, file | {
            unsafe {
                let values = range_stream.iter().map(|[s, e]| (s, e));
                if compressed {
                    Index::encode_compressed_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
                } else {
                    Index::encode_uncompressed_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
                }
            }
        });

        builder = builder.add_component("EndSort", idxtype, | bom_entry, file | {
            unsafe {
                let values = range_stream.iter().map(|[s, e]| (e, s));
                if compressed {
                    Index::encode_compressed_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
                } else {
                    Index::encode_uncompressed_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
                }
            }
        });

        builder.build().try_into().expect("SegmentationLayer returned by its constructor is inconsistent")
    }
}

impl<'map> TryFrom<Container<'map>> for SegmentationLayer<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let header = *container.header();
        match header.container_type() {
            container::Type::SegmentationLayer => {
                let base = get_container_base!(container, SegmentationLayer);

                let range_stream =
                    check_and_return_component!(container, "RangeStream", Vector)?;
                if range_stream.width() != 2 || range_stream.len() != header.dim1() {
                    return Err(Self::Error::WrongComponentDimensions("RangeStream"));
                }
                let range_stream = CachedVector::<2>::new(range_stream)
                    .expect("width already checked, should be 2");

                let start_sort = check_and_return_component!(container, "StartSort", Index)?;
                if start_sort.len() != header.dim1() {
                    return Err(Self::Error::WrongComponentDimensions("StartSort"));
                }
                let start_sort = CachedIndex::new(start_sort);

                let end_sort = check_and_return_component!(container, "EndSort", Index)?;
                if end_sort.len() != header.dim1() {
                    return Err(Self::Error::WrongComponentDimensions("EndSort"));
                }
                let end_sort = CachedIndex::new(end_sort);

                let (name, mmap, header, _) = container.into_raw_parts();

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
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
    ranges: components::RowIterator<'map, 2>,
}

impl<'map> Iterator for SegmentationLayerIterator<'map> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        self.ranges.next()
            .map(| [start, end] | (start as usize, end as usize))
    }
}

impl<'a, 'map> IntoIterator for &'a SegmentationLayer<'map> {
    type Item = (usize, usize);
    type IntoIter = SegmentationLayerIterator<'map>;

    fn into_iter(self) -> Self::IntoIter {
        SegmentationLayerIterator {
            ranges: self.range_stream.iter()
        }
    }
}
