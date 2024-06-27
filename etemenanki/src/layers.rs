use enum_as_inner::EnumAsInner;
use memmap2::{Mmap, MmapOptions};
use uuid::Uuid;

use std::collections::{hash_map, HashMap};
use std::fs::File;
use std::ops;

use crate::components::{CachedVector, Component, Index, Vector};
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
    start_sort: components::Index<'map>,
    end_sort: components::Index<'map>,
}

impl<'map> SegmentationLayer<'map> {
    pub fn contains(&self, range: (usize, usize)) -> bool {
        let start = range.0 as i64;
        let end = range.1 as i64;

        match self.start_sort {
            Index::Compressed { .. } => panic!("StartSort should not be compressed"),

            Index::Uncompressed { length: _, pairs } => {
                // get block
                let bi = match pairs.binary_search_by_key(&start, |(s, _)| *s) {
                    Ok(bi) => bi,
                    Err(0) => return false,
                    Err(bi) => bi - 1,
                };

                // if a plausible block exists, decode it and do another binary search
                let bcache = self.range_stream.get_block_cache().unwrap();
                let mut bcref = bcache.borrow_mut();

                let block = bcref.get_block(bi).unwrap();

                match block.rows().binary_search_by_key(&start, |[s, _]| *s) {
                    Ok(i) => block.get_row_unchecked(i)[1] == end,
                    Err(_) => false,
                }
            }
        }
    }

    pub fn contains_end(&self, end: usize) -> bool {
        let end = end as i64;

        match self.end_sort {
            Index::Compressed { .. } => panic!("EndSort should not be compressed"),

            Index::Uncompressed { length: _, pairs } => {
                // check the index for a direct hit
                let bi = match pairs.binary_search_by_key(&end, |(e, _)| *e) {
                    Ok(_) => return true,
                    Err(0) => 0,
                    Err(bi) => bi - 1,
                };

                // if a plausible block exists, decode it and do another binary search
                let bcache = self.range_stream.get_block_cache().unwrap();
                let mut bcref = bcache.borrow_mut();

                let block = bcref.get_block(bi).unwrap();

                match block.rows().binary_search_by_key(&end, |[_, e]| *e) {
                    Ok(_) => true,
                    Err(_) => false,
                }
            }
        }
    }

    pub fn contains_start(&self, start: usize) -> bool {
        let start = start as i64;

        match self.start_sort {
            Index::Compressed { .. } => panic!("StartSort should not be compressed"),

            Index::Uncompressed { length: _, pairs } => {
                // check the index for a direct hit
                let bi = match pairs.binary_search_by_key(&start, |(s, _)| *s) {
                    Ok(_) => return true,
                    Err(0) => return false,
                    Err(bi) => bi - 1,
                };

                // if a plausible block exists, decode it and do another binary search
                let bcache = self.range_stream.get_block_cache().unwrap();
                let mut bcref = bcache.borrow_mut();

                let block = bcref.get_block(bi).unwrap();

                match block.rows().binary_search_by_key(&start, |[s, _]| *s) {
                    Ok(_) => true,
                    Err(_) => false,
                }
            }
        }
    }

    /// Finds the index of the range containing baselayer position `position`
    pub fn find_containing(&self, position: usize) -> Option<usize> {
        let position = position as i64;

        match self.start_sort {
            Index::Compressed { .. } => panic!("StartSort should not be compressed"),

            Index::Uncompressed { length: _, pairs } => {
                // check the index for a direct hit
                let bi = match pairs.binary_search_by_key(&position, |(s, _)| *s) {
                    Ok(bi) => return Some(bi * 16),
                    Err(0) => return None,
                    Err(bi) => bi - 1,
                };

                // if a plausible block exists, decode it and do another binary search
                let bcache = self.range_stream.get_block_cache().unwrap();
                let mut bcref = bcache.borrow_mut();

                let block = bcref.get_block(bi).unwrap();

                match block.rows().binary_search_by_key(&position, |[s, _]| *s) {
                    Ok(i) => Some((bi * 16) + i),
                    Err(0) => None,
                    Err(i) => {
                        let i = i-1;
                        let [start, end] = block.get_row_unchecked(i);
                        if position >= start && position < end {
                            Some((bi * 16) + i)
                        } else {
                            None
                        }
                    }
                }
            }
        }
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
        if !compressed {
            eprintln!("Warning: Uncompressed SegmentationLayers are impossible, layer will be compressed");
        }
        
        let mut builder = ContainerBuilder::new_into_file(name, file, 3)
            .edit_header(| h | {
                h.comment(comment)
                    .ziggurat_type(container::Type::SegmentationLayer)
                    .dim1(n)
                    .dim2(0)
                    .base1(Some(base));
            })
            .add_component("RangeStream", components::Type::VectorDelta, | bom_entry, file | {
                unsafe {
                    let values = values.map(|(s, e)| [s as i64, e as i64]);
                    Vector::encode_delta_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
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

        let block_cache = range_stream.get_block_cache().unwrap();
        let mut bcref = block_cache.borrow_mut();

        let n_blocks = bcref.n_blocks();

        builder = builder.add_component("StartSort", components::Type::Index, | bom_entry, file | {
            unsafe {
                let values = (0..n_blocks)
                    .map(| bi | {
                        let b = bcref.get_block(bi).unwrap();
                        // tuple of start boundary of first segment in block and the block index
                        (b.first()[0], bi as i64)
                    });

                Index::encode_uncompressed_to_container_file(values, n_blocks, file, bom_entry, bom_entry.offset as u64);
            }
        });

        builder = builder.add_component("EndSort", components::Type::Index, | bom_entry, file | {
            unsafe {
                let values = (0..n_blocks)
                    .map(| bi | {
                        let block = bcref.get_block(bi).unwrap();
                        // tuple of end boundary of last segment in block and the block index
                        (block.last()[1], bi as i64)
                    });

                Index::encode_uncompressed_to_container_file(values, n_blocks, file, bom_entry, bom_entry.offset as u64);
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

                let n_blocks = range_stream.get_block_cache().unwrap().borrow().n_blocks();

                let start_sort = check_and_return_component!(container, "StartSort", Index)?;
                if start_sort.len() != n_blocks {
                    return Err(Self::Error::WrongComponentDimensions("StartSort"));
                }

                let end_sort = check_and_return_component!(container, "EndSort", Index)?;
                if end_sort.len() != n_blocks {
                    return Err(Self::Error::WrongComponentDimensions("EndSort"));
                }

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
