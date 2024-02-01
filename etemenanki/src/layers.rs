use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use std::cell::RefCell;
use std::collections::{hash_map, HashMap};
use std::ops;
use std::rc::Rc;

use crate::components::{CachedIndex, CachedVector};
use crate::container::{self, Container};
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
    pub header: container::Header<'map>,
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
            components: _,
        } = container;

        match header.container_type {
            container::Type::PrimaryLayer => {
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
    pub header: container::Header<'map>,
    range_stream: Rc<RefCell<components::CachedVector<'map>>>,
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
        let mut range_stream = self.range_stream.borrow_mut();
        let row = range_stream.get_row_unchecked(index);
        (row[0] as usize, row[1] as usize)
    }

    pub fn iter(&'map self) -> SegmentationLayerIterator<'map> {
        self.into_iter()
    }

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

                let range_stream =
                    check_and_return_component!(components, "RangeStream", Vector)?;
                if range_stream.width() != 2 || range_stream.len() != header.dim1 {
                    return Err(Self::Error::WrongComponentDimensions("RangeStream"));
                }
                let range_stream = Rc::new(RefCell::new(CachedVector::new(range_stream)));

                let start_sort = check_and_return_component!(components, "StartSort", Index)?;
                if start_sort.len() != header.dim1 {
                    return Err(Self::Error::WrongComponentDimensions("StartSort"));
                }
                let start_sort = CachedIndex::new(start_sort);

                let end_sort = check_and_return_component!(components, "EndSort", Index)?;
                if end_sort.len() != header.dim1 {
                    return Err(Self::Error::WrongComponentDimensions("EndSort"));
                }
                let end_sort = CachedIndex::new(end_sort);

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
    ranges: Rc<RefCell<components::CachedVector<'map>>>,
    index: usize,
}

impl<'map> Iterator for SegmentationLayerIterator<'map> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let mut ranges = self.ranges.borrow_mut();
        if self.index < ranges.len() {
            let row = ranges.get_row_unchecked(self.index);
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
            ranges: self.range_stream.clone(),
            index: 0,
        }
    }
}
