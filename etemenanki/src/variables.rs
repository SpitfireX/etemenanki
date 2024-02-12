use std::collections::HashSet;
use std::rc::Rc;

use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use crate::components::{self, CachedIndex, CachedInvertedIndex, CachedVector, CompressionType};
use crate::container::{self, Container};
use crate::macros::{check_and_return_component, get_container_base};

#[derive(Debug, EnumAsInner)]
pub enum Variable<'map> {
    IndexedString(IndexedStringVariable<'map>),
    PlainString(PlainStringVariable<'map>),
    Integer(IntegerVariable<'map>),
    Pointer(PointerVariable<'map>),
    ExternalPointer,
    Set(SetVariable<'map>),
    Hash,
}

impl<'map> TryFrom<Container<'map>> for Variable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        match container.header().container_type() {
            container::Type::IndexedStringVariable => Ok(Self::IndexedString(
                IndexedStringVariable::try_from(container)?,
            )),

            container::Type::PlainStringVariable => {
                Ok(Self::PlainString(PlainStringVariable::try_from(container)?))
            }

            container::Type::IntegerVariable => {
                Ok(Self::Integer(IntegerVariable::try_from(container)?))
            }

            container::Type::PointerVariable => {
                Ok(Self::Pointer(PointerVariable::try_from(container)?))
            }

            container::Type::ExternalPointerVariable => todo!(),

            container::Type::SetVariable => Ok(Self::Set(SetVariable::try_from(container)?)),

            container::Type::HashVariable => todo!(),

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct IndexedStringVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: &'map container::RawHeader,
    lexicon: components::StringVector<'map>,
    lex_hash: components::CachedIndex<'map>,
    lex_id_stream: components::CachedVector<'map, 1>,
    lex_id_index: Rc<components::CachedInvertedIndex<'map>>,
}

impl<'map> IndexedStringVariable<'map> {
    pub fn get(&self, index: usize) -> Option<&str> {
        if index < self.lex_id_stream.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> &str {
        let ti = self.lex_id_stream.get_row_unchecked(index)[0];
        &self.lexicon[ti as usize]
    }

    pub fn get_id(&self, index: usize) -> Option<usize> {
        if index < self.lex_id_stream.len() {
            Some(self.get_id_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_id_unchecked(&self, index: usize) -> usize {
        self.lex_id_stream.get_row_unchecked(index)[0] as usize
    }

    pub fn get_range(&self, start: usize, end: usize) -> IndexedStringIterator<'map> {
        IndexedStringIterator {
            lexicon: self.lexicon,
            id_stream: self.lex_id_stream.clone(),
            index: start,
            end,
        }
    }

    pub fn id_stream(&'map self) -> components::CachedVector<'map, 1> {
        self.lex_id_stream.clone()
    }

    pub fn index(&self) -> components::CachedIndex<'map> {
        self.lex_hash.clone()
    }

    pub fn inverted_index(&self) -> Rc<components::CachedInvertedIndex<'map>> {
        self.lex_id_index.clone()
    }

    pub fn iter(&'map self) -> IndexedStringIterator<'map> {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.header.dim1()
    }

    pub fn lexicon(&self) -> &components::StringVector {
        &self.lexicon
    }

    pub fn n_types(&self) -> usize {
        self.header.dim2()
    }
}

impl<'map> TryFrom<Container<'map>> for IndexedStringVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let header = *container.header();

        match header.container_type() {
            container::Type::IndexedStringVariable => {
                let base = crate::macros::get_container_base!(container, PlainStringVariable);
                let n = header.dim1();
                let v = header.dim2();

                let lexicon = check_and_return_component!(container, "Lexicon", StringVector)?;
                if lexicon.len() != v {
                    return Err(Self::Error::WrongComponentDimensions("Lexicon"));
                }

                let lex_hash = check_and_return_component!(container, "LexHash", Index)?;
                if lex_hash.len() != v {
                    return Err(Self::Error::WrongComponentDimensions("LexHash"));
                }
                let lex_hash = CachedIndex::new(lex_hash);

                let lex_id_stream = check_and_return_component!(container, "LexIDStream", Vector)?;
                if lex_id_stream.len() != n || lex_id_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("LexIDStream"));
                }
                let lex_id_stream = CachedVector::<1>::new(lex_id_stream)
                    .expect("width already checked, should be 1");

                let lex_id_index =
                    check_and_return_component!(container, "LexIDIndex", InvertedIndex)?;
                if lex_id_index.n_types() != v {
                    return Err(Self::Error::WrongComponentDimensions("LexIDIndex"));
                }
                let lex_id_index = Rc::new(CachedInvertedIndex::new(lex_id_index));

                let (name, mmap, header, _) = container.into_raw_parts();

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    lexicon,
                    lex_hash,
                    lex_id_stream,
                    lex_id_index,
                })
            }

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}

pub struct IndexedStringIterator<'map> {
    lexicon: components::StringVector<'map>,
    id_stream: components::CachedVector<'map, 1>,
    index: usize,
    end: usize,
}

impl<'map> Iterator for IndexedStringIterator<'map> {
    type Item = &'map str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let lexid = self.id_stream.get_row_unchecked(self.index)[0] as usize;
            self.index += 1;

            Some(&self.lexicon.get_unchecked(lexid))
        } else {
            None
        }
    }
}

impl<'map> IntoIterator for &'map IndexedStringVariable<'map> {
    type Item = &'map str;
    type IntoIter = IndexedStringIterator<'map>;

    fn into_iter(self) -> Self::IntoIter {
        IndexedStringIterator {
            lexicon: self.lexicon,
            id_stream: self.lex_id_stream.clone(),
            index: 0,
            end: self.len(),
        }
    }
}

#[derive(Debug)]
pub struct PlainStringVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: &'map container::RawHeader,
    string_data: components::StringList<'map>,
    offset_stream: components::CachedVector<'map, 1>,
    string_hash: components::CachedIndex<'map>,
}

impl<'map> PlainStringVariable<'map> {
    pub fn get(&self, index: usize) -> Option<&'map str> {
        if index + 1 < self.offset_stream.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> &'map str {
        let start = self.offset_stream.get_row_unchecked(index)[0] as usize;
        let end = self.offset_stream.get_row_unchecked(index + 1)[0] as usize;

        unsafe { std::str::from_utf8_unchecked(&self.string_data.data()[start..end - 1]) }
    }

    pub fn iter(&'map self) -> PlainStringIterator<'map> {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.header.dim1()
    }
}

impl<'map> TryFrom<Container<'map>> for PlainStringVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let header = *container.header();

        match header.container_type() {
            container::Type::PlainStringVariable => {
                let base = get_container_base!(container, PlainStringVariable);
                let n = header.dim1();

                let string_data =
                    check_and_return_component!(container, "StringData", StringList)?;
                if string_data.len() != n {
                    return Err(Self::Error::WrongComponentDimensions("StringData"));
                }

                let offset_stream =
                    check_and_return_component!(container, "OffsetStream", Vector)?;
                if offset_stream.len() != n + 1 || offset_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("OffsetStream"));
                }
                let offset_stream = CachedVector::<1>::new(offset_stream)
                    .expect("width already checked, should be 2");

                let string_hash = check_and_return_component!(container, "StringHash", Index)?;
                if string_hash.len() != n {
                    return Err(Self::Error::WrongComponentDimensions("StringHash"));
                }
                let string_hash = CachedIndex::new(string_hash);

                let (name, mmap, header, _) = container.into_raw_parts();

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    string_data,
                    offset_stream,
                    string_hash,
                })
            }

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}

pub struct PlainStringIterator<'map> {
    string_data: components::StringList<'map>,
    offset_stream: components::CachedVector<'map, 1>,
    len: usize,
    index: usize,
}

impl<'map> Iterator for PlainStringIterator<'map> {
    type Item = &'map str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len {
            let start = self.offset_stream.get_row_unchecked(self.index)[0] as usize;
            let end = self.offset_stream.get_row_unchecked(self.index + 1)[0] as usize;
            self.index += 1;

            Some(unsafe { std::str::from_utf8_unchecked(&self.string_data.data()[start..end - 1]) })
        } else {
            None
        }
    }
}

impl<'map> IntoIterator for &'map PlainStringVariable<'map> {
    type Item = &'map str;
    type IntoIter = PlainStringIterator<'map>;

    fn into_iter(self) -> Self::IntoIter {
        PlainStringIterator {
            string_data: self.string_data,
            offset_stream: self.offset_stream.clone(),
            len: self.len(),
            index: 0,
        }
    }
}

#[derive(Debug)]
pub struct IntegerVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: &'map container::RawHeader,
    int_stream: components::CachedVector<'map, 1>,
    int_sort: components::CachedIndex<'map>,
}

impl<'map> IntegerVariable<'map> {
    pub fn from_iter<I>(iter: I, storage_mode: CompressionType) -> Self where I: Iterator<Item=i64> {
        let container = Container::new_mmap().unwrap();

        

        container.try_into().unwrap()
    }

    pub fn get(&self, index: usize) -> Option<i64> {
        if index < self.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_all(&self, value: i64) -> components::CachedValueIterator<'map> {
        self.int_sort.get_all(value)
    }

    pub fn get_unchecked(&self, index: usize) -> i64 {
        self.int_stream.get_row_unchecked(index)[0]
    }

    pub fn iter(&self) -> IntegerIterator<'map> {
        IntegerIterator {
            int_stream: self.int_stream.clone(),
            index: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.header.dim1()
    }

    pub fn b(&self) -> usize {
        self.header.dim2()
    }
}

impl<'map> TryFrom<Container<'map>> for IntegerVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let header = *container.header();

        match header.container_type() {
            container::Type::IntegerVariable => {
                let base = get_container_base!(container, PlainStringVariable);
                let n = header.dim1();

                let int_stream = check_and_return_component!(container, "IntStream", Vector)?;
                if int_stream.len() != n || int_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("IntStream"));
                }
                let int_stream = CachedVector::<1>::new(int_stream)
                    .expect("width already checked, should be 1");

                let int_sort = check_and_return_component!(container, "IntSort", Index)?;
                if int_sort.len() != n {
                    return Err(Self::Error::WrongComponentDimensions("IntSort"));
                }
                let int_sort = CachedIndex::new(int_sort);

                let (name, mmap, header, _) = container.into_raw_parts();

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    int_stream,
                    int_sort,
                })
            }

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}

pub struct IntegerIterator<'map> {
    int_stream: CachedVector<'map, 1>,
    index: usize,
}

impl<'map> Iterator for IntegerIterator<'map> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.int_stream.len() {
            let value = self.int_stream.get_row_unchecked(self.index)[0];
            self.index += 1;
            Some(value)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct SetVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: &'map container::RawHeader,
    lexicon: components::StringVector<'map>,
    lex_hash: components::CachedIndex<'map>,
    id_set_stream: components::Set<'map>,
    id_set_index: components::CachedInvertedIndex<'map>,
}

impl<'map> SetVariable<'map> {
    pub fn get(&self, index: usize) -> Option<HashSet<&str>> {
        if index < self.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> HashSet<&str> {
        let tids = self.id_set_stream.get_unchecked(index);

        tids.iter()
            .map(|id| *id as usize)
            .map(|id| self.lexicon.get_unchecked(id))
            .collect::<HashSet<&str>>()
    }

    pub fn len(&self) -> usize {
        self.header.dim1()
    }

    pub fn n_types(&self) -> usize {
        self.header.dim2()
    }
}

impl<'map> TryFrom<Container<'map>> for SetVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let header = *container.header();

        match header.container_type() {
            container::Type::SetVariable => {
                let base = get_container_base!(container, PlainStringVariable);
                let n = header.dim1();
                let v = header.dim2();

                let lexicon = check_and_return_component!(container, "Lexicon", StringVector)?;
                if lexicon.len() != v {
                    return Err(Self::Error::WrongComponentDimensions("Lexicon"));
                }

                let lex_hash = check_and_return_component!(container, "LexHash", Index)?;
                if lex_hash.len() != v {
                    return Err(Self::Error::WrongComponentDimensions("LexHash"));
                }
                let lex_hash = CachedIndex::new(lex_hash);

                let id_set_stream = check_and_return_component!(container, "IDSetStream", Set)?;
                if id_set_stream.len() != n ||
                    id_set_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("IDSetStream"));
                }

                let id_set_index =
                    check_and_return_component!(container, "IDSetIndex", InvertedIndex)?;
                if id_set_index.n_types() != v {
                    return Err(Self::Error::WrongComponentDimensions("IDSetIndex"));
                }
                let id_set_index = CachedInvertedIndex::new(id_set_index);

                let (name, mmap, header, _) = container.into_raw_parts();

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    lexicon,
                    lex_hash,
                    id_set_stream,
                    id_set_index,
                })
            }

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}


#[derive(Debug)]
pub struct PointerVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: &'map container::RawHeader,
    head_stream: components::CachedVector<'map, 1>,
    head_sort: components::CachedIndex<'map>,
}

impl<'map> PointerVariable<'map> {
    pub fn get(&self, tail: usize) -> Option<usize> {
        if tail < self.len() {
            self.get_unchecked(tail)
        } else {
            None
        }
    }

    pub fn tail_positions(&self, head: usize) -> Option<components::CachedValueIterator<'map>>{
        if head < self.len() {
            Some(self.head_sort.get_all(head as i64))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> Option<usize> {
        let head = self.head_stream.get_row_unchecked(index)[0];
        if head.is_negative() {
            None
        } else {
            Some(head as usize)
        }
    }

    pub fn len(&self) -> usize {
        self.header.dim1()
    }
}

impl<'map> TryFrom<Container<'map>> for PointerVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let header = *container.header();

        match header.container_type() {
            container::Type::PointerVariable => {
                let base = get_container_base!(container, PlainStringVariable);
                let n = header.dim1();

                let head_stream = check_and_return_component!(container, "HeadStream", Vector)?;
                if head_stream.len() != n || head_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("HeadStream"));
                }
                let head_stream = CachedVector::<1>::new(head_stream)
                    .expect("width already checked, should be 1");

                let head_sort = check_and_return_component!(container, "HeadSort", Index)?;
                if head_sort.len() != n {
                    return Err(Self::Error::WrongComponentDimensions("HeadSort"));
                }
                let head_sort = CachedIndex::new(head_sort);

                let (name, mmap, header, _) = container.into_raw_parts();

                Ok(Self {
                    base,
                    mmap,
                    name,
                    header,
                    head_stream,
                    head_sort,
                })
            }

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}
