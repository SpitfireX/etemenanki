use std::cell::RefCell;
use std::collections::HashSet;
use std::ops;
use std::rc::Rc;

use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use crate::components::{self, CachedVector};
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
        match container.header.container_type {
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
    pub header: container::Header<'map>,
    lexicon: components::StringVector<'map>,
    lex_hash: components::Index<'map>,
    lex_id_stream: Rc<RefCell<components::CachedVector<'map>>>,
    lex_id_index: components::InvertedIndex<'map>,
}

impl<'map> IndexedStringVariable<'map> {
    pub fn get(&self, index: usize) -> Option<&str> {
        let lex_id_stream = self.lex_id_stream.borrow();
        if index < lex_id_stream.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> &str {
        let mut lex_id_stream = self.lex_id_stream.borrow_mut();
        let ti = lex_id_stream.get_row_unchecked(index)[0];
        &self.lexicon[ti as usize]
    }

    pub fn get_id(&self, index: usize) -> Option<usize> {
        let lex_id_stream = self.lex_id_stream.borrow();
        if index < lex_id_stream.len() {
            Some(self.get_id_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_id_unchecked(&self, index: usize) -> usize {
        let mut lex_id_stream = self.lex_id_stream.borrow_mut();
        lex_id_stream.get_row_unchecked(index)[0] as usize
    }

    pub fn get_range(&'map self, start: usize, end: usize) -> IndexedStringIterator<'map> {
        IndexedStringIterator {
            var: self,
            id_stream: self.lex_id_stream.clone(),
            index: start,
            end,
        }
    }

    pub fn id_stream(&'map self) -> Rc<RefCell<components::CachedVector<'map>>> {
        self.lex_id_stream.clone()
    }

    pub fn index(&self) -> components::Index {
        self.lex_hash
    }

    pub fn inverted_index(&self) -> components::InvertedIndex {
        self.lex_id_index
    }

    pub fn iter(&'map self) -> IndexedStringIterator<'map> {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.header.dim1
    }

    pub fn lexicon(&self) -> &components::StringVector {
        &self.lexicon
    }

    pub fn n_types(&self) -> usize {
        self.header.dim2
    }
}

impl<'map> ops::Index<usize> for IndexedStringVariable<'map> {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        self.get_unchecked(index)
    }
}

impl<'map> TryFrom<Container<'map>> for IndexedStringVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            container::Type::IndexedStringVariable => {
                let base = crate::macros::get_container_base!(header, PlainStringVariable);
                let n = header.dim1;
                let v = header.dim2;

                let lexicon = check_and_return_component!(components, "Lexicon", StringVector)?;
                if lexicon.len() != v {
                    return Err(Self::Error::WrongComponentDimensions("Lexicon"));
                }

                let lex_hash = check_and_return_component!(components, "LexHash", Index)?;
                if lex_hash.len() != v {
                    return Err(Self::Error::WrongComponentDimensions("LexHash"));
                }

                let lex_id_stream = check_and_return_component!(components, "LexIDStream", Vector)?;
                if lex_id_stream.len() != n || lex_id_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("LexIDStream"));
                }
                let lex_id_stream = Rc::new(RefCell::new(CachedVector::new(lex_id_stream)));

                let lex_id_index =
                    check_and_return_component!(components, "LexIDIndex", InvertedIndex)?;
                if lex_id_index.n_types() != v {
                    return Err(Self::Error::WrongComponentDimensions("LexIDIndex"));
                }

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
    var: &'map IndexedStringVariable<'map>,
    id_stream: Rc<RefCell<components::CachedVector<'map>>>,
    index: usize,
    end: usize,
}

impl<'map> Iterator for IndexedStringIterator<'map> {
    type Item = &'map str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let mut id_stream = self.id_stream.borrow_mut();
            let lexid = id_stream.get_row_unchecked(self.index)[0] as usize;
            self.index += 1;

            Some(&self.var.lexicon[lexid])
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
            var: self,
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
    pub header: container::Header<'map>,
    string_data: components::StringList<'map>,
    offset_stream: Rc<RefCell<components::CachedVector<'map>>>,
    string_hash: components::Index<'map>,
}

impl<'map> PlainStringVariable<'map> {
    pub fn get(&self, index: usize) -> Option<&str> {
        let offset_stream = self.offset_stream.borrow();
        if index + 1 < offset_stream.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> &str {
        let mut offset_stream = self.offset_stream.borrow_mut();
        let start = offset_stream.get_row_unchecked(index)[0] as usize;
        let end = offset_stream.get_row_unchecked(index + 1)[0] as usize;

        unsafe { std::str::from_utf8_unchecked(&self.string_data[start..end - 1]) }
    }

    pub fn iter(&'map self) -> PlainStringIterator<'map> {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'map> ops::Index<usize> for PlainStringVariable<'map> {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        self.get_unchecked(index)
    }
}

impl<'map> TryFrom<Container<'map>> for PlainStringVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            container::Type::PlainStringVariable => {
                let base = get_container_base!(header, PlainStringVariable);
                let n = header.dim1;

                let string_data =
                    check_and_return_component!(components, "StringData", StringList)?;
                if string_data.len() != n {
                    return Err(Self::Error::WrongComponentDimensions("StringData"));
                }

                let offset_stream =
                    check_and_return_component!(components, "OffsetStream", Vector)?;
                if offset_stream.len() != n + 1 || offset_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("OffsetStream"));
                }
                let offset_stream = Rc::new(RefCell::new(CachedVector::new(offset_stream)));

                let string_hash = check_and_return_component!(components, "StringHash", Index)?;
                if string_hash.len() != n {
                    return Err(Self::Error::WrongComponentDimensions("StringHash"));
                }

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
    var: &'map PlainStringVariable<'map>,
    offset_stream: Rc<RefCell<components::CachedVector<'map>>>,
    len: usize,
    index: usize,
}

impl<'map> Iterator for PlainStringIterator<'map> {
    type Item = &'map str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len {
            let mut offset_stream = self.offset_stream.borrow_mut();
            let start = offset_stream.get_row_unchecked(self.index)[0] as usize;
            let end = offset_stream.get_row_unchecked(self.index + 1)[0] as usize;
            self.index += 1;

            Some(unsafe { std::str::from_utf8_unchecked(&self.var.string_data[start..end - 1]) })
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
            var: self,
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
    pub header: container::Header<'map>,
    int_stream: Rc<RefCell<components::CachedVector<'map>>>,
    int_sort: components::Index<'map>,
}

impl<'map> IntegerVariable<'map> {
    pub fn get(&self, index: usize) -> Option<i64> {
        if index < self.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_all(&self, value: i64) -> components::IndexIterator {
        self.int_sort.get_all(value)
    }

    pub fn get_unchecked(&self, index: usize) -> i64 {
        let mut int_stream = self.int_stream.borrow_mut();
        int_stream.get_row_unchecked(index)[0]
    }

    pub fn iter(&'map self) -> IntegerIterator<'map> {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.header.dim1
    }

    pub fn b(&self) -> usize {
        self.header.dim2
    }
}

impl<'map> TryFrom<Container<'map>> for IntegerVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            container::Type::IntegerVariable => {
                let base = get_container_base!(header, PlainStringVariable);
                let n = header.dim1;

                let int_stream = check_and_return_component!(components, "IntStream", Vector)?;
                if int_stream.len() != n || int_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("IntStream"));
                }
                let int_stream = Rc::new(RefCell::new(CachedVector::new(int_stream)));

                let int_sort = check_and_return_component!(components, "IntSort", Index)?;
                if int_sort.len() != n {
                    return Err(Self::Error::WrongComponentDimensions("IntSort"));
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

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}

pub struct IntegerIterator<'map> {
    int_stream: Rc<RefCell<CachedVector<'map>>>,
    index: usize,
}

impl<'map> Iterator for IntegerIterator<'map> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        let mut int_stream = self.int_stream.borrow_mut();
        if self.index < int_stream.len() {
            let value = int_stream.get_row_unchecked(self.index)[0];
            self.index += 1;
            Some(value)
        } else {
            None
        }
    }
}

impl<'map> IntoIterator for &'map IntegerVariable<'map> {
    type Item = i64;
    type IntoIter = IntegerIterator<'map>;

    fn into_iter(self) -> Self::IntoIter {
        IntegerIterator {
            int_stream: self.int_stream.clone(),
            index: 0,
        }
    }
}

#[derive(Debug)]
pub struct SetVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'map>,
    lexicon: components::StringVector<'map>,
    lex_hash: components::Index<'map>,
    id_set_stream: components::Set<'map>,
    id_set_index: components::InvertedIndex<'map>,
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
        self.header.dim1
    }

    pub fn n_types(&self) -> usize {
        self.header.dim2
    }
}

impl<'map> TryFrom<Container<'map>> for SetVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            container::Type::SetVariable => {
                let base = get_container_base!(header, PlainStringVariable);
                let n = header.dim1;
                let v = header.dim2;

                let lexicon = check_and_return_component!(components, "Lexicon", StringVector)?;
                if lexicon.len() != v {
                    return Err(Self::Error::WrongComponentDimensions("Lexicon"));
                }

                let lex_hash = check_and_return_component!(components, "LexHash", Index)?;
                if lex_hash.len() != v {
                    return Err(Self::Error::WrongComponentDimensions("LexHash"));
                }

                let id_set_stream = check_and_return_component!(components, "IDSetStream", Set)?;
                if id_set_stream.len() != n ||
                    id_set_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("IDSetStream"));
                }

                let id_set_index =
                    check_and_return_component!(components, "IDSetIndex", InvertedIndex)?;
                if id_set_index.n_types() != v {
                    return Err(Self::Error::WrongComponentDimensions("IDSetIndex"));
                }

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
    pub header: container::Header<'map>,
    head_stream: Rc<RefCell<components::CachedVector<'map>>>,
    head_sort: components::Index<'map>,
}

impl<'map> PointerVariable<'map> {
    pub fn get(&self, tail: usize) -> Option<usize> {
        if tail < self.len() {
            self.get_unchecked(tail)
        } else {
            None
        }
    }

    pub fn tail_positions(&self, head: usize) -> Option<components::IndexIterator<'_>>{
        if head < self.len() {
            Some(self.head_sort.get_all(head as i64))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> Option<usize> {
        let mut head_stream = self.head_stream.borrow_mut();
        let head = head_stream.get_row_unchecked(index)[0];
        if head.is_negative() {
            None
        } else {
            Some(head as usize)
        }
    }

    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'map> TryFrom<Container<'map>> for PointerVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let Container {
            mmap,
            name,
            header,
            mut components,
        } = container;

        match header.container_type {
            container::Type::PointerVariable => {
                let base = get_container_base!(header, PlainStringVariable);
                let n = header.dim1;

                let head_stream = check_and_return_component!(components, "HeadStream", Vector)?;
                if head_stream.len() != n || head_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("HeadStream"));
                }
                let head_stream = Rc::new(RefCell::new(CachedVector::new(head_stream)));

                let head_sort = check_and_return_component!(components, "HeadSort", Index)?;
                if head_sort.len() != n {
                    return Err(Self::Error::WrongComponentDimensions("HeadSort"));
                }

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
