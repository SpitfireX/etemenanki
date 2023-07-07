use std::ops;

use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use crate::components;
use crate::container::{self, Container};
use crate::macros::{check_and_return_component, get_container_base};

#[derive(Debug, EnumAsInner)]
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
    type Error = container::TryFromError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
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

            container::Type::PointerVariable => todo!(),

            container::Type::ExternalPointerVariable => todo!(),

            container::Type::SetVariable => Ok(Self::Set(SetVariable::try_from(container)?)),

            container::Type::HashVariable => todo!(),

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct IndexedStringVariable<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'a>,
    lexicon: components::StringVector<'a>,
    lex_hash: components::Index<'a>,
    partition: components::Vector<'a>,
    lex_id_stream: components::Vector<'a>,
    lex_id_index: components::InvertedIndex<'a>,
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
    type Error = container::TryFromError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
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

                let partition = check_and_return_component!(components, "Partition", Vector)?;
                // consistency gets checked at datastore creation

                let lex_id_stream = check_and_return_component!(components, "LexIDStream", Vector)?;
                if lex_id_stream.len() != n || lex_id_stream.width() != 1 {
                    return Err(Self::Error::WrongComponentDimensions("LexIDStream"));
                }

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
                    partition,
                    lex_id_stream,
                    lex_id_index,
                })
            }

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}

#[derive(Debug)]
pub struct PlainStringVariable<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'a>,
    string_data: components::StringList<'a>,
    offset_stream: components::Vector<'a>,
    string_hash: components::Index<'a>,
}

impl<'a> PlainStringVariable<'a> {
    pub fn iter(&self) -> PlainStringIterator {
        PlainStringIterator {
            var: self,
            index: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'a> ops::Index<usize> for PlainStringVariable<'a> {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        let start = self.offset_stream.get(index) as usize;
        let end = self.offset_stream.get(index + 1) as usize;

        unsafe { std::str::from_utf8_unchecked(&self.string_data[start..end - 1]) }
    }
}

impl<'a> TryFrom<Container<'a>> for PlainStringVariable<'a> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
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

pub struct PlainStringIterator<'a> {
    var: &'a PlainStringVariable<'a>,
    index: usize,
}

impl<'a> Iterator for PlainStringIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.var.len() {
            let string = &self.var[self.index];
            self.index += 1;
            Some(string)
        } else {
            None
        }
    }
}

impl<'a> IntoIterator for &'a PlainStringVariable<'a> {
    type Item = &'a str;
    type IntoIter = PlainStringIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PlainStringIterator {
            var: self,
            index: 0,
        }
    }
}

#[derive(Debug)]
pub struct IntegerVariable<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'a>,
    int_stream: components::Vector<'a>,
    int_sort: components::Index<'a>,
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
    type Error = container::TryFromError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
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

#[derive(Debug)]
pub struct SetVariable<'a> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'a>,
    lexicon: components::StringVector<'a>,
    lex_hash: components::Index<'a>,
    partition: components::Vector<'a>,
    id_set_stream: components::Set<'a>,
    id_set_index: components::InvertedIndex<'a>,
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
    type Error = container::TryFromError;

    fn try_from(container: Container<'a>) -> Result<Self, Self::Error> {
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

                let partition = check_and_return_component!(components, "Partition", Vector)?;
                // consistency gets checked at datastore creation

                let id_set_stream = check_and_return_component!(components, "IDSetStream", Set)?;
                if id_set_stream.len() != n {
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
                    partition,
                    id_set_stream,
                    id_set_index,
                })
            }

            _ => Err(Self::Error::WrongContainerType),
        }
    }
}
