use std::ops;

use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use crate::components;
use crate::container::{self, Container};
use crate::macros::{check_and_return_component, get_container_base};

#[derive(Debug, EnumAsInner)]
pub enum Variable<'map> {
    IndexedString(IndexedStringVariable<'map>),
    PlainString(PlainStringVariable<'map>),
    Integer(IntegerVariable<'map>),
    Pointer,
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

            container::Type::PointerVariable => todo!(),

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
    partition: components::Vector<'map>,
    lex_id_stream: components::Vector<'map>,
    lex_id_index: components::InvertedIndex<'map>,
}

impl<'map> IndexedStringVariable<'map> {
    pub fn len(&self) -> usize {
        self.header.dim1
    }

    pub fn n_types(&self) -> usize {
        self.header.dim2
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
pub struct PlainStringVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'map>,
    string_data: components::StringList<'map>,
    offset_stream: components::Vector<'map>,
    string_hash: components::Index<'map>,
}

impl<'map> PlainStringVariable<'map> {
    pub fn iter(&self) -> PlainStringIterator {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.header.dim1
    }
}

impl<'map> ops::Index<usize> for PlainStringVariable<'map> {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        let start = self.offset_stream.get(index) as usize;
        let end = self.offset_stream.get(index + 1) as usize;

        unsafe { std::str::from_utf8_unchecked(&self.string_data[start..end - 1]) }
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
    offset_reader: components::VectorReader<'map>,
    len: usize,
    index: usize,
}

impl<'map> Iterator for PlainStringIterator<'map> {
    type Item = &'map str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len {
            let start = self.offset_reader.get(self.index) as usize;
            let end = self.offset_reader.get(self.index + 1) as usize;
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
            offset_reader: self.offset_stream.into_iter(),
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
    int_stream: components::Vector<'map>,
    int_sort: components::Index<'map>,
}

impl<'map> IntegerVariable<'map> {
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
pub struct SetVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: container::Header<'map>,
    lexicon: components::StringVector<'map>,
    lex_hash: components::Index<'map>,
    partition: components::Vector<'map>,
    id_set_stream: components::Set<'map>,
    id_set_index: components::InvertedIndex<'map>,
}

impl<'map> SetVariable<'map> {
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
