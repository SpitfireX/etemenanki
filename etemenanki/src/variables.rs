use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::rc::Rc;

use enum_as_inner::EnumAsInner;
use memmap2::Mmap;
use uuid::Uuid;

use crate::components::{self, CachedIndex, CachedInvertedIndex, CachedVector, ColumnIterator, FnvHash, Index, LexiconBuilder, Vector};
use crate::container::{self, Container, ContainerBuilder};
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
    pub header: &'map container::Header,
    lexicon: components::StringVector<'map>,
    lex_hash: components::CachedIndex<'map>,
    lex_id_stream: components::CachedVector<'map, 1>,
    lex_id_index: Rc<components::CachedInvertedIndex<'map>>,
}

impl<'map> IndexedStringVariable<'map> {
    pub fn encode_to_file<I>(file: File, strings: I, n: usize, name: String, base: Uuid, compressed: bool, comment: &str) -> Self where I: Iterator<Item=String> {
        let vectype = if compressed { components::Type::VectorComp } else { components::Type::Vector };

        let lexbuilder = LexiconBuilder::from_strings(strings);
        assert!(lexbuilder.tokens() == n, "found fewer tokens than layer size");

        let builder = ContainerBuilder::new_into_file(name, file, 4)
            .edit_header(| h | {
                h.comment(comment)
                    .ziggurat_type(container::Type::IndexedStringVariable)
                    .dim1(lexbuilder.tokens())
                    .dim2(lexbuilder.types())
                    .base1(Some(base));
            })
            .add_component("Lexicon", components::Type::StringVector, | bom_entry, file | {
                unsafe {
                    lexbuilder.write_lexicon(file, bom_entry, bom_entry.offset as u64);
                }
            })
            .add_component("LexHash", components::Type::Index, | bom_entry, file | {
                unsafe {
                    lexbuilder.write_index(file, bom_entry, bom_entry.offset as u64);
                }
            })
            .add_component("LexIDStream", vectype, | bom_entry, file | {
                unsafe {
                    lexbuilder.write_id_stream(file, bom_entry, bom_entry.offset as u64, compressed);
                }
            })
            .add_component("LexIDIndex", components::Type::InvertedIndex, | bom_entry, file | {
                lexbuilder.write_inverted_index(file, bom_entry, bom_entry.offset as u64);
            });

        builder.build().try_into().expect("IndexedStringVariable returned by its constructor is inconsistent")
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        if index < self.lex_id_stream.len() {
            let ti = self.lex_id_stream.get_row_unchecked(index)[0];
            self.lexicon.get(ti as usize)
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

    pub fn get_range(&self, start: usize, end: usize) -> Option<IndexedStringIterator<'map>> {
        IndexedStringIterator::new(self, start, end)
    }

    pub fn id_stream(&self) -> components::CachedVector<'map, 1> {
        self.lex_id_stream.clone()
    }

    pub fn index(&self) -> components::CachedIndex<'map> {
        self.lex_hash.clone()
    }

    pub fn inverted_index(&self) -> Rc<components::CachedInvertedIndex<'map>> {
        self.lex_id_index.clone()
    }

    pub fn iter(&self) -> IndexedStringIterator<'map> {
        self.get_range(0, self.len()).unwrap()
    }

    pub fn len(&self) -> usize {
        self.header.dim1()
    }

    pub fn lexicon(&self) -> &components::StringVector<'map> {
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
    ids: components::ColumnIterator<'map, 1>,
}

impl<'map> IndexedStringIterator<'map> {
    pub fn new(var: &IndexedStringVariable<'map>, start: usize, end: usize) -> Option<Self> {
        var.id_stream().column_iter_range(start, end, 0)
            .map(| ids | {
                IndexedStringIterator {
                    lexicon: *var.lexicon(),
                    ids,
                }
            })
    }
}

impl<'map> Iterator for IndexedStringIterator<'map> {
    type Item = &'map str;

    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next()
            .and_then(| id | {
                self.lexicon.get(id as usize)
            })
    }
}

impl<'a, 'map> IntoIterator for &'a IndexedStringVariable<'map> {
    type Item = &'map str;
    type IntoIter = IndexedStringIterator<'map>;

    fn into_iter(self) -> Self::IntoIter {
        IndexedStringIterator::new(self, 0, self.len()).unwrap()
    }
}

#[derive(Debug)]
pub struct PlainStringVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: &'map container::Header,
    string_data: components::StringList<'map>,
    offset_stream: components::CachedVector<'map, 1>,
    string_hash: components::CachedIndex<'map>,
}

impl<'map> PlainStringVariable<'map> {
    pub fn encode_to_file<I>(file: File, strings: I, n: usize, name: String, base: Uuid, compressed: bool, comment: &str) -> Self where I: Iterator<Item=String> {
        let vectype = if compressed { components::Type::VectorDelta } else { components::Type::Vector };
        let idxtype = if compressed { components::Type::IndexComp } else { components::Type::Index };

        let mut offsets = Vec::with_capacity(n + 1);
        offsets.push(0);

        let mut hashes = Vec::with_capacity(n);

        let builder = ContainerBuilder::new_into_file(name, file, 3)
            .edit_header(| h | {
                h.comment(comment)
                    .ziggurat_type(container::Type::PlainStringVariable)
                    .dim1(n)
                    .dim2(0)
                    .base1(Some(base));
            })
            .add_component("StringData", components::Type::StringList, | bom_entry, file | {
                let start_offset = bom_entry.offset as u64;
                file.seek(SeekFrom::Start(start_offset)).unwrap();

                let mut writer = BufWriter::new(file);

                // write all string data to component and
                // - record lengths/offsets
                // - record hash and index
                for (i, s) in strings.take(n).enumerate() {
                    let bytes = s.as_bytes();

                    writer.write_all(bytes).unwrap();
                    writer.write_all(&[0]).unwrap();

                    // offset
                    if let Some(offset) = offsets.last() {
                        offsets.push(offset + (bytes.len() + 1) as i64);
                    }

                    // hash
                    let hash = bytes.fnv_hash();
                    hashes.push((hash, i as i64));
                }

                assert!(offsets.len() == n + 1, "found fewer tokens than layer size");

                bom_entry.size = *offsets.last().unwrap();
                bom_entry.param1 = n as i64;
                bom_entry.param2 = 0;
            })
            .add_component("OffsetStream", vectype, | bom_entry, file | {
                unsafe {
                    if compressed {
                        Vector::encode_delta_to_container_file(offsets.into_iter().map(|i| [i]), n + 1, file, bom_entry, bom_entry.offset as u64);
                    } else {
                        Vector::encode_uncompressed_to_container_file(offsets.into_iter(), n + 1, 1, file, bom_entry, bom_entry.offset as u64);
                    }
                }
            })
            .add_component("StringHash", idxtype, | bom_entry, file | {
                hashes.sort_by_key(|(h, _)| *h);

                unsafe {
                    if compressed {
                        Index::encode_compressed_to_container_file(hashes.into_iter(), n, file, bom_entry, bom_entry.offset as u64);
                    } else {
                        Index::encode_uncompressed_to_container_file(hashes.into_iter(), n, file, bom_entry, bom_entry.offset as u64);
                    }
                }
            });

        builder.build().try_into().expect("PlainStringVariable returned by its constructor is inconsistent")
    }

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

impl<'a, 'map> IntoIterator for &'a PlainStringVariable<'map> {
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
    pub header: &'map container::Header,
    int_stream: components::CachedVector<'map, 1>,
    int_sort: components::CachedIndex<'map>,
}

impl<'map> IntegerVariable<'map> {
    pub fn encode_to_file<I>(file: File, values: I, n: usize, name: String, base: Uuid, compressed: bool, delta: bool, comment: &str) -> Self where I: Iterator<Item=i64> {
        let vectype = if compressed { 
            if delta {
                components::Type::VectorDelta
            } else {
                components::Type::VectorComp
            }
         } else {
            components::Type::Vector
        };
        let idxtype = if compressed { components::Type::IndexComp } else { components::Type::Index };

        // we need to load all values into memory so we can sort them later
        // this step is very memory-intensive and could be replaced with a reverse index component later on
        // format: [(value, index); n]
        let mut values: Vec<(i64, i64)> = values.take(n).enumerate().map(|(i, v)| (v, i as i64)).collect();
        
        let mut builder = ContainerBuilder::new_into_file(name, file, 2)
            .edit_header(| h | {
                h.comment(comment)
                    .ziggurat_type(container::Type::IntegerVariable)
                    .dim1(n)
                    .dim2(1)
                    .base1(Some(base));
            })
            .add_component("IntStream", vectype, | bom_entry, file | {
                unsafe {
                    if compressed {
                        let values = values.iter().map(|(v, _)| [*v; 1]);
                        if delta {
                            Vector::encode_delta_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
                        } else {
                            Vector::encode_compressed_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
                        }
                    } else {
                        Vector::encode_uncompressed_to_container_file(values.iter().map(|(v, _)| *v), n, 1, file, bom_entry, bom_entry.offset as u64);
                    }
                }
            });

        // sort values via their value
        values.sort_by_key(|(v, _)| *v);

        builder = builder.add_component("IntSort", idxtype, | bom_entry, file | {
            unsafe {
                if compressed {
                    Index::encode_compressed_to_container_file(values.iter().copied(), n, file, bom_entry, bom_entry.offset as u64);
                } else {
                    Index::encode_uncompressed_to_container_file(values.iter().copied(), n, file, bom_entry, bom_entry.offset as u64);
                }
            }
        });

        builder.build().try_into().expect("IntegerVariable returned by its constructor is inconsistent")
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

    pub fn iter(&self) -> ColumnIterator<'map, 1> {
        self.int_stream.column_iter(0)
    }

    pub fn len(&self) -> usize {
        self.header.dim1()
    }

    pub fn b(&self) -> usize {
        self.header.dim2()
    }
}

impl<'a, 'map> IntoIterator for &'a IntegerVariable<'map> {
    type Item = i64;
    type IntoIter = ColumnIterator<'map, 1>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'map> TryFrom<Container<'map>> for IntegerVariable<'map> {
    type Error = container::TryFromError;

    fn try_from(container: Container<'map>) -> Result<Self, Self::Error> {
        let header = *container.header();

        match header.container_type() {
            container::Type::IntegerVariable => {
                let base = get_container_base!(container, IntegerVariable);
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

#[derive(Debug)]
pub struct SetVariable<'map> {
    base: Uuid,
    mmap: Mmap,
    pub name: String,
    pub header: &'map container::Header,
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
    pub header: &'map container::Header,
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

    pub fn encode_to_file<I>(file: File, heads: I, n: usize, name: String, base: Uuid, compressed: bool, comment: &str) -> Self where I: Iterator<Item=i64> {
        let vectype = if compressed { components::Type::VectorDelta } else { components::Type::Vector };
        let idxtype = if compressed { components::Type::IndexComp } else { components::Type::Index };

        // we need to load all values into memory so we can sort them later
        // this step is very memory-intensive and could be replaced with a reverse index component later on
        // format: [(head, cpos); n]
        let mut values: Vec<(i64, i64)> = heads.take(n).enumerate().map(|(cpos, head)| (head, cpos as i64)).collect();
        
        let mut builder = ContainerBuilder::new_into_file(name, file, 2)
            .edit_header(| h | {
                h.comment(comment)
                    .ziggurat_type(container::Type::PointerVariable)
                    .dim1(n)
                    .dim2(0)
                    .base1(Some(base));
            })
            .add_component("HeadStream", vectype, | bom_entry, file | {
                unsafe {
                    if compressed {
                        let values = values.iter().map(|(head, _)| [*head; 1]);
                        Vector::encode_delta_to_container_file(values, n, file, bom_entry, bom_entry.offset as u64);
                    } else {
                        Vector::encode_uncompressed_to_container_file(values.iter().map(|(head, _)| *head), n, 1, file, bom_entry, bom_entry.offset as u64);
                    }
                }
            });

        // sort values via their value
        values.sort_by_key(|(head, _)| *head);

        builder = builder.add_component("HeadSort", idxtype, | bom_entry, file | {
            unsafe {
                if compressed {
                    Index::encode_compressed_to_container_file(values.iter().copied(), n, file, bom_entry, bom_entry.offset as u64);
                } else {
                    Index::encode_uncompressed_to_container_file(values.iter().copied(), n, file, bom_entry, bom_entry.offset as u64);
                }
            }
        });

        builder.build().try_into().expect("PointerVariable returned by its constructor is inconsistent")
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

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::IntegerVariable;

    #[test]
    fn encode_intvar_uncompressed() {
        let file = tempfile::tempfile().unwrap();

        let values = 1337..9_000_001;
        
        let _ = IntegerVariable::encode_to_file(file, values, 5_000_000, "testintvar".to_owned(), Uuid::new_v4(), false, true, "IntVar encoded for testing purposes.");
    }

    #[test]
    fn encode_intvar_compressed() {
        let file = tempfile::tempfile().unwrap();

        let values = 1337..9_000_001;
        
        let _ = IntegerVariable::encode_to_file(file, values, 5_000_001, "testintvar".to_owned(), Uuid::new_v4(), true, true, "IntVar encoded for testing purposes.");
    }
}
