use std::{cell::RefCell, rc::Rc};

use lru::LruCache;

#[derive(Debug, Clone, Copy)]
pub struct InvertedIndex<'map> {
    types: usize,
    typeinfo: &'map [(i64, i64)],
    data: &'map [u8],
}

impl<'map> InvertedIndex<'map> {
    pub fn from_parts(k: usize, typeinfo: &'map [(i64, i64)], data: &'map [u8]) -> Self {
        Self {
            types: k,
            typeinfo,
            data,
        }
    }

    /// Returns the frequency of type `i`
    pub fn frequency(&self, i: usize) -> usize {
        self.typeinfo[i].0 as usize
    }

    /// Returns the number of types in this index
    pub fn n_types(&self) -> usize {
        self.types
    }

    /// Returns the start offset of the postings list for type `i`
    /// within the `data` component
    pub fn offset(&self, i: usize) -> usize {
        self.typeinfo[i].1 as usize
    }

    /// Returns an iterator over the postings for type `i`
    pub fn postings(&self, i: usize) -> PostingsIterator {
        let slice = if i < self.n_types() - 1 {
            &self.data[self.offset(i)..self.offset(i + 1)]
        } else {
            &self.data[self.offset(i)..]
        };

        PostingsIterator {
            data: slice,
            len: self.frequency(i),
            i: 0,
            offset: 0,
            value: 0,
        }
    }
}

pub struct PostingsIterator<'map> {
    data: &'map [u8],
    len: usize,
    i: usize,
    offset: usize,
    value: usize,
}

impl<'map> Iterator for PostingsIterator<'map> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.len {
            let (value, readlen) = ziggurat_varint::decode(&self.data[self.offset..]);
            self.i += 1;
            self.offset += readlen;
            self.value += value as usize;
            Some(self.value)
        } else {
            None
        }
    }
}

/// A lazy postings list. Will only decode new data when needed.
#[derive(Debug)]
struct Postings<'map> {
    length: usize,
    decoded: Vec<i64>,
    undecoded: &'map [u8],
}

impl<'map> Postings<'map> {
    fn new(length: usize, data: &'map [u8]) -> Self {
        Self {
            length,
            decoded: Vec::new(),
            undecoded: data,
        }
    }

    fn decoded(&self) -> &[i64] {
        &self.decoded[..]
    }

    fn get(&self, i: usize) -> Option<i64> {
        if i < self.length {
            todo!()
        } else {
            None
        }
    }

    fn get_all(&self) -> &[i64] {
        todo!()
    }

    fn get_first(&self, n: usize) -> Option<&[i64]> {
        todo!()
    }

    fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug)]
pub struct PostingsCache<'map> {
    typeinfo: &'map [(i64, i64)],
    data: &'map [u8],
    cache: LruCache<usize, Postings<'map>>,
}

#[derive(Debug, Clone)]
pub struct CachedInvertedIndex<'map> {
    postings: Rc<RefCell<PostingsCache<'map>>>,
}
