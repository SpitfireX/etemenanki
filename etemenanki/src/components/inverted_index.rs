use std::{cell::RefCell, num::NonZeroUsize, rc::Rc};

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


/// A decoded in-memory postings list
#[derive(Debug)]
pub struct Postings {
    length: usize,
    decoded: Vec<usize>,
}

impl Postings {
    /// Will decode a complete postings list
    pub fn new(length: usize, data: &[u8]) -> Self {
        let (decoded, _) = ziggurat_varint::decode_fixed_delta_block(data, length);
        let decoded = decoded.into_iter().map(|i| i as usize).collect(); // compiler magic should make this a nop

        Self {
            length,
            decoded,
        }
    }

    /// Returns an individual position from the postings list
    pub fn get(&self, index: usize) -> Option<usize> {
        self.decoded.get(index).copied()
    }


    /// Returns all positions of the postings list as a slice
    pub fn get_all(&self) -> &[usize] {
        &self.decoded[..]
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Clone)]
pub struct CachedInvertedIndex<'map> {
    typeinfo: &'map [(i64, i64)],
    data: &'map [u8],
    cache: Rc<RefCell<LruCache<usize, Rc<Postings>>>>,
}

impl<'map> CachedInvertedIndex<'map> {
    pub fn new(invidx: InvertedIndex<'map>) -> Self {
        let InvertedIndex {types: _, typeinfo, data} = invidx;

        Self {
            typeinfo,
            data,
            cache: Rc::new(RefCell::new(LruCache::new(NonZeroUsize::new(500).unwrap()))),
        }
    }

    /// Returns the frequency of a type
    pub fn frequency(&self, type_id: usize) -> Option<usize> {
        self.typeinfo
            .get(type_id)
            .map(|(freq, _)| *freq as usize)
    }

    /// Returns the postings list of a type
    pub fn get_postings(&self, type_id: usize) -> Option<Rc<Postings>> {
        if type_id < self.typeinfo.len() {
            let mut cache = self.cache.borrow_mut();
            if !cache.contains(&type_id) {
                let (freq, offset) = self.typeinfo[type_id];
                let postings = Rc::new(Postings::new(freq as usize, &self.data[offset as usize..]));
                cache.put(type_id, postings);
            }

            cache
                .get(&type_id)
                .map(|rc| rc.clone())
        } else {
            None
        }
    }

    /// Iterator over the positions of a type
    pub fn positions(&self, type_id: usize) -> Option<CachedPostingsIterator> {
        self.frequency(type_id)
            .and_then(| max | self.positions_range(type_id, 0, max))
    }

    pub fn positions_from(&self, type_id: usize, start: usize) -> Option<CachedPostingsIterator> {
        self.frequency(type_id)
            .and_then(| max | self.positions_range(type_id, start, max))
    }

    pub fn positions_range(&self, type_id: usize, start: usize, end: usize) -> Option<CachedPostingsIterator> {
        self.frequency(type_id)
            .filter(| freq | end <= *freq)
            .and_then(| _ | self.get_postings(type_id))
            .map(| postings | CachedPostingsIterator::new(postings, type_id, start, end))
    }

    pub fn positions_until(&self, type_id: usize, end: usize) -> Option<CachedPostingsIterator> {
        self.positions_range(type_id, 0, end)
    }

    pub fn n_types(&self) -> usize {
        self.typeinfo.len()
    }
}

#[derive(Debug)]
pub struct CachedPostingsIterator {
    postings: Rc<Postings>,
    type_id: usize,
    position: usize,
    end: usize,
}

impl CachedPostingsIterator {
    pub fn new(postings: Rc<Postings>, type_id: usize, start: usize, end: usize) -> Self {
        Self { postings, type_id, position: start, end }
    }
}

impl Iterator for CachedPostingsIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.postings
            .get(self.position);
        self.position += 1;
        value
    }
}
