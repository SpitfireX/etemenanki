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

/// A lazy postings list. Will only decode new data when needed.
#[derive(Debug)]
pub struct Postings<'map> {
    length: usize,
    decoded: Vec<usize>,
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

    fn decoded(&self) -> &[usize] {
        &self.decoded[..]
    }

    fn get(&mut self, index: usize) -> Option<usize> {
        self.get_first(index + 1)
            .map(|p| p[index])
    }

    fn get_all(&mut self) -> &[usize] {
        self.get_first(self.length)
            .expect("this should never fail")
    }

    fn get_first(&mut self, n: usize) -> Option<&[usize]> {
        if n <= self.decoded.len() {
            // index within already decoded postings, all good
            Some(&self.decoded[..n])
        } else if n <= self.length {
            // index within possible postings. we need to decode additional values
            // check if we need to decode additional values and extend self.positions
            let decode_len = n - self.decoded.len();
            for _ in 0..decode_len {
                let (i, readlen) = ziggurat_varint::decode(self.undecoded);
                self.undecoded = &self.undecoded[readlen..]; // move slice to new beginning of undecoded data
                let last = self.decoded.last().copied().unwrap_or(0);
                self.decoded.push(last + i as usize);
            }

            Some(&self.decoded[..n])
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Clone)]
pub struct CachedInvertedIndex<'map> {
    typeinfo: &'map [(i64, i64)],
    data: &'map [u8],
    cache: Rc<RefCell<LruCache<usize, Rc<RefCell<Postings<'map>>>>>>,
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

    pub fn frequency(&self, type_id: usize) -> Option<usize> {
        self.typeinfo
            .get(type_id)
            .map(|(freq, _)| *freq as usize)
    }

    pub fn get_postings(&self, type_id: usize) -> Option<Rc<RefCell<Postings<'map>>>> {
        if type_id < self.typeinfo.len() {
            let mut cache = self.cache.borrow_mut();
            if !cache.contains(&type_id) {
                let (freq, offset) = self.typeinfo[type_id];
                let postings = Rc::new(RefCell::new(Postings::new(freq as usize, &self.data[offset as usize..])));
                cache.put(type_id, postings);
            }

            cache
                .get(&type_id)
                .map(|rc| rc.clone())
        } else {
            None
        }
    }

    pub fn positions(&self, type_id: usize) -> Option<CachedPostingsIterator<'map>> {
        self.frequency(type_id)
            .and_then(| max | self.positions_range(type_id, 0, max))
    }

    pub fn positions_from(&self, type_id: usize, start: usize) -> Option<CachedPostingsIterator<'map>> {
        self.frequency(type_id)
            .and_then(| max | self.positions_range(type_id, start, max))
    }

    pub fn positions_range(&self, type_id: usize, start: usize, end: usize) -> Option<CachedPostingsIterator<'map>> {
        self.frequency(type_id)
            .filter(| freq | end <= *freq)
            .and_then(| _ | self.get_postings(type_id))
            .map(| postings | CachedPostingsIterator::new(postings, type_id, start, end))
    }

    pub fn positions_until(&self, type_id: usize, end: usize) -> Option<CachedPostingsIterator<'map>> {
        self.positions_range(type_id, 0, end)
    }

    pub fn n_types(&self) -> usize {
        self.typeinfo.len()
    }
}

#[derive(Debug)]
pub struct CachedPostingsIterator<'map> {
    postings: Rc<RefCell<Postings<'map>>>,
    type_id: usize,
    position: usize,
    end: usize,
}

impl<'map> CachedPostingsIterator<'map> {
    pub fn new(postings: Rc<RefCell<Postings<'map>>>, type_id: usize, start: usize, end: usize) -> Self {
        Self { postings, type_id, position: start, end }
    }
}

impl<'map> Iterator for CachedPostingsIterator<'map> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.end {
            let mut postings = self.postings.borrow_mut();
            let value = postings.get(self.position);
            self.position += 1;
            value
        } else {
            None
        }
    }
}


#[derive(Debug)]
pub struct GreedyPostings {
    length: usize,
    decoded: Vec<usize>,
}

impl GreedyPostings {
    pub fn new(length: usize, data: &[u8]) -> Self {
        let (decoded, _) = ziggurat_varint::decode_fixed_delta_block(data, length);
        let decoded = decoded.into_iter().map(|i| i as usize).collect(); // compiler magic should make this a nop

        Self {
            length,
            decoded,
        }
    }

    pub fn get(&self, index: usize) -> Option<usize> {
        self.decoded.get(index).copied()
    }

    pub fn get_all(&self) -> &[usize] {
        &self.decoded[..]
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Clone)]
pub struct GreedyCachedInvertedIndex<'map> {
    typeinfo: &'map [(i64, i64)],
    data: &'map [u8],
    cache: Rc<RefCell<LruCache<usize, Rc<GreedyPostings>>>>,
}

impl<'map> GreedyCachedInvertedIndex<'map> {
    pub fn new(invidx: InvertedIndex<'map>) -> Self {
        let InvertedIndex {types: _, typeinfo, data} = invidx;

        Self {
            typeinfo,
            data,
            cache: Rc::new(RefCell::new(LruCache::new(NonZeroUsize::new(500).unwrap()))),
        }
    }

    pub fn frequency(&self, type_id: usize) -> Option<usize> {
        self.typeinfo
            .get(type_id)
            .map(|(freq, _)| *freq as usize)
    }

    pub fn get_postings(&self, type_id: usize) -> Option<Rc<GreedyPostings>> {
        if type_id < self.typeinfo.len() {
            let mut cache = self.cache.borrow_mut();
            if !cache.contains(&type_id) {
                let (freq, offset) = self.typeinfo[type_id];
                let postings = Rc::new(GreedyPostings::new(freq as usize, &self.data[offset as usize..]));
                cache.put(type_id, postings);
            }

            cache
                .get(&type_id)
                .map(|rc| rc.clone())
        } else {
            None
        }
    }

    pub fn positions(&self, type_id: usize) -> Option<CachedGreedyPostingsIterator> {
        self.frequency(type_id)
            .and_then(| max | self.positions_range(type_id, 0, max))
    }

    pub fn positions_from(&self, type_id: usize, start: usize) -> Option<CachedGreedyPostingsIterator> {
        self.frequency(type_id)
            .and_then(| max | self.positions_range(type_id, start, max))
    }

    pub fn positions_range(&self, type_id: usize, start: usize, end: usize) -> Option<CachedGreedyPostingsIterator> {
        self.frequency(type_id)
            .filter(| freq | end <= *freq)
            .and_then(| _ | self.get_postings(type_id))
            .map(| postings | CachedGreedyPostingsIterator::new(postings, type_id, start, end))
    }

    pub fn positions_until(&self, type_id: usize, end: usize) -> Option<CachedGreedyPostingsIterator> {
        self.positions_range(type_id, 0, end)
    }

    pub fn n_types(&self) -> usize {
        self.typeinfo.len()
    }
}

#[derive(Debug)]
pub struct CachedGreedyPostingsIterator {
    postings: Rc<GreedyPostings>,
    type_id: usize,
    position: usize,
    end: usize,
}

impl CachedGreedyPostingsIterator {
    pub fn new(postings: Rc<GreedyPostings>, type_id: usize, start: usize, end: usize) -> Self {
        Self { postings, type_id, position: start, end }
    }
}

impl Iterator for CachedGreedyPostingsIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.postings
            .get(self.position);
        self.position += 1;
        value
    }
}
