use core::hash::Hasher;
use std::{cell::RefCell, cmp::min, num::NonZeroUsize, rc::Rc};

use fnv::FnvHasher;
use lru::LruCache;

pub trait FnvHash {
    fn fnv_hash(&self) -> i64;
}

impl<T> FnvHash for T
where
    T: AsRef<[u8]>,
{
    fn fnv_hash(&self) -> i64 {
        let mut hasher = FnvHasher::default();
        hasher.write(self.as_ref());
        hasher.finish() as i64
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Index<'map> {
    Compressed {
        length: usize,
        r: usize,
        sync: &'map [(i64, usize)],
        data: &'map [u8],
    },

    Uncompressed {
        length: usize,
        pairs: &'map [(i64, i64)],
    },
}

impl<'map> Index<'map> {
    #[inline]
    pub fn contains_key(&self, key: i64) -> bool {
        match self.get_first(key) {
            Some(_) => true,
            None => false,
        }
    }

    pub fn compressed_from_parts(
        n: usize,
        r: usize,
        sync: &'map [(i64, usize)],
        data: &'map [u8],
    ) -> Self {
        Self::Compressed {
            length: n,
            r,
            sync,
            data,
        }
    }

    #[inline]
    pub fn get_first(&self, key: i64) -> Option<i64> {
        match *self {
            Index::Compressed { .. } => self.get_all(key).next(),

            Index::Uncompressed { length: _, pairs } => match Self::position(pairs, key) {
                Some(i) => Some(pairs[i].1),
                None => None,
            },
        }
    }

    #[inline]
    pub fn get_all(&self, key: i64) -> IndexIterator {
        IndexIterator::new(*self, key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Index::Compressed { length, .. } => *length,
            Index::Uncompressed { length, .. } => *length,
        }
    }

    /// Returns the sync block in which a key may be
    pub fn sync_block_position(sync: &[(i64, usize)], key: i64) -> usize {
        match sync.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(bi) => bi,
            Err(0) => 0,
            Err(nbi) => nbi - 1,
        }
    }

    fn position(pairs: &[(i64, i64)], key: i64) -> Option<usize> {
        match pairs.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => Some(i),
            Err(_) => None,
        }
    }

    pub fn uncompressed_from_parts(n: usize, pairs: &'map [(i64, i64)]) -> Self {
        Self::Uncompressed { length: n, pairs }
    }
}

#[derive(Debug)]
pub enum IndexIterator<'map> {
    None,

    Compressed {
        data: &'map [u8],
        position: usize,
        len: usize,
        last_value: i64,
    },

    Uncompressed {
        pairs: &'map [(i64, i64)],
        key: i64,
        position: usize,
    },
}

impl<'map> IndexIterator<'map> {
    pub fn new(index: Index<'map>, key: i64) -> Self {
        match index {
            Index::Compressed {
                length: _,
                r,
                sync,
                data,
            } => {
                let bi = Index::sync_block_position(sync, key);
                let mut offset = sync[bi].1 as usize;

                // number of overflow items
                let (o, readlen) = ziggurat_varint::decode(&data[offset..]);
                offset += readlen;

                // read keys vector
                let klen = min(r - (bi * 16), 16); // number of keys can be <16
                let (keys, readlen) = ziggurat_varint::decode_delta_array::<16>(&data[offset..]);
                offset += readlen;

                let p = keys[..klen].partition_point(|&x| x < key);
                if p == klen {
                    // key not in block
                    Self::None
                } else {
                    // key potentially in block at i

                    // determine number of elements with key in block
                    let mut len = keys.iter().filter(|&x| *x == key).count();
                    // add overflow items if key is the last in block
                    if keys[keys.len() - 1] == key {
                        len += o as usize;
                    }

                    // discard first ki values in block
                    let mut start = 0;
                    for _ in 0..p {
                        let (v, readlen) = ziggurat_varint::decode(&data[offset..]);
                        start += v;
                        offset += readlen;
                    }

                    Self::Compressed {
                        data: &data[offset..],
                        position: 0,
                        len,
                        last_value: start,
                    }
                }
            }

            Index::Uncompressed { length: _, pairs } => match Index::position(pairs, key) {
                Some(position) => Self::Uncompressed {
                    pairs,
                    key,
                    position,
                },
                None => Self::None,
            },
        }
    }
}

impl<'map> Iterator for IndexIterator<'map> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            Self::None => None,

            Self::Compressed {
                ref mut data,
                ref mut position,
                len,
                ref mut last_value,
            } => {
                if *position < len {
                    let (v, readlen) = ziggurat_varint::decode(data);
                    *data = &data[readlen..];
                    *position += 1;
                    *last_value += v;
                    Some(*last_value)
                } else {
                    None
                }
            }

            Self::Uncompressed {
                pairs,
                key,
                ref mut position,
            } => {
                if *position < pairs.len() && pairs[*position].0 == key {
                    let value = pairs[*position].1;
                    *position += 1;
                    Some(value)
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct IndexBlock<'map> {
    regular_items: usize,
    overflow_items: usize,
    keys: [i64; 16],
    positions: Vec<i64>,
    overflow_data: &'map [u8],
}

impl<'map> IndexBlock<'map> {

    /// Decodes a block from compressed raw data.
    pub fn decode(data: &'map [u8], regular_items: usize) -> Self {
        // decode the number of overflow items in block
        // this should be:
        //  - overflow_items = 0 when regular_items < B (16)
        //  - overflow_items > 0 when regular_items >= B (16)
        let (overflow_items, mut offset) = ziggurat_varint::decode(data);

        // decode the 16 keys always present in block
        let (keys, readlen) = ziggurat_varint::decode_delta_array(&data[offset..]);
        offset += readlen;

        // decode the first regular_items, max B = 16
        let (positions, readlen) =
            ziggurat_varint::decode_fixed_delta_block(&data[offset..], regular_items);

        // keep position of next possible data position for future decoding
        let overflow_data = &data[offset + readlen..];

        Self {
            regular_items,
            overflow_items: overflow_items as usize,
            keys,
            positions,
            overflow_data,
        }
    }

    /// Returns a slice over the first 16 keys of the block.
    pub fn keys(&self) -> &[i64] {
        &self.keys[..self.regular_items]
    }

    /// Returns the key of a given position in the block.
    pub fn get_key(&self, index: usize) -> Option<i64> {
        if index < self.len() {
            if index < self.regular_items {
                Some(self.keys[index])
            } else {
                self.keys.last().copied()
            }
        } else {
            None
        }
    }

    /// Returns a tuple of the form (key, value) for a given position in the block
    /// This action may decode additional overflow items. 
    pub fn get_pair(&mut self, index: usize) -> Option<(i64, i64)> {
        self.get_key(index)
            .zip(self.get_position(index))
    }

    /// Returns a slice over all positions in the block including all overflow items.
    /// This action may decode additional overflow items.
    pub fn get_all_position_(&mut self) -> &[i64] {
        self.get_first_positions(self.regular_items + self.overflow_items)
            .expect("this should never fail")
    }

    /// Returns a slice over the first n positions from the block.
    /// This action may decode additional overflow items.
    pub fn get_first_positions(&mut self, n: usize) -> Option<&[i64]> {
        if n <= self.regular_items {
            // index within already decoded regular positions, all good
            Some(&self.positions[..n])
        } else if self.overflow_items > 0 && n <= 16 + self.overflow_items {
            // index within overflow items. we may need to decode additional values
            // overflow items should only be possible when the block is full, thus fixed 16 and not
            // self.overflow_items in check

            // check if we need to decode additional values and extend self.positions
            if n > self.positions.len() {
                let decode_len = n - self.positions.len();
                for _ in 0..decode_len {
                    let (i, readlen) = ziggurat_varint::decode(self.overflow_data);
                    self.overflow_data = &self.overflow_data[readlen..]; // move slice to new beginning of undecoded data
                    let last = self.positions.last().copied().unwrap_or(0);
                    self.positions.push(last + i);
                }
            }

            Some(&self.positions[..n])
        } else {
            None
        }
    }

    /// Returns the value of a single position from the block.
    /// This action may decode additional overflow items.
    pub fn get_position(&mut self, index: usize) -> Option<i64> {
        self.get_first_positions(index + 1)
            .map(|p| p[index])
    }

    /// Returns the total length of the block.
    pub fn len(&self) -> usize {
        self.regular_items + self.overflow_items
    }

    /// Returns the number of regular items in the block.
    pub fn regular_items(&self) -> usize {
        self.regular_items
    }

    /// Returns the number of overflow items in the block.
    pub fn overflow_items(&self) -> usize {
        self.overflow_items
    }
}

#[derive(Debug)]
pub struct IndexBlockCache<'map> {
    r: usize,
    sync: &'map [(i64, usize)],
    data: &'map [u8],
    cache: LruCache<usize, IndexBlock<'map>>,
}

impl<'map> IndexBlockCache<'map> {
    pub fn new(r: usize, sync: &'map [(i64, usize)], data: &'map [u8]) -> Self {
        Self {
            r,
            sync,
            data,
            cache: LruCache::new(NonZeroUsize::new(250).unwrap())
        }
    }

    pub fn sync_block_position(&self, key: i64) -> usize {
        Index::sync_block_position(self.sync, key)
    }

    /// Returns the reference to a cached IndexBlock.
    /// If the is not yet in the cache it will be decoded.
    pub fn get_block(&mut self, block_index: usize) -> Option<&mut IndexBlock<'map>> {
        if block_index < self.sync.len() {
            if !self.cache.contains(&block_index) {
                let offset = self.sync[block_index].1 as usize;
                let br = min(self.r - (block_index * 16), 16);
                let block = IndexBlock::decode(&self.data[offset..], br);
                self.cache.put(block_index, block);
            }
    
            self.cache
                .get_mut(&block_index)
        } else {
            None
        }
    }
}

/// Alternative type for `Index` implementing efficient cached access.
/// Compressed index blocks are stored in an LRU cache and only decoded
/// as needed.
#[derive(Debug, Clone)]
pub enum CachedIndex<'map> {
    Uncompressed {
        length: usize,
        pairs: &'map [(i64, i64)],
    },

    Compressed {
        length: usize,
        cache: Rc<RefCell<IndexBlockCache<'map>>>,
    },
}

impl<'map> CachedIndex<'map> {

    pub fn new(index: Index<'map>) -> Self {
        match index {
            Index::Uncompressed { length, pairs } => Self::Uncompressed { length, pairs },
            Index::Compressed { length, r, sync, data } => {
                Self::Compressed {
                    length,
                    cache: Rc::new(RefCell::new(IndexBlockCache::new(r, sync, data)))
                }
            }
        }
    }

    pub fn cache(&self) -> Option<Rc<RefCell<IndexBlockCache<'map>>>> {
        if let Self::Compressed { length: _, cache } = self {
            Some(cache.clone())
        } else {
            None
        }
    }

    pub fn contains_key(&self, key: i64) -> bool {
        self.get_first(key).is_some()
    }

    pub fn get_all(&self, key: i64) -> CachedValueIterator<'map> {
        CachedValueIterator::new(self, key)
    }

    pub fn get_first(&self, key: i64) -> Option<i64> {
        self.get_all(key).next()
    }

    pub fn len(&self) -> usize {
        match self {
            CachedIndex::Uncompressed { length, .. } |
            CachedIndex::Compressed { length, .. } => *length,
        }
    }

}

/// Iterator that yields all positions for a key from a given CachedIndex
pub enum CachedValueIterator<'map> {
    None,

    Uncompressed {
        pairs: &'map [(i64, i64)],
        position: usize,
        key: i64,
    },

    Compressed {
        cache: Rc<RefCell<IndexBlockCache<'map>>>,
        block_index: usize,
        position: usize,
        key: i64,
    }
}

impl<'map> CachedValueIterator<'map> {
    fn new(cidx: &CachedIndex<'map>, key: i64) -> Self {
        match cidx {
            CachedIndex::Uncompressed { length: _, pairs } => {
                if let Some(position) = Index::position(pairs, key) {
                    CachedValueIterator::Uncompressed { 
                        pairs,
                        position,
                        key,
                    }
                } else {
                    Self::None
                }
            },

            CachedIndex::Compressed { length: _, cache } => {
                let cache = cache.clone();

                let (block_index, position ) = {
                    let mut cacheref = cache.borrow_mut();
                    
                    let block_index = cacheref.sync_block_position(key);
                    let block = cacheref.get_block(block_index).expect("at this point the block must be cached");

                    // partition_point() will result in Some(position), even if the key is
                    // not actually in the block. This is fine, since the iterator will
                    // check the key at a later point again.
                    let position = block.keys().partition_point(|&x| x < key );
                    if position < block.keys().len() {
                        return Self::None;
                    }

                    (block_index, position)
                };

                Self::Compressed {
                    cache,
                    block_index,
                    position,
                    key,
                }
            }
        }
    }
}

impl<'map> Iterator for CachedValueIterator<'map> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::None => None,

            Self::Uncompressed { pairs, key, position } => {
                if *position < pairs.len() {
                    let (ckey, value) = pairs[*position];
                    if ckey == *key {
                        *position += 1;
                        return Some(value)
                    }
                }
                None
            }

            Self::Compressed { cache, block_index, position, key } => {
                let mut cache = cache.borrow_mut();
                let block = cache.get_block(*block_index).expect("at this point the block must be cached");

                if *position < block.len() {
                    let (ckey, value) = block.get_pair(*position).unwrap();
                    if ckey == *key {
                        *position += 1;
                        return Some(value)
                    }
                }
                None
            }
        }
    }
}
