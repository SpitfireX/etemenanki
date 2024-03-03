use core::hash::Hasher;
use std::{cell::RefCell, cmp::min, fs::File, io::{self, BufWriter, Seek, SeekFrom, Write}, mem, num::NonZeroUsize, rc::Rc, slice};

use fnv::FnvHasher;
use lru::LruCache;
use memmap2::MmapOptions;
use ziggurat_varint::EncodeVarint;

use crate::container::BomEntry;

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

    pub unsafe fn encode_compressed_to_container_file<I>(values: I, n: usize, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64) where I: Iterator<Item=(i64, i64)> {
        const INTSIZE: usize =  mem::size_of::<i64>();
        let m = (n-1) / 16 + 1; // worst case number of blocks = no overflow items
        let headlen = INTSIZE + (m * 2 * INTSIZE);

        // map constant header
        file.set_len(start_offset + headlen as u64).unwrap();
        let mut mmap = unsafe { MmapOptions::new().offset(start_offset).len(headlen).map_mut(&*file).unwrap()};
        let r = unsafe { (mmap.as_mut_ptr() as *mut i64).as_mut().unwrap() };
        let sync = unsafe { slice::from_raw_parts_mut(mmap.as_mut_ptr().offset(INTSIZE as isize) as *mut (i64, usize), m) };
        // since the size of sync is not known in advance it's preempively mapped as int[m][2].
        // Its final size will be int[mr][2]. mr can only be calculated after the number of regular items in all blocks (r) is known,
        // i.e. after encoding all blocks. If mr < m there would be empty space between sync and data in the final file, thus we encode
        // to a separate file first and copy data to the container at the end.

        let tmpfile = tempfile::tempfile().unwrap();
        let mut writer = BufWriter::new(tmpfile);

        let mut values = values.take(n);

        let mut buffer = [0u8; 9*17]; // byte buffer for encoded data
        *r = 0; // zero total number of regular items
        let mut total_overflow = 0; // total number of overflow items in blocks
        let mut keys = Vec::with_capacity(16); // keys of the current block
        let mut positions = Vec::with_capacity(100); // values of the current block
        let mut bi = 0; // runnign block index
        let mut boffset = 0; // relative starting offset of the current block

        'outer: loop {
            let mut overflow = 0i64;

            // collect 16 regular items (or padding)
            while keys.len() < 16 {
                match values.next() {
                    Some((key, position)) => {
                        // 
                        keys.push(key);
                        positions.push(position);
                        *r += 1; // count regular items
                    }

                    None => {
                        // generate padding
                        keys.push(-1);
                        positions.push(-1);
                    }
                }
            }

            // if the iterator has more values:
            // add overflow items or encode and continue to next block
            while let Some((key, position)) = values.next() {
                if key == keys[15] {
                    // add overflow item
                    positions.push(position);
                    overflow += 1;
                } else {
                    total_overflow += overflow as usize;

                    // encode block and continue with next
                    let mut blen = overflow.encode_varint_into(&mut buffer);
                    blen += ziggurat_varint::encode_delta_block_into(&keys, &mut buffer[blen..]);
                    writer.write_all(&buffer[..blen]).unwrap();

                    let encoded_positions = ziggurat_varint::encode_delta_block(&positions);
                    blen += encoded_positions.len();
                    writer.write_all(&encoded_positions).unwrap();

                    sync[bi] = (keys[0], boffset);
                    bi += 1;
                    boffset += blen;

                    keys.clear();
                    positions.clear();

                    // add consumed iter value to next block
                    keys.push(key);
                    positions.push(position);
                    *r += 1;

                    continue 'outer;
                }
            }

            // else (iterator is empty):
            // encode and write last block
            total_overflow += overflow as usize;
            let mut blen = overflow.encode_varint_into(&mut buffer);
            blen += ziggurat_varint::encode_delta_block_into(&keys, &mut buffer[blen..]);
            writer.write_all(&buffer[..blen]).unwrap();

            let encoded_positions = ziggurat_varint::encode_delta_block(&positions);
            blen += encoded_positions.len();
            writer.write_all(&encoded_positions).unwrap();

            sync[bi] = (keys[0], boffset);
            boffset += blen;

            break;
        }
        writer.flush().unwrap();
        let mut tmpfile = writer.into_inner().unwrap();

        assert!(n == *r as usize + total_overflow, "encoded different number of values than specified");

        // copy encoded data from tmp file into container
        let mr = (*r as usize - 1) / 16 + 1; // actual number of blocks
        let headlen = INTSIZE + (mr * 2 * INTSIZE); // actual header size

        file.seek(SeekFrom::Start(start_offset + headlen as u64)).unwrap();
        tmpfile.seek(SeekFrom::Start(0)).unwrap();
        io::copy(&mut tmpfile, file).unwrap();
        file.flush().unwrap();

        bom_entry.size = (headlen + boffset) as i64;
        bom_entry.param1 = n as i64;
        bom_entry.param2 = 0;
    }

    pub unsafe fn encode_uncompressed_to_container_file<I>(values: I, n: usize, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64) where I: Iterator<Item=(i64, i64)> {
        file.seek(SeekFrom::Start(start_offset)).unwrap();

        // write data
        let mut written = 0;
        let mut writer = BufWriter::new(file);
        for (k, v) in values.take(n) {
            writer.write_all(&k.to_le_bytes()).unwrap();
            writer.write_all(&v.to_le_bytes()).unwrap();
            written += 1;
        }
        writer.flush().unwrap();
        assert!(written == n, "could not write all values");

        bom_entry.size = (written * mem::size_of::<i64>() * 2) as i64;
        bom_entry.param1 = n as i64;
        bom_entry.param2 = 0;
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
pub struct IndexBlock {
    regular_items: usize,
    overflow_items: usize,
    keys: [i64; 16],
    positions: Vec<i64>,
}

impl IndexBlock {

    /// Decodes a block from compressed raw data.
    pub fn decode(data: &[u8], regular_items: usize) -> Self {
        // decode the number of overflow items in block
        // this should be:
        //  - overflow_items = 0 when regular_items < B (16)
        //  - overflow_items > 0 when regular_items >= B (16)
        let (overflow_items, mut offset) = ziggurat_varint::decode(data);

        // decode the 16 keys always present in block
        let (keys, readlen) = ziggurat_varint::decode_delta_array(&data[offset..]);
        offset += readlen;

        // decode the first regular_items, max B = 16
        let (positions, _) =
            ziggurat_varint::decode_fixed_delta_block(&data[offset..], regular_items + overflow_items as usize);

        Self {
            regular_items,
            overflow_items: overflow_items as usize,
            keys,
            positions,
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
    pub fn get_pair(&self, index: usize) -> Option<(i64, i64)> {
        self.get_key(index)
            .zip(self.get_position(index))
    }

    /// Returns a slice over all positions in the block including all overflow items.
    /// This action may decode additional overflow items.
    pub fn get_all_position_(&self) -> &[i64] {
        &self.positions[..]
    }

    /// Returns the value of a single position from the block.
    /// This action may decode additional overflow items.
    pub fn get_position(&self, index: usize) -> Option<i64> {
        self.positions.get(index).copied()
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
    cache: LruCache<usize, Rc<IndexBlock>>,
}

impl<'map> IndexBlockCache<'map> {
    pub fn new(r: usize, sync: &'map [(i64, usize)], data: &'map [u8]) -> Self {
        Self {
            r,
            sync,
            data,
            cache: LruCache::new(NonZeroUsize::new(500).unwrap())
        }
    }

    pub fn sync_block_position(&self, key: i64) -> usize {
        Index::sync_block_position(self.sync, key)
    }

    /// Returns the reference to a cached IndexBlock.
    /// If the is not yet in the cache it will be decoded.
    pub fn get_block(&mut self, block_index: usize) -> Option<Rc<IndexBlock>> {
        if block_index < self.sync.len() {
            if !self.cache.contains(&block_index) {
                let offset = self.sync[block_index].1 as usize;
                let br = min(self.r - (block_index * 16), 16);
                let block = Rc::new(IndexBlock::decode(&self.data[offset..], br));
                self.cache.put(block_index, block);
            }
    
            self.cache
                .get(&block_index)
                .map(|rc| rc.clone())
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
        block: Rc<IndexBlock>,
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
                let mut cache = cache.borrow_mut();

                let block_index = cache.sync_block_position(key);
                let block = cache.get_block(block_index).expect("at this point the block must be cached");

                // partition_point() will result in Some(position), even if the key is
                // not actually in the block. This is fine, since the iterator will
                // check the key at a later point again.
                let position = block.keys().partition_point(|&x| x < key );
                if position >= block.keys().len() {
                    return Self::None;
                }

                Self::Compressed {
                    block,
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

            Self::Compressed { block, position, key } => {
                let value = block.get_pair(*position)
                    .filter(|(k, _)| k == key)
                    .map(|(_, v)| v);
                *position += 1;
                value
            }
        }
    }
}
