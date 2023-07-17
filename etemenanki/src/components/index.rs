use core::hash::Hasher;
use std::cmp::min;

use fnv::FnvHasher;

pub trait FnvHash {
    fn fnv_hash(&self) -> u64;
}

impl<T> FnvHash for T
where
    T: AsRef<[u8]>,
{
    fn fnv_hash(&self) -> u64 {
        let mut hasher = FnvHasher::default();
        hasher.write(self.as_ref());
        hasher.finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Index<'map> {
    Compressed {
        length: usize,
        r: usize,
        sync: &'map [(u64, u64)],
        data: &'map [u8],
    },

    Uncompressed {
        length: usize,
        pairs: &'map [(u64, i64)],
    },
}

impl<'map> Index<'map> {
    #[inline]
    pub fn contains_key(&self, key: u64) -> bool {
        match self.get_first(key) {
            Some(_) => true,
            None => false,
        }
    }

    pub fn compressed_from_parts(
        n: usize,
        r: usize,
        sync: &'map [(u64, u64)],
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
    pub fn get_first(&self, key: u64) -> Option<i64> {
        match *self {
            Index::Compressed { .. } => self.get_all(key).next(),

            Index::Uncompressed { length: _, pairs } => match Self::position(pairs, key) {
                Some(i) => Some(pairs[i].1),
                None => None,
            },
        }
    }

    #[inline]
    pub fn get_all(&self, key: u64) -> IndexIterator {
        IndexIterator::new(*self, key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Index::Compressed { length, .. } => *length,
            Index::Uncompressed { length, .. } => *length,
        }
    }

    fn block_position(sync: &[(u64, u64)], key: u64) -> usize {
        match sync.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(bi) => bi,
            Err(0) => 0,
            Err(nbi) => nbi - 1,
        }
    }

    fn position(pairs: &[(u64, i64)], key: u64) -> Option<usize> {
        match pairs.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => Some(i),
            Err(_) => None,
        }
    }

    pub fn uncompressed_from_parts(n: usize, pairs: &'map [(u64, i64)]) -> Self {
        Self::Uncompressed { length: n, pairs }
    }
}

pub enum IndexIterator<'map> {
    None,

    Compressed {
        data: &'map [u8],
        position: usize,
        len: usize,
    },

    Uncompressed {
        pairs: &'map [(u64, i64)],
        key: u64,
        position: usize,
    },
}

impl<'map> IndexIterator<'map> {
    pub fn new(index: Index<'map>, key: u64) -> Self {
        match index {
            Index::Compressed {
                length: _,
                r,
                sync,
                data,
            } => {
                let bi = Index::block_position(sync, key);
                let mut offset = sync[bi].1 as usize - (8 + (sync.len() * 16));

                let (o, readlen) = ziggurat_varint::decode(&data[offset..]);
                offset += readlen;

                // read keys vector
                let klen = min(r - (bi * 16), 16); // number of keys can be <16
                let mut keys = Vec::with_capacity(16);
                for _ in 0..klen {
                    let (k, readlen) = ziggurat_varint::decode(&data[offset..]);
                    keys.push(k as u64);
                    offset += readlen;
                }

                match keys.binary_search(&key) {
                    // key not in block
                    Err(_) => Self::None,

                    // key in block at i
                    Ok(ki) => {
                        // determine number of elements with key in block
                        let mut len = keys.iter().filter(|&x| *x == key).count();
                        // add overflow items if key is the last in block
                        if keys[keys.len()] == key {
                            len += o as usize;
                        }

                        // discard first ki values in block
                        for _ in 0..ki {
                            let (_, readlen) = ziggurat_varint::decode(&data[offset..]);
                            offset += readlen;
                        }

                        Self::Compressed {
                            data: &data[offset..],
                            position: 0,
                            len,
                        }
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
            } => {
                if *position > len {
                    let (v, readlen) = ziggurat_varint::decode(data);
                    *data = &data[readlen..];
                    *position += 1;
                    Some(v)
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
