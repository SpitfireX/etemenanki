use core::hash::Hasher;

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
        sync: &'map [i64],
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

    pub fn compressed_from_parts(n: usize, r: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
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
            Index::Compressed { length, r, sync, data } => todo!(),

            Index::Uncompressed { length: _, pairs } => match self.position(key) {
                Some(i) => Some(pairs[i].1),
                None => None,
            },
        }
    }

    #[inline]
    pub fn get_all(&self, key: u64) -> IndexIterator {
        IndexIterator {
            index: *self,
            key,
            position: self.position(key),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Index::Compressed { length, .. } => *length,
            Index::Uncompressed { length, .. } => *length,
        }
    }

    #[inline]
    pub fn position(&self, key: u64) -> Option<usize> {
        match *self {
            Index::Compressed { length, r, sync, data } => todo!(),
            
            Index::Uncompressed { length: _, pairs } => {
                match pairs.binary_search_by_key(&key, |(k, _)| *k) {
                    Ok(i) => Some(i),
                    Err(_) => None,
                }
            }
        }
    }

    pub fn uncompressed_from_parts(n: usize, pairs: &'map [(u64, i64)]) -> Self {
        Self::Uncompressed { length: n, pairs }
    }
}

pub struct IndexIterator<'a> {
    index: Index<'a>,
    key: u64,
    position: Option<usize>,
}

impl<'a> Iterator for IndexIterator<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self.index {
            Index::Compressed { length, r, sync, data } => todo!(),

            Index::Uncompressed { length: _, pairs } => match self.position {
                None => None,
                Some(i) => {
                    if i < pairs.len() && pairs[i].0 == self.key {
                        let value = pairs[i].1;
                        self.position = Some(i + 1);
                        Some(value)
                    } else {
                        None
                    }
                }
            },
        }
    }
}
