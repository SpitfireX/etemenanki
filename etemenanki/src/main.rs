#![feature(iter_intersperse)]

use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::Result;
use std::num::NonZeroUsize;
use std::thread::panicking;

use etemenanki::components::{FnvHash, VecSlice, Vector};
use etemenanki::container::Container;
use etemenanki::{container, Datastore};
use lru::LruCache;
use memmap2::Mmap;
use streaming_iterator::StreamingIterator;

fn main() -> Result<()> {
    // let args: Vec<String> = env::args().collect();

    // let datastore = Datastore::open(&args[1]).expect("could not open datastore");

    // let words = datastore["primary"]["word"]
    //     .as_indexed_string()
    //     .unwrap();

    // let heads = datastore["primary"]["head-index"]
    //     .as_pointer()
    //     .unwrap();

    // let relation = datastore["primary"]["relation"]
    //     .as_indexed_string()
    //     .unwrap();

    // for cpos in 0..words.len() {
    //     let word = words.get(cpos).unwrap();
    //     if let Some(head) = heads.get(cpos) {
    //         let relation = relation.get(cpos).unwrap();
    //         let head_word = words.get(head).unwrap();
    //         println!("{}: {} --> {}: {} ({})", cpos, word, head, head_word, relation);
    //     } else {
    //         println!("{}: {}", cpos, word);
    //     }
    // }

    // let strings = datastore["primary"]["token"]
    //     .as_indexed_string()
    //     .unwrap();

    // let pos = datastore["primary"]["pos"]
    //     .as_indexed_string()
    //     .unwrap();

    // let s = datastore["s"].as_segmentation().unwrap();
    // let text = datastore["text"].as_segmentation().unwrap();

    // let tests = [
    //     "Schinken",
    //     "Tortellini",
    //     "Hallo",
    //     "Cremefine",
    //     "Quäse",
    //     "Rahm",
    //     "Sahne",
    //     "Schlagsahne",
    //     "Tofu",
    // ];

    // let posses: HashSet<_> = pos.lexicon().all_starting_with("VVI").collect_strs();

    // for test in tests {
    //     let result = strings.index().get_first(test.fnv_hash());
    //     match result {
    //         Some(i) => {
    //             println!("{} in index at {}", test, i);
    //             let positions: Vec<_> = strings.inverted_index().postings(i as usize).collect();

    //             let mut usage = HashSet::new();
    //             for p in positions {
    //                 let nextpos = pos.get_unchecked(p + 1);
    //                 if posses.contains(nextpos) {
    //                     usage.insert(strings.get_unchecked(p + 1));

    //                     let sid = s.find_containing(p).unwrap();
    //                     let (start, end) = s.get_unchecked(sid);

    //                     let surface: String = strings
    //                         .get_range(start, end)
    //                         .map(|str| {
    //                             if str == test {
    //                                 format!("|{}|", str)
    //                             } else {
    //                                 str.to_owned()
    //                             }
    //                         })
    //                         .intersperse(" ".to_owned())
    //                         .collect();
    //                     println!("{}", surface);

    //                     let tid = text.find_containing(p).unwrap();
    //                     let title = &text["title"].as_plain_string().unwrap()[tid];
    //                     let author = &text["author"].as_indexed_string().unwrap()[tid];
    //                     let url = &text["url"].as_plain_string().unwrap()[tid];
    //                     let year = &text["year"]
    //                         .as_integer()
    //                         .unwrap()
    //                         .get_unchecked(tid);
    //                     let keywords = &text["keywords"].as_set().unwrap().get_unchecked(tid);
    //                     let ingredients = &text["ingredients"].as_set().unwrap().get_unchecked(tid);
    //                     println!(
    //                         "text {} with title \"{}\" from {} by {} at url {} with keywords {:?} using ingredients {:?}\n",
    //                         tid, title, year, author, url, keywords, ingredients
    //                     );
    //                 }
    //             }

    //             println!(
    //                 "{} => {}",
    //                 test,
    //                 usage.into_iter().intersperse(", ").collect::<String>()
    //             );
    //         }
    //         None => println!("{} not in index", test),
    //     }
    // }

    // let all2016: Vec<_> = text.variable_by_name("year")
    //     .unwrap()
    //     .as_integer()
    //     .unwrap()
    //     .get_all(2016)
    //     .collect();

    // println!("\nthere are {} texts from 2016", all2016.len());

    let filename = "../scripts/recipes4000/s/s.zigl";
    let file = File::open(filename).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let container = Container::from_mmap(mmap, "word".to_owned()).unwrap();

    let vector = container
        .components
        .get("RangeStream")
        .unwrap()
        .as_vector()
        .unwrap();

    let mut cached = CachedVector::new(*vector);
    let mut iter = cached.column_iter_range(0, 10, 15).unwrap();

    while let Some(row) = iter.next() {
        println!("{:?}", row);
    }
    println!("loop done");
    println!("{:?}", iter.next());

    Ok(())
}

struct CachedVector<'map> {
    inner: Vector<'map>,
    cache: LruCache<usize, Vec<i64>>,
}

impl<'map> CachedVector<'map> {
    /// Decodes a compressed block and returns it as a contiguous Vec of dimension n*d in row major order.
    fn decode_compressed_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
        let mut block = vec![0i64; d * 16];
        let mut offset = 0;

        for i in 0..d {
            for j in 0..16 {
                let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
                block[(j * d) + i] = int; // wonky because conversion from col-major to row-major
                offset += len;
            }
        }

        block
    }

    /// Decodes a delta compressed block and returns it as a contiguous Vec of dimension n*d in row-major order.
    fn decode_delta_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
        let mut delta_block = vec![0i64; d * 16];
        let mut offset = 0;

        for i in 0..d {
            for j in 0..16 {
                let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
                let current = (j * d) + i;
                if j == 0 {
                    delta_block[current] = int; // initial seed values
                } else {
                    let last = ((j - 1) * d) + i;
                    delta_block[current] = delta_block[last] + int;
                }
                offset += len;
            }
        }

        delta_block
    }

    pub fn new(vector: Vector<'map>) -> Self {
        Self {
            inner: vector,
            cache: LruCache::new(NonZeroUsize::new(100).unwrap()),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn width(&self) -> usize {
        self.inner.width()
    }

    pub fn get_row(&mut self, index: usize) -> Option<&[i64]> {
        if index < self.len() {
            Some(self.get_row_unchecked(index))
        } else {
            None
        }
    }

    fn row_index_to_block_offsets(&self, index: usize) -> (usize, usize, usize) {
        let bi = index / 16;
        let start = (index % 16) * self.width();
        let end = start + self.width();
        (bi, start, end)
    }

    // Returns Some(row) if the row is immediately available (either uncompressed or already decoded in the cache)
    // else None. This method never decodes any new blocks and doesn't modify the cache's LRU list.
    fn peek_row (&self, index: usize) -> Option<&[i64]> {
        match self.inner {
            Vector::Uncompressed { .. } => {
                let row = self.inner.get_row_unchecked(index);
                if let VecSlice::Borrowed(s) = row {
                    Some(s)
                } else {
                    panic!("in case of uncompressed vec the row must be a slice into the mmap");
                }
            }

            Vector::Compressed { .. } | Vector::Delta { .. } => {
                let (bi, start, end) = self.row_index_to_block_offsets(index);
                self.cache.peek(&bi)
                    .map(|b| &b[start..end])
            }
        }
    }

    pub fn get_row_unchecked(&mut self, index: usize) -> &[i64] {
        match self.inner {
            Vector::Uncompressed { .. } => {
                let row = self.inner.get_row_unchecked(index);
                if let VecSlice::Borrowed(s) = row {
                    s
                } else {
                    panic!("in case of uncompressed vec the row must be a slice into the mmap");
                }
            }
            Vector::Delta { .. } | Vector::Compressed { .. } => {
                let (bi, start, end) = self.row_index_to_block_offsets(index);

                // decode and cache block if needed
                if !self.cache.contains(&bi) {
                    println!("cache miss, decoding block {}", bi);
                    let block = match self.inner {
                        Vector::Uncompressed { .. } => panic!("n/a for uncompressed vector"),
                        
                        Vector::Compressed { length: _, width: _, sync, data } => {
                            let block_start = sync[bi] as usize;
                            Self::decode_compressed_block(self.width(), &data[block_start..])
                        }
        
                        Vector::Delta { length: _, width: _, sync, data } => {
                            let block_start = sync[bi] as usize;
                            Self::decode_delta_block(self.width(), &data[block_start..])
                        }
                    };
        
                    self.cache.put(bi, block);
                }

                // return reference into cache
                let block = self.cache
                    .get(&bi)
                    .expect("at this point the block must be cached");
                &block[start..end]
            }
        }
    }

    pub fn column_iter(&mut self, column: usize) -> Option<ColumnIter<'_, 'map>> {
        let len = self.len();
        if column < self.width() {
            Some(ColumnIter::new(column, 0, len, self))
        } else {
            None
        }
    }

    pub fn column_iter_from(&mut self, column: usize, start: usize) -> Option<ColumnIter<'_, 'map>> {
        let len = self.len();
        if column < self.width() {
            Some(ColumnIter::new(column, start, len, self))
        } else {
            None
        }
    }

    pub fn column_iter_range(&mut self, column: usize, start: usize, end: usize) -> Option<ColumnIter<'_, 'map>> {
        let len = self.len();
        if column < self.width() && end < len {
            Some(ColumnIter::new(column, start, end, self))
        } else {
            None
        }
    }

    pub fn iter(&mut self) -> RowIter<'_, 'map> {
        let len = self.len();
        RowIter::new(0, len, self)
    }

    pub fn iter_from(&mut self, start: usize) -> RowIter<'_, 'map> {
        let len = self.len();
        RowIter::new(start, len, self)
    }

    pub fn iter_range(&mut self, start: usize, end: usize) -> Option<RowIter<'_, 'map>> {
        let len = self.len();
        if end <= len {
            Some(RowIter::new(start, end, self))
        } else {
            None
        }
    }
}

struct ColumnIter<'cv, 'map> {
    cvec: &'cv mut CachedVector<'map>,
    col: usize,
    position: usize,
    end: usize,
}

impl <'cv, 'map> ColumnIter<'cv, 'map> {
    pub fn new(column: usize, start: usize, end: usize, cvec: &'cv mut CachedVector<'map>) -> Self {
        Self {
            cvec,
            col: column,
            position: start,
            end,
        }
    }
}

impl <'cv, 'map> StreamingIterator for ColumnIter<'cv, 'map> {
    type Item = i64;

    fn advance(&mut self) {
        if self.position <= self. end {
            self.cvec.get_row(self.position);
            self.position += 1;
        }
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.position <= self.end {
            let (bi, start, _) = self.cvec.row_index_to_block_offsets(self.position - 1);
            self.cvec
                .cache.peek(&bi)
                .map(|b| &b[start + self.col])
        } else {
            None
        }
    }
}

struct RowIter<'cv, 'map> {
    cvec: &'cv mut CachedVector<'map>,
    position: usize,
    end: usize,
}

impl <'cv, 'map> RowIter<'cv, 'map> {
    pub fn new(start: usize, end: usize, cvec: &'cv mut CachedVector<'map>) -> Self {
        Self {
            cvec,
            position: start,
            end
        }
    }
}

impl <'cv, 'map> StreamingIterator for RowIter<'cv, 'map> {
    type Item = [i64];

    fn advance(&mut self) {
        if self.position <= self.end {
            self.cvec.get_row(self.position);
            self.position += 1;
        }
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.position <= self.end {
            self.cvec.peek_row(self.position-1) // -1 because get always gets called after advace
        } else {
            None
        }
    }
}
