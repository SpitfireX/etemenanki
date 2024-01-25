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
    let mut container = Container::from_mmap(mmap, "word".to_owned()).unwrap();

    let vector = container
        .components
        .get("RangeStream")
        .unwrap()
        .as_vector()
        .unwrap();

    let mut cached = CachedVector::new(*vector);

    for i in 0..20 {
        println!("{:?}", cached.get_row_unchecked(i));
    }

    Ok(())
}

struct CachedVector<'map> {
    inner: Vector<'map>,
    cache: LruCache<usize, Vec<i64>>,
}

impl<'map> CachedVector<'map> {
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

    fn decode_compressed_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
        let mut block = vec![0i64; d*16];

        let mut offset = 0;
        for i in 0..d {
            for j in 0..16 {
                let (int, len) = ziggurat_varint::decode(&raw_data[offset..]);
                block[(j*d) + i] = int; // wonky becaus conversion from col-major to row-major
                offset += len;
            }
        }

        block
    }

    fn decode_delta_block(d: usize, raw_data: &[u8]) -> Vec<i64> {
        let mut delta_block = Self::decode_compressed_block(d, raw_data);

        for i in 0..d {
            for j in 1..16 {
                let current = (j*d) + i;
                let last = ((j-1)*d) + i;
                delta_block[current] = delta_block[last] + delta_block[current];
            }
        }

        delta_block
    }

    fn get_block(&mut self, block_index: usize) -> &[i64] {
        if !self.cache.contains(&block_index) {
            println!("cache miss: {}", block_index);
            let block = match self.inner {
                Vector::Uncompressed { .. } => panic!("n/a for uncompressed vector"),
                
                Vector::Compressed { length: _, width: _, sync, data } => {
                    let block_start = sync[block_index] as usize;
                    Self::decode_compressed_block(self.width(), &data[block_start..])
                }

                Vector::Delta { length: _, width: _, sync, data } => {
                    let block_start = sync[block_index] as usize;
                    Self::decode_delta_block(self.width(), &data[block_start..])
                }
            };

            self.cache.put(block_index, block);
        } else {
            println!("cache hit: {}", block_index);
        }

        let cached_block = self
            .cache
            .get(&block_index)
            .expect("at this point a block must be already cached");
        &cached_block[..]
    }

    pub fn get_row(&mut self, index: usize) -> Option<&[i64]> {
        if index < self.len() * self.width() {
            Some(self.get_row_unchecked(index))
        } else {
            None
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
                let start = (index % 16) * self.width();
                let end = start + self.width();
                let bi = index / 16;
                let block = self.get_block(bi);
                &block[start..end]
            }
        }
    }
}
