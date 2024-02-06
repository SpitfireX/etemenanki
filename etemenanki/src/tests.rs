use std::{fs::File, num::NonZeroUsize};

use lru::LruCache;
use memmap2::Mmap;
use test::{Bencher, black_box};
use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng};

use crate::{components::{CachedIndex, CachedInvertedIndex, CachedVector, Index, IndexBlock, InvertedIndex, Vector, VectorBlock}, container::Container, layers::SegmentationLayer};

const DATASTORE_PATH: &'static str = "testdata/simpledickens/";

fn vec_setup(filename: &'static str, component_name: &'static str) -> (Vector<'static>, Container<'static>) {
    let file = File::open(DATASTORE_PATH.to_owned() + filename).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let container = Container::from_mmap(mmap, "word".to_owned()).unwrap();

    let vec = *container
        .components
        .get(component_name)
        .unwrap()
        .as_vector()
        .unwrap();

    (vec, container)
}

#[test]
fn vec() {
    let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
    assert!(vec.len() == 3407085);
    assert!(vec.get_row(10).unwrap()[0] == 40);
}

#[bench]
fn vec_seq_no(b: &mut Bencher) {
    let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
    b.iter(|| {
        for i in 0..vec.len() {
            black_box(vec.get_row(i));
        }
    })
}

#[bench]
fn vec_seq_cached(b: &mut Bencher) {
    let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
    b.iter(|| {
        let cvec = CachedVector::<1>::new(vec).unwrap();
        for i in 0..vec.len() {
            black_box(cvec.get_row(i));
        }
    })
}

#[bench]
fn vec_seq_cached_iter(b: &mut Bencher) {
    let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
    b.iter(|| {
        let cvec = CachedVector::<1>::new(vec).unwrap();
        for row in cvec.iter() {
            black_box(row);
        }
    })
}

fn setup_rand(len: usize, max: usize) -> Vec<usize> {
    let rng = StdRng::seed_from_u64(42);
    let dist = Uniform::new(0, max);
    Vec::from_iter(dist.sample_iter(rng).take(len))
}

const NACCESS: usize = 1_000_000;

#[bench]
fn vec_rand_no(b: &mut Bencher) {
    let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
    let ids = setup_rand(NACCESS, vec.len());
    b.iter(|| {
        for i in &ids {
            black_box(vec.get_row(*i));
        }
    })
}

#[bench]
fn vec_rand_cached(b: &mut Bencher) {
    let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
    let ids = setup_rand(NACCESS, vec.len());
    b.iter(|| {
        let cached = CachedVector::<1>::new(vec).unwrap();
        for i in &ids {
            black_box(cached.get_row(*i));
        }
    })
}

fn idxcmp_setup(filename: &'static str, component_name: &'static str) -> (Index<'static>, Container<'static>) {
    let file = File::open(DATASTORE_PATH.to_owned() + filename).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let container = Container::from_mmap(mmap, "test".to_owned()).unwrap();

    let index = *container
        .components
        .get(component_name)
        .unwrap()
        .as_index()
        .unwrap();

    (index, container)
}

#[test]
fn idxcmp_block() {
    let (index, _container) = idxcmp_setup("chapter/num.zigv", "IntSort");
    if let Index::Compressed { length, r, sync, data } = index {
        println!("\n index len {} with r {}", length, r);
        for (i, (_, o)) in sync.iter().enumerate(){
            let br = if i < sync.len()-1 {
                16
            } else {
                ((r - 1) & 0x0f) + 1
            };
            let mut block = IndexBlock::decode(&data[*o..], br);
            
            println!("block {}: r {}, o {}", i, block.regular_items(), block.overflow_items());
            println!("keys: {:?}", block.keys());
            println!("positions: {:?}", block.get_all_position_());
        }
    } else {
        panic!("index not compressed");
    }
}

#[test]
fn idx_iter() {
    let (index, _container) = idxcmp_setup("chapter/num.zigv", "IntSort");
    println!();
    println!("{:?}", index.get_all(0).collect::<Vec<_>>());
    println!("{:?}", index.get_all(1).collect::<Vec<_>>());
    println!("{:?}", index.get_all(9001).collect::<Vec<_>>());
}

#[test]
fn cachedidx_iter() {
    let (index, _container) = idxcmp_setup("chapter/num.zigv", "IntSort");
    let cidx = CachedIndex::new(index);
    println!();
    println!("{:?}", cidx.get_all(0).collect::<Vec<_>>());
    println!("{:?}", cidx.get_all(3).collect::<Vec<_>>());
    println!("{:?}", cidx.get_all(2002).collect::<Vec<_>>());
    println!("{:?}", cidx.get_all(2003).collect::<Vec<_>>());
    println!("{:?}", cidx.get_all(9001).collect::<Vec<_>>());
}

fn seg_setup(filename: &'static str) -> SegmentationLayer<'static> {
    let file = File::open(DATASTORE_PATH.to_owned() + filename).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let container = Container::from_mmap(mmap, "word".to_owned()).unwrap();

    let seg = SegmentationLayer::try_from(container).unwrap();

    seg
}

#[test]
fn seg_containing() {
    let seg = seg_setup("s/s.zigl");
    assert!(seg.find_containing(0) == Some(0));
    assert!(seg.find_containing(10) == Some(2));
    assert!(seg.find_containing(9001) == Some(494));
    assert!(seg.find_containing(3407085) == None);
}

#[test]
fn vec_block_decode() {
    let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
    let bdata = match vec {
        Vector::Uncompressed { .. } => panic!(),
        Vector::Compressed { length: _, width: _, sync, data } |
        Vector::Delta { length: _, width: _, sync, data } => &data[sync[10] as usize..],
    };

    let b1 = Vector::decode_compressed_block(1, bdata);
    let b2 = VectorBlock::<1>::decode_compressed(bdata, 16);

    assert!(b2.rows().iter().flatten().eq(b1.iter()));
}

#[test]
fn vec_deltablock_decode() {
    let (vec, _c) = vec_setup("s/s.zigl", "RangeStream");
    let bdata = match vec {
        Vector::Uncompressed { .. } => panic!(),
        Vector::Compressed { length: _, width: _, sync, data } |
        Vector::Delta { length: _, width: _, sync, data } => &data[sync[10] as usize..],
    };

    let b1 = Vector::decode_delta_block(2, bdata);
    let b2 = VectorBlock::<2>::decode_delta(bdata, 16);

    assert!(b2.rows().iter().flatten().eq(b1.iter()));
}

#[test]
fn vec_deltablock_decode_len() {
    let (vec, _c) = vec_setup("s/s.zigl", "RangeStream");
    let bdata = match vec {
        Vector::Uncompressed { .. } => panic!(),
        Vector::Compressed { length: _, width: _, sync, data } |
        Vector::Delta { length: _, width: _, sync, data } => &data[*sync.last().unwrap() as usize..],
    };
    let lastlen = vec.len() % 16;

    let b1 = Vector::decode_delta_block(2, bdata);
    let b2 = VectorBlock::<2>::decode_delta(bdata, lastlen);

    assert!(b2.len() == 7);
    assert!(b2.rows().len() == 7);
    assert!(&b1[..2] == b2.rows()[0]);
}

#[test]
fn vec_cached2_access() {
    let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
    let cvec2 = CachedVector::<1>::new(vec).unwrap();

    assert!(cvec2.len() == 3407085);
    assert!(cvec2.get_row(0) == Some([195]));
    assert!(cvec2.get_row(1234567) == Some([655]));
    assert!(cvec2.get_row(3407084) == Some([2]));
    assert!(cvec2.get_row(3407085) == None);
}

#[test]
fn vec_cached2_iter() {
    let (vec, _c) = vec_setup("s/s.zigl", "RangeStream");
    let cvec2 = CachedVector::<2>::new(vec).unwrap();

    let all: Vec<_> = cvec2.iter().collect();
    assert!(all.len() == cvec2.len());
    
    let start: Vec<_> = cvec2.iter_until(100).unwrap().collect();
    assert!(start.len() == 100);

    let end: Vec<_> = cvec2.iter_from(100).collect();
    assert!(end.len() == cvec2.len()-100);
    
    let middle: Vec<_> = cvec2.iter_range(100, 110).unwrap().collect();
    assert!(middle.len() == 10);
}

#[test]
fn invidx_cache_eval() {
    for size in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000] {
        // println!("\nTesting with cache size: {}", size);
        let (vec, _c) = vec_setup("word.zigv", "LexIDStream");
        let cvec = CachedVector::<1>::new(vec).unwrap();

        let mut typecache: LruCache<i64, ()> = LruCache::new(NonZeroUsize::new(size).unwrap());
        let mut accesses = 0;
        let mut hits = 0;

        for id in cvec.column_iter(0) {
            accesses += 1;
            match typecache.put(id, ()) {
                Some(_) => hits += 1,
                None => (),
            }
        }

        // println!("total accesses: {}, hits: {}, hit ratio: {}", accesses, hits, hits as f32 / accesses as f32);
        println!("({}, {}),", size, hits as f32 / accesses as f32);

        // let mut cachestate: Vec<_> = typecache.iter().map(|(k, _)| k).collect();
        // cachestate.sort();
        // println!("blocks in cache: {:?}", cachestate);
    }
}

fn invidx_setup(filename: &'static str, vec_name: &'static str, invidx_name: &'static str) -> (Vector<'static>, InvertedIndex<'static>, Container<'static>) {
    let file = File::open(DATASTORE_PATH.to_owned() + filename).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let container = Container::from_mmap(mmap, "test".to_owned()).unwrap();

    let vec = *container
        .components
        .get(vec_name)
        .unwrap()
        .as_vector()
        .unwrap();

    let invidx = *container
        .components
        .get(invidx_name)
        .unwrap()
        .as_inverted_index()
        .unwrap();

    (vec, invidx, container)
}

#[bench]
fn invidx_decode_no(b: &mut Bencher) {
    let (lexids, invidx, _c) = invidx_setup("word.zigv", "LexIDStream", "LexIDIndex");
    b.iter(|| {
        let cvec = CachedVector::<1>::new(lexids).unwrap();
        for [id, ..] in cvec.iter() {
            for position in invidx.postings(id as usize) {
                black_box(position);
            }
        }
    });
}

#[test]
fn cachedinvidx() {
    let (_, invidx, _c) = invidx_setup("word.zigv", "LexIDStream", "LexIDIndex");
    let cinvidx = CachedInvertedIndex::new(invidx);

    println!("{:?}", cinvidx.positions(0).unwrap().collect::<Vec<_>>());
}

#[bench]
fn invidx_decode_cache(b: &mut Bencher) {
    let (lexids, invidx, _c) = invidx_setup("word.zigv", "LexIDStream", "LexIDIndex");
    b.iter(|| {
        let cvec = CachedVector::<1>::new(lexids).unwrap();
        let cinvidx = CachedInvertedIndex::new(invidx);
        for [id, ..] in cvec.iter() {
            for position in cinvidx.positions(id as usize).unwrap() {
                black_box(position);
            }
        }
    });
}

#[bench]
fn invidx_decode0_no(b: &mut Bencher) {
    let (_, invidx, _c) = invidx_setup("word.zigv", "LexIDStream", "LexIDIndex");
    b.iter(|| {
        for position in invidx.postings(0) {
            black_box(position);
        }
    });
}

#[bench]
fn invidx_decode0_cache(b: &mut Bencher) {
    let (_, invidx, _c) = invidx_setup("word.zigv", "LexIDStream", "LexIDIndex");
    let cinvidx = CachedInvertedIndex::new(invidx);
    b.iter(|| {
        for position in cinvidx.positions(0).unwrap() {
            black_box(position);
        }
    });
}

#[bench]
fn invidx_decode0_cache_cold(b: &mut Bencher) {
    let (_, invidx, _c) = invidx_setup("word.zigv", "LexIDStream", "LexIDIndex");
    b.iter(|| {
        let cinvidx = CachedInvertedIndex::new(invidx);
        for position in cinvidx.positions(0).unwrap() {
            black_box(position);
        }
    });
}

#[bench]
fn invidx_decode0_cache_warm(b: &mut Bencher) {
    let (_, invidx, _c) = invidx_setup("word.zigv", "LexIDStream", "LexIDIndex");
    let cinvidx = CachedInvertedIndex::new(invidx);
    for position in cinvidx.positions(0).unwrap() {
        black_box(position);
    }

    b.iter(|| {
        for position in cinvidx.positions(0).unwrap() {
            black_box(position);
        }
    });
}
