use std::fs::File;

use memmap2::Mmap;
use streaming_iterator::StreamingIterator;
use test::{Bencher, black_box};
use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng};

use crate::{components::{CachedIndex, CachedVector, Index, IndexBlock, Vector, VectorReader}, container::Container, layers::SegmentationLayer};

const DATASTORE_PATH: &'static str = "../scripts/recipes4000/";

fn vec_setup() -> (Vector<'static>, Container<'static>) {
    let file = File::open(DATASTORE_PATH.to_owned() + "token.zigv").unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let container = Container::from_mmap(mmap, "word".to_owned()).unwrap();

    let vec = *container
        .components
        .get("LexIDStream")
        .unwrap()
        .as_vector()
        .unwrap();

    (vec, container)
}

#[test]
fn vec() {
    let (vec, _c) = vec_setup();
    assert!(vec.len() == 3508);
    assert!(vec.get_row(10).unwrap()[0] == 31);
}

#[bench]
fn vec_seq_no(b: &mut Bencher) {
    let (vec, _c) = vec_setup();
    b.iter(|| {
        for i in 0..vec.len() {
            black_box(vec.get_row(i));
        }
    })
}

#[bench]
fn vec_seq_reader(b: &mut Bencher) {
    let (vec, _c) = vec_setup();
    b.iter(|| {
        let mut reader = VectorReader::from_vector(vec);
        for i in 0..vec.len() {
            black_box(reader.get_row(i));
        }
    })
}

#[bench]
fn vec_seq_reader_iter(b: &mut Bencher) {
    let (vec, _c) = vec_setup();
    b.iter(|| {
        for row in vec {
            black_box(row);
        }
    })
}

#[bench]
fn vec_seq_cached(b: &mut Bencher) {
    let (vec, _c) = vec_setup();
    b.iter(|| {
        let mut cvec = CachedVector::new(vec);
        for i in 0..vec.len() {
            black_box(cvec.get_row(i));
        }
    })
}

#[bench]
fn vec_seq_cached_iter(b: &mut Bencher) {
    let (vec, _c) = vec_setup();
    b.iter(|| {
        let mut cvec = CachedVector::new(vec);
        let mut iter = cvec.iter();
        while let Some(row) = iter.next() {
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
    let (vec, _c) = vec_setup();
    let ids = setup_rand(NACCESS, vec.len());
    b.iter(|| {
        for i in &ids {
            black_box(vec.get_row(*i));
        }
    })
}

#[bench]
fn vec_rand_reader(b: &mut Bencher) {
    let (vec, _c) = vec_setup();
    let ids = setup_rand(NACCESS, vec.len());
    b.iter(|| {
        let mut reader = VectorReader::from_vector(vec);
        for i in &ids {
            black_box(reader.get_row(*i));
        }
    })
}

#[bench]
fn vec_rand_cached(b: &mut Bencher) {
    let (vec, _c) = vec_setup();
    let ids = setup_rand(NACCESS, vec.len());
    b.iter(|| {
        let mut cached = CachedVector::new(vec);
        for i in &ids {
            black_box(cached.get_row(*i));
        }
    })
}

fn idxcmp_setup(filename: &'static str, component_name: &'static str) -> (Index<'static>, Container<'static>) {
    let file = File::open(DATASTORE_PATH.to_owned() + filename).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let container = Container::from_mmap(mmap, "word".to_owned()).unwrap();

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
    let (index, _container) = idxcmp_setup("text/year.zigv", "IntSort");
    if let Index::Compressed { length, r, sync, data } = index {
        println!("\n index len {} with r {}", length, r);
        for (i, (_, o)) in sync.iter().enumerate(){
            println!();

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
    let (index, _container) = idxcmp_setup("text/year.zigv", "IntSort");
    println!();
    println!("{:?}", index.get_all(0).collect::<Vec<_>>());
    println!("{:?}", index.get_all(3).collect::<Vec<_>>());
    println!("{:?}", index.get_all(2002).collect::<Vec<_>>());
    println!("{:?}", index.get_all(2003).collect::<Vec<_>>());
    println!("{:?}", index.get_all(9001).collect::<Vec<_>>());
}

#[test]
fn cachedidx_iter() {
    let (index, _container) = idxcmp_setup("text/year.zigv", "IntSort");
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
    assert!(seg.find_containing(10) == Some(1));
    assert!(seg.find_containing(1337) == Some(98));
    assert!(seg.find_containing(9001) == None);
}
