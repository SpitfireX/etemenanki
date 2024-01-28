use std::fs::File;

use memmap2::Mmap;
use streaming_iterator::StreamingIterator;
use test::{Bencher, black_box};
use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, Rng, SeedableRng};

use crate::{components::{CachedVector, Vector, VectorReader}, container::Container};

#[test]
fn hello() {
    println!("Hello!");
}

fn vec_setup() -> (Vector<'static>, Container<'static>) {
    let filename = "../scripts/recipes4000/token.zigv";
    let file = File::open(filename).unwrap();
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
    println!("{}", vec.len());
    assert!(vec.len() == 273);
    println!("{:?}", vec.get_row(10));
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
