use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng, seq::SliceRandom};
use etemenanki::Datastore;
use libcl_rs::Corpus;

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn setup_rand(len: usize, max: usize) -> Vec<usize> {
    let rng = StdRng::seed_from_u64(42);
    let dist = Uniform::new(0, max);
    Vec::from_iter(dist.sample_iter(rng).take(len))
}

fn setup_windows(len: usize, wmin: usize, wmax: usize) -> Vec<(usize, usize)> {
    let mut rng = StdRng::seed_from_u64(42);
    let dist = Uniform::new(wmin, wmax);
    let mut windows: Vec<(usize, usize)> = Vec::new();

    let mut last_end = 0;
    
    loop {
        let start = last_end;
        let end = start + dist.sample(&mut rng);
        if end < len {
            windows.push((start, end));
            last_end = end;
        } else {
            windows.push((start, len));
            break;
        }
    }

    windows.shuffle(&mut rng);

    windows
}

fn open_ziggurat() -> Datastore<'static> {
    // open ziggurat datastore
    Datastore::open("../etemenanki/testdata/simpledickens").unwrap()
}

fn open_cwb() -> Corpus {
    // open CWB corpus
    Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus")
}

//
// Benchmarks
//

// Sequential Layer Decode

fn z_seq_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for s in words {
            black_box(s);
        }
    })
}

fn c_seq_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    b.iter(|| {
        for cpos in 0..words.max_cpos().unwrap() {
            black_box(words.cpos2str(cpos).unwrap());
        }
    })
}

// Random Layer Decode

fn z_rnd_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let positions = setup_rand(words.len(), words.len());

    b.iter(|| {
        for cpos in positions.iter() {
            black_box(words.get(*cpos).unwrap());
        }
    })
}

fn c_rnd_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let positions = setup_rand(words.max_cpos().unwrap() as usize, words.max_cpos().unwrap() as usize);

    b.iter(|| {
        for cpos in positions.iter() {
            black_box(words.cpos2str(*cpos as i32).unwrap());
        }
    })
}

// Windowed Sequential Layer Decode

fn z_window_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let windows = setup_windows(words.len(), 20, 50);


    b.iter(|| {
        for (start, end) in windows.iter() {
            for s in words.get_range(*start, *end).unwrap() {
                black_box(s);
            }
        }
    })
}

fn c_window_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let windows = setup_windows(words.max_cpos().unwrap() as usize, 20, 50);

    b.iter(|| {
        for (start, end) in windows.iter() {
            for cpos in *start..*end {
                black_box(words.cpos2str(cpos as i32).unwrap());
            }
        }
    })
}

//
// Criterion Main
//

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison tests");
    group.sample_size(10);
    group.measurement_time(Duration::new(60, 0));
    // group.measurement_time(Duration::new(600, 0));


    // Sequential Layer Decode
    group.bench_function("ziggurat sequential layer decode", z_seq_decode);
    group.bench_function("libcl sequential layer decode", c_seq_decode);

    // Random Layer Decode
    group.bench_function("ziggurat random layer decode", z_rnd_decode);
    group.bench_function("libcl random layer decode", c_rnd_decode);

    // Windowed Sequential Layer Decode
    group.bench_function("ziggurat windowed sequential layer decode", z_window_decode);
    group.bench_function("libcl windowed sequential layer decode", c_window_decode);
}
