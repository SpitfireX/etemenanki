use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use rand::{Rng, distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng};
use etemenanki::Datastore;

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn setup_rand(len: usize, max: usize) -> Vec<usize> {
    let rng = rng();
    let dist = Uniform::new(0, max);
    Vec::from_iter(dist.sample_iter(rng).take(len))
}

fn setup_windows(len: usize, max: usize, wmin: usize, wmax: usize) -> Vec<(usize, usize)> {
    let mut rng = rng();
    let dist = Uniform::new(0, max);
    let wdist = Uniform::new(wmin, wmax);
    let mut windows: Vec<(usize, usize)> = Vec::new();
    
    for _ in 0..len {
        let start = dist.sample(&mut rng);
        let end = start + wdist.sample(&mut rng);
        if end < max {
            windows.push((start, end));
        } else {
            windows.push((start, max));
        }
    }

    windows
}

fn setup_jumps(len: usize, maxjumps: usize, jumplen: isize) -> Vec<usize> {
    let max = len as isize - 1;
    let mut rng = rng();
    let ndist = Uniform::new(0, maxjumps);
    let lendist = Uniform::new(-jumplen, jumplen);
    let mut jumps = Vec::new();

    for cpos in 0..len {
        jumps.push(cpos);
        if rng.gen_bool(0.5) {
            for _ in 0..ndist.sample(&mut rng) {
                jumps.push((cpos as isize + lendist.sample(&mut rng)).clamp(0, max) as usize);
            }
        }
    }

    jumps
}

fn open_large() -> Datastore<'static> {
    // open large ziggurat datastore
    Datastore::open("ziggurat_large").unwrap()
}

//
// Benchmarks
//

// Raw Access
//

// Sequential Layer Decode

fn l_seq_decode(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for s in words {
            black_box(s);
        }
    })
}

// Random Layer Decode

fn l_rnd_decode(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let positions = setup_rand(10_000_000, words.len());

    b.iter(|| {
        for cpos in positions.iter() {
            black_box(words.get(*cpos).unwrap());
        }
    })
}

// Windowed Random Layer Decode

fn l_rnd_window_decode(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let windows = setup_windows(10_000_000, words.len(), 20, 50);


    b.iter(|| {
        for (start, end) in windows.iter() {
            for s in words.get_range(*start, *end).unwrap() {
                black_box(s);
            }
        }
    })
}

// Windowed Sequential Layer Decode

fn l_seq_window_decode(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let mut windows = setup_windows(10_000_000, words.len(), 20, 50);
    windows.sort_by_key(|(s, _)| *s);

    b.iter(|| {
        for (start, end) in windows.iter() {
            for s in words.get_range(*start, *end).unwrap() {
                black_box(s);
            }
        }
    })
}

// Narrowing Alternating Window Decode

fn l_alternating_decode(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let windows = setup_windows(10_000_000, words.len(), 20, 50);


    b.iter(|| {
        for (start, end) in windows.iter() {
            let zigzag = (*start..*end)
                .zip((*start..*end).rev())
                .map(|(v1, v2)| [v1, v2])
                .flatten()
                .take(*end - *start);

            for cpos in zigzag {
                black_box(words.get(cpos).unwrap());
            }
        }
    })
}

// Sequential, Head Locally Random Decode

fn l_headlocal_decode(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let jumps = setup_jumps(words.len(), 5, 10);

    b.iter(|| {
        for cpos in jumps.iter() {
            black_box(words.get(*cpos));
        }
    })
}

// Sequential Segmentation Decode

fn l_seq_seg_decode(b: &mut Bencher) {
    let datastore = open_large();
    let s = datastore["s"]
        .as_segmentation()
        .unwrap();

    b.iter(|| {
        for seg in s.iter() {
            black_box(seg);
        }
    })
}

// Random Segmentation Decode

fn l_rnd_seg_decode(b: &mut Bencher) {
    let datastore = open_large();
    let s = datastore["s"]
        .as_segmentation()
        .unwrap();
    let positions = setup_rand(10_000_000, s.len());

    b.iter(|| {
        for pos in positions.iter() {
            black_box(s.get(*pos).unwrap());
        }
    })
}

// Sequential Segmentation Lookup

fn l_seq_seg_lookup(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let s = datastore["s"]
        .as_segmentation()
        .unwrap();

    b.iter(|| {
        for cpos in 0..words.len() {
            black_box(s.find_containing(cpos));
        }
    })
}

// Random Segmentation Lookup

fn l_rnd_seg_lookup(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let s = datastore["s"]
        .as_segmentation()
        .unwrap();
    let positions = setup_rand(10_000_000, words.len());


    b.iter(|| {
        for cpos in positions.iter() {
            black_box(s.find_containing(*cpos));
        }
    })
}

// Windowed Segmentation Lookup

fn l_window_seg_lookup(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let s = datastore["s"]
        .as_segmentation()
        .unwrap();
    let windows = setup_windows(10_000_000, words.len(), 20, 50);

    b.iter(|| {
        for (start, end) in windows.iter() {
            for cpos in *start..*end {
                black_box(s.find_containing(cpos));
            }
        }
    })
}

// Segmentation Start

fn l_seg_start(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let s = datastore["s"]
        .as_segmentation()
        .unwrap();

    b.iter(|| {
        for cpos in 0..words.len() {
            black_box(s.contains_start(cpos));
        }
    })
}

// Combined Access
//

// Join Performance

fn l_join(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let s = datastore["s"]
        .as_segmentation()
        .unwrap();
    let positions = setup_rand(10_000_000, words.len());

    b.iter(|| {
        for cpos in positions.iter() {
           if let Some(spos) = s.find_containing(*cpos) {
                let (start, end) = s.get_unchecked(spos);
                for s in words.get_range(start, end).unwrap() {
                    black_box(s);
                }
           }
        }
    })
}

// Lexicon Lookup
//

// Lookup Strategy 2:
// Scanning the variable's lexicon for all matching strings and then using the variable's ReverseIndex component for actual result lookup
fn l_lexicon_scan(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        let types: Vec<_> = words.lexicon().all_matching_regex("^be").unwrap().collect();
        let positions = words.inverted_index().get_combined_postings(&types);
        for cpos in positions {
            black_box(words.get(cpos));
        }
    })
}

//
// Criterion Main
//

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("large tests");
    group.sample_size(10);
    group.measurement_time(Duration::new(60, 0));
    // group.measurement_time(Duration::new(600, 0));
    group.sampling_mode(criterion::SamplingMode::Flat);


    // Sequential Layer Decode
    group.bench_function("large ziggurat sequential layer decode", l_seq_decode);

    // Random Layer Decode
    group.bench_function("large ziggurat random layer decode", l_rnd_decode);

    // Windowed Sequential Layer Decode
    group.bench_function("large ziggurat sequential windowed layer decode", l_seq_window_decode);

    // Random Sequential Layer Decode
    group.bench_function("large ziggurat random windowed layer decode", l_rnd_window_decode);

    // Narrowing Alternating Window Decode
    group.bench_function("large ziggurat narrowing alternating window layer decode", l_alternating_decode);

    // Sequential, Head Locally Random Decode
    group.bench_function("large ziggurat head locally random layer decode", l_headlocal_decode);

    // Sequential Segmentation Decode
    group.bench_function("large ziggurat sequential segmentation decode", l_seq_seg_decode);

    // Random Segmentation Decode
    group.bench_function("large ziggurat random segmentation decode", l_rnd_seg_decode);

    // Sequential Segmentation Lookup
    group.bench_function("large ziggurat sequential segmentation lookup", l_seq_seg_lookup);

    // Random Segmentation Lookup
    group.bench_function("large ziggurat random segmentation lookup", l_rnd_seg_lookup);

    // Windowed Segmentation Lookup
    group.bench_function("large ziggurat windowed segmentation lookup", l_window_seg_lookup);

    // Segmentation Start
    group.bench_function("large ziggurat segmentation start check", l_seg_start);

    // Join Performance
    group.bench_function("large ziggurat join performance", l_join);

    // Lexicon Lookup Performance
    group.bench_function("large regex lookup 2", l_lexicon_scan);
}
