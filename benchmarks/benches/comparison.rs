use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use rand::{Rng, distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng};
use etemenanki::Datastore;
use libcl_rs::Corpus;

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
                jumps.push((cpos as isize + lendist.sample(&mut rng)).min(max) as usize);
            }
        }
    }

    jumps
}

fn open_ziggurat() -> Datastore<'static> {
    // open ziggurat datastore
    Datastore::open("ziggurat").unwrap()
}

fn open_cwb() -> Corpus {
    // open CWB corpus
    Corpus::new("cwb/registry", "encow_cwb").expect("Could not open corpus")
}

//
// Benchmarks
//

// Raw Access
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
    let positions = setup_rand(10_000_000, words.len());

    b.iter(|| {
        for cpos in positions.iter() {
            black_box(words.get(*cpos).unwrap());
        }
    })
}

fn c_rnd_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let positions = setup_rand(10_000_000, words.max_cpos().unwrap() as usize);

    b.iter(|| {
        for cpos in positions.iter() {
            black_box(words.cpos2str(*cpos as i32).unwrap());
        }
    })
}

// Windowed Random Layer Decode

fn z_rnd_window_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_rnd_window_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let windows = setup_windows(10_000_000, words.max_cpos().unwrap() as usize, 20, 50);

    b.iter(|| {
        for (start, end) in windows.iter() {
            for cpos in *start..*end {
                black_box(words.cpos2str(cpos as i32).unwrap());
            }
        }
    })
}

// Windowed Sequential Layer Decode

fn z_seq_window_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_seq_window_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let mut windows = setup_windows(10_000_000, words.max_cpos().unwrap() as usize, 20, 50);
    windows.sort_by_key(|(s, _)| *s);

    b.iter(|| {
        for (start, end) in windows.iter() {
            for cpos in *start..*end {
                black_box(words.cpos2str(cpos as i32).unwrap());
            }
        }
    })
}

// Narrowing Alternating Window Decode

fn z_alternating_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_alternating_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let windows = setup_windows(10_000_000, words.max_cpos().unwrap() as usize, 20, 50);

    b.iter(|| {
        for (start, end) in windows.iter() {
            let zigzag = (*start..*end)
                .zip((*start..*end).rev())
                .map(|(v1, v2)| [v1, v2])
                .flatten()
                .take(*end - *start);

            for cpos in zigzag {
                black_box(words.cpos2str(cpos as i32).unwrap());
            }
        }
    })
}

// Sequential, Head Locally Random Decode

fn z_headlocal_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_headlocal_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let jumps = setup_jumps(words.max_cpos().unwrap() as usize, 5, 10);

    b.iter(|| {
        for cpos in jumps.iter() {
            black_box(words.cpos2str(*cpos as i32).unwrap());
        }
    })
}

// Sequential Segmentation Decode

fn z_seq_seg_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let s = datastore["s"]
        .as_segmentation()
        .unwrap();

    b.iter(|| {
        for seg in s.iter() {
            black_box(seg);
        }
    })
}

fn c_seq_seg_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let s = corpus.get_s_attribute("s").unwrap();

    b.iter(|| {
        for struc in 0..s.max_struc().unwrap() {
            black_box(s.struc2cpos(struc).unwrap());
        }
    })
}

// Random Segmentation Decode

fn z_rnd_seg_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_rnd_seg_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let s = corpus.get_s_attribute("s").unwrap();
    let positions = setup_rand(10_000_000, s.max_struc().unwrap() as usize);

    b.iter(|| {
        for pos in positions.iter() {
            black_box(s.struc2cpos(*pos as i32).unwrap());
        }
    })
}

// Sequential Segmentation Lookup

fn z_seq_seg_lookup(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_seq_seg_lookup(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let s = corpus.get_s_attribute("s").unwrap();

    b.iter(|| {
        for cpos in 0..words.max_cpos().unwrap() {
            let _ = black_box(s.cpos2struc(cpos));
        }
    })
}


// Random Segmentation Lookup

fn z_rnd_seg_lookup(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_rnd_seg_lookup(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let s = corpus.get_s_attribute("s").unwrap();
    let positions = setup_rand(10_000_000, words.max_cpos().unwrap() as usize);

    b.iter(|| {
        for cpos in positions.iter() {
            let _ = black_box(s.cpos2struc(*cpos as i32));
        }
    })
}

// Windowed Segmentation Lookup

fn z_window_seg_lookup(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_window_seg_lookup(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let s = corpus.get_s_attribute("s").unwrap();
    let windows = setup_windows(10_000_000, words.max_cpos().unwrap() as usize, 20, 50);

    b.iter(|| {
        for (start, end) in windows.iter() {
            for cpos in *start..*end {
                let _ = black_box(s.cpos2struc(cpos as i32));
            }
        }
    })
}

// Segmentation Start

fn z_seg_start(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_seg_start(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let s = corpus.get_s_attribute("s").unwrap();

    b.iter(|| {
        for cpos in 0..words.max_cpos().unwrap() {
            black_box(s.cpos2boundary(cpos).unwrap() == 2);
        }
    })
}

// Combined Access
//

// Join Performance

fn z_join(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_join(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let s = corpus.get_s_attribute("s").unwrap();
    let positions = setup_rand(10_000_000, words.max_cpos().unwrap() as usize);

    b.iter(|| {
        for cpos in positions.iter() {
            if let Ok((start, end)) = s.cpos2struc2cpos(*cpos as i32) {
                for i in start..end {
                    black_box(words.cpos2str(i).unwrap());
                }
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
    group.sampling_mode(criterion::SamplingMode::Flat);


    // Sequential Layer Decode
    group.bench_function("ziggurat sequential layer decode", z_seq_decode);
    group.bench_function("libcl sequential layer decode", c_seq_decode);

    // Random Layer Decode
    group.bench_function("ziggurat random layer decode", z_rnd_decode);
    group.bench_function("libcl random layer decode", c_rnd_decode);

    // Windowed Sequential Layer Decode
    group.bench_function("ziggurat sequential windowed layer decode", z_seq_window_decode);
    group.bench_function("libcl sequential windowed layer decode", c_seq_window_decode);

    // Random Sequential Layer Decode
    group.bench_function("ziggurat random windowed layer decode", z_rnd_window_decode);
    group.bench_function("libcl random windowed layer decode", c_rnd_window_decode);

    // Narrowing Alternating Window Decode
    group.bench_function("ziggurat narrowing alternating window layer decode", z_alternating_decode);
    group.bench_function("libcl narrowing alternating window layer decode", c_alternating_decode);

    // Sequential, Head Locally Random Decode
    group.bench_function("ziggurat head locally random layer decode", z_headlocal_decode);
    group.bench_function("libcl head locally random layer decode", c_headlocal_decode);

    // Sequential Segmentation Decode
    group.bench_function("ziggurat sequential segmentation decode", z_seq_seg_decode);
    group.bench_function("libcl sequential segmentation decode", c_seq_seg_decode);

    // Random Segmentation Decode
    group.bench_function("ziggurat random segmentation decode", z_rnd_seg_decode);
    group.bench_function("libcl random segmentation decode", c_rnd_seg_decode);

    // Sequential Segmentation Lookup
    group.bench_function("ziggurat sequential segmentation lookup", z_seq_seg_lookup);
    group.bench_function("libcl sequential segmentation lookup", c_seq_seg_lookup);

    // Random Segmentation Lookup
    group.bench_function("ziggurat random segmentation lookup", z_rnd_seg_lookup);
    group.bench_function("libcl random segmentation lookup", c_rnd_seg_lookup);

    // Windowed Segmentation Lookup
    group.bench_function("ziggurat windowed segmentation lookup", z_window_seg_lookup);
    group.bench_function("libcl windowed segmentation lookup", c_window_seg_lookup);

    // Segmentation Start
    group.bench_function("ziggurat segmentation start check", z_seg_start);
    group.bench_function("libcl segmentation start check", c_seg_start);

    // Join Performance
    group.bench_function("ziggurat join performance", z_join);
    group.bench_function("libcl join performance", c_join);
}
