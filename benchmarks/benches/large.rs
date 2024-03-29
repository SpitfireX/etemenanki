use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng};
use etemenanki::{components::FnvHash, Datastore};

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

fn jumps(len: usize, maxjumps: usize, jumplen: isize) -> impl Iterator<Item = usize> {
    let max = len as isize - 1;
    let mut rng = rng();
    let ndist = Uniform::new(0, maxjumps);
    let lendist = Uniform::new(-jumplen, jumplen);

    (0..len)
        .map(move |cpos| {
            let mut jumps = vec![cpos];
            for _ in 0..ndist.sample(&mut rng) {
                jumps.push((cpos as isize + lendist.sample(&mut rng)).clamp(0, max) as usize);
            }
            jumps
        })
        .flatten()
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

    b.iter(|| {
        let jumps = jumps(words.len(), 5, 10);
        for cpos in jumps {
            black_box(words.get(cpos));
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

// RegEx Search
//

// Lexicon Lookup Baseline:
// Find a type id in the lexicon

fn l_baseline_lexicon_lookup(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        black_box(words.lexicon().find_match("ziggurat"));
    })
}

fn l_baseline_lexicon_index_lookup(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        let hash = "ziggurat".fnv_hash();
        let tid = words.index().get_first(hash).unwrap() as usize;
        black_box(tid);
    })
}

// RegEx Lexicon Lookup:
// Generate a type id list from scanning the lexicon

fn l_regex_lexicon_lookup(b: &mut Bencher, regex: &str) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let mut r = "^".to_string();
    r.push_str(regex);
    r.push('$');

    b.iter(|| {
        black_box(words.lexicon().get_all_matching_regex(&r));
    })
}

// RegEx Lexicon Scan:
// Scanning the variable's lexicon for all matching strings as lexicon IDs and then collecting a position list

fn l_regex_lexicon_scan(b: &mut Bencher) {
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

// Postings Lookup:
// Identify a list of types and decode their whole postings lists individually

const TYPES: [&'static str; 11] = ["the", "end", "is", "near", "Cthulhu", "will", "rise", "and", "destroy", "every", "ziggurat"];

fn l_postings_decode(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let tids: Option<Vec<usize>> = TYPES.iter()
        .map(|s| words.lexicon().iter().position(|t| t == *s))
        .collect();
    let tids = tids.unwrap();

    b.iter(|| {
        for tid in tids.iter() {
            // get the decoded postings list from the cache
            // always a cache miss, this will implicitly decode the whole postings lists
            black_box(words.inverted_index().get_postings(*tid).unwrap());
        }
    })
}

// Combined Postings:
// Get a combined, sorted postings list for a set of types

fn l_postings_combined(b: &mut Bencher) {
    let datastore = open_large();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let tids: Option<Vec<usize>> = TYPES.iter()
        .map(|s| words.lexicon().iter().position(|t| t == *s))
        .collect();
    let tids = tids.unwrap();

    b.iter(|| {
        // get a combined and sorted postings list
        // this gets the the decoded postings list for each type id from the cache
        // and copies them into a new vec. this will double allocate and always copy
        // always a cache miss, this will implicitly decode the whole postings lists
        black_box(words.inverted_index().get_combined_postings(&tids));
    })
}


//
// Criterion Main
//

static REGEX_TESTS: [&'static str; 20] = [
    r"ziggurat",
    r"be.+",
    r"imp.ss.ble",
    r"colou?r",
    r"...+able",
    r"(work|works|worked|working)",
    r"super.+listic.+ous",
    r"show(s|ed|n|ing)?",
    r"(.*lier|.*liest)",
    r".*(lier|liest)",
    r".*li(er|est)",
    r".*lie(r|st)",
    r".*(rr.+rr|ss.+ss|tt.+tt).*y",
    r"(un)?easy",
    r".{3,}(ness(es)?|it(y|ies)|(tion|ment)s?)",
    r".+tio.+",
    r".*a{3,}.*",
    r"[^!-~]+",
    r"[aeiou][bcdfghjklmnpqrstvwxyz]{2}){4,}",
    r"[aeiou][b-df-hj-np-tv-z]{2}){4,}",
];

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("large tests");
    group.sample_size(25);
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

    // Lexicon Lookup Baseline
    group.bench_function("large baseline lexicon lookup", l_baseline_lexicon_lookup);
    group.bench_function("large baseline lexicon lookup index", l_baseline_lexicon_index_lookup);

    // RegEx Lexicon Lookup
    for regex in REGEX_TESTS {
        group.bench_function(format!("large regex lexicon lookup \"{}\"", regex), |b| l_regex_lexicon_lookup(b, regex));
    }

    // Lexicon Lookup Performance
    group.bench_function("large regex lexicon scan", l_regex_lexicon_scan);

    // Postings Lookup (raw concordance decoding)
    group.bench_function("large postings list decode", l_postings_decode);

    // Combined Postings (combined sorted concordance list creation)
    group.bench_function("large combined postings list", l_postings_combined);
}
