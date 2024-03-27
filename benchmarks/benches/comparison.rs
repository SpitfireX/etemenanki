use std::{ffi::CString, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use rand::{Rng, distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng};
use etemenanki::{components::FnvHash, Datastore};
use libcl_rs::{ClRegex, Corpus};
use regex::Regex;

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


// RegEx Search
//

// Lexicon Lookup Baseline:
// Find a type id in the lexicon

fn z_baseline_lexicon_lookup(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        black_box(words.lexicon().find_match("ziggurat"));
    })
}

fn z_baseline_lexicon_index_lookup(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        let hash = "ziggurat".fnv_hash();
        let tid = words.index().get_first(hash).unwrap() as usize;
        black_box(tid);
    })
}

fn c_baseline_lexicon_lookup(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let cstr = CString::new("ziggurat").unwrap();

    b.iter(|| {
        let tid = words.str2id(&cstr).unwrap();
        black_box(tid);
    })
}

// RegEx Lexicon Lookup:
// Generate a type id list from scanning the lexicon

fn z_regex_lexicon_lookup(b: &mut Bencher, regex: &str) {
    let datastore = open_ziggurat();
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

fn c_regex_lexicon_lookup(b: &mut Bencher, regex: &str) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let cstr = &CString::new(regex).unwrap();

    b.iter(|| {
        black_box(words.regex2id(&cstr, 0).unwrap());
    });
}

// RegEx Layer Scan:
// Sequentially scanning the whole variable while regex-matching each token 
fn z_regex_layer_scan(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        let regex = Regex::new("^be").unwrap();
        for s in words {
            if regex.is_match(s) {
                black_box(s);
            }
        }
    })
}

fn c_regex_layer_scan_rust_regex(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    b.iter(|| {
        let regex = Regex::new("^be").unwrap();
        for cpos in 0..words.max_cpos().unwrap() {
            let s = words.cpos2str(cpos).unwrap();
            if regex.is_match(s.to_str().unwrap()) {
                black_box(s);
            }
        }
    })
}

fn c_regex_layer_scan_libcl_regex(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let cstr = CString::new("be.+").unwrap();

    b.iter(|| {
        Regex::new("^be").unwrap();
        let regex = ClRegex::new(&cstr, 0, corpus.charset()).unwrap();
        for cpos in 0..words.max_cpos().unwrap() {
            let s = words.cpos2str(cpos).unwrap();
            if regex.is_match(s) {
                black_box(s);
            }
        }
    })
}

// RegEx Lexicon Scan:
// Scanning the variable's lexicon for all matching strings as lexicon IDs and then collecting a position list

fn z_regex_lexicon_scan(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        let types: Vec<_> = words.lexicon().get_all_matching_regex("^be");
        let positions = words.inverted_index().get_combined_postings(&types);
        for cpos in positions {
            black_box(words.get(cpos));
        }
    })
}

fn c_regex_lexicon_scan(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let cstr = CString::new("be.+").unwrap();

    b.iter(|| {
        let ids = words.regex2id(&cstr, 0).unwrap();
        let positions = words.idlist2cpos(&ids, true).unwrap();
        for cpos in positions.iter() {
            let _ = black_box(words.cpos2str(*cpos));
        }
    })
}

// Postings Lookup:
// Identify a list of types and decode their whole postings lists individually

const TYPES: [&'static str; 11] = ["the", "end", "is", "near", "Cthulhu", "will", "rise", "and", "destroy", "every", "ziggurat"];

fn z_postings_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_postings_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let tids: Result<Vec<i32>, _> = TYPES.iter()
        .map(|s| words.str2id(&CString::new(*s).unwrap()))
        .collect();
    let tids = tids.unwrap();

    b.iter(|| {
        for tid in tids.iter() {
            // decodes the specified postings list
            black_box(words.id2cpos(*tid).unwrap());
        }
    })
}

// Combined Postings:
// Get a combined, sorted postings list for a set of types

fn z_postings_combined(b: &mut Bencher) {
    let datastore = open_ziggurat();
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

fn c_postings_combined(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let tids: Result<Vec<i32>, _> = TYPES.iter()
        .map(|s| words.str2id(&CString::new(*s).unwrap()))
        .collect();
    let tids = tids.unwrap();

    b.iter(|| {
        // gets the combined postings list for all types
        black_box(words.idlist2cpos(&tids, true).unwrap());
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
    let mut group = c.benchmark_group("comparison tests");
    group.sample_size(50);
    group.measurement_time(Duration::new(60, 0));
    // group.measurement_time(Duration::new(600, 0));
    group.sampling_mode(criterion::SamplingMode::Flat);


    // Sequential Layer Decode (raw token stream decoding)
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

    // Lexicon Lookup Baseline
    group.bench_function("ziggurat baseline lexicon lookup", z_baseline_lexicon_lookup);
    group.bench_function("ziggurat baseline lexicon lookup index", z_baseline_lexicon_index_lookup);
    group.bench_function("libcl baseline lexicon lookup", c_baseline_lexicon_lookup);

    // RegEx Lexicon Lookup
    for regex in REGEX_TESTS {
        group.bench_function(format!("ziggurat regex lexicon lookup \"{}\"", regex), |b| z_regex_lexicon_lookup(b, regex));
        group.bench_function(format!("libcl regex lexicon lookup \"{}\"", regex), |b| c_regex_lexicon_lookup(b, regex));
    }

    // RegEx Layer Scan
    group.bench_function("ziggurat regex layer scan", z_regex_layer_scan);
    group.bench_function("libcl regex layer scan (rust regex)", c_regex_layer_scan_rust_regex);
    group.bench_function("libcl regex layer scan (libcl regex)", c_regex_layer_scan_libcl_regex);

    // RegEx Lexicon Scan
    group.bench_function("ziggurat regex lexicon scan", z_regex_lexicon_scan);
    group.bench_function("libcl regex lexicon scan", c_regex_lexicon_scan);

    // Postings Lookup (raw concordance decoding)
    group.bench_function("ziggurat postings list decode", z_postings_decode);
    group.bench_function("libcl postings list decode", c_postings_decode);

    // Combined Postings (combined sorted concordance list creation)
    group.bench_function("ziggurat combined postings list", z_postings_combined);
    group.bench_function("libcl combined postings list", c_postings_combined);
}
