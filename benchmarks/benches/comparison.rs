#![feature(c_str_literals)]

use std::{ffi::CString, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use etemenanki::{components::FnvHash, variables::IndexedStringVariable};
use libcl_rs::{ClRegex, PositionalAttribute};
use regex::Regex;

include!("common.rs");
use common::*;

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

const RANDOM: usize = 30_000_000;

//
// Benchmarks
//

// Instantiation

fn z_instantiation(b: &mut Bencher) {
    b.iter(|| {
        black_box(open_ziggurat());
    })
}

fn c_instantiation(b: &mut Bencher) {
    b.iter(|| {
        black_box(open_cwb());
    })
}

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
    let positions = setup_rand(RANDOM, words.len());

    b.iter(|| {
        for cpos in positions.iter() {
            black_box(words.get(*cpos).unwrap());
        }
    })
}

fn c_rnd_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let positions = setup_rand(RANDOM, words.max_cpos().unwrap() as usize);

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
    let windows = setup_windows(RANDOM, words.len(), 20, 50);


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
    let windows = setup_windows(RANDOM, words.max_cpos().unwrap() as usize, 20, 50);

    b.iter(|| {
        for (start, end) in windows.iter() {
            for cpos in *start..*end {
                black_box(words.cpos2str(cpos as i32).unwrap());
            }
        }
    })
}

// Windowed Sequential Layer Decode
// this should have the same performance sequential layer decode

fn z_seq_window_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let mut windows = setup_windows(RANDOM, words.len(), 20, 50);
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
    let mut windows = setup_windows(RANDOM, words.max_cpos().unwrap() as usize, 20, 50);
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
    let windows = setup_windows(RANDOM, words.len(), 20, 50);


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
    let windows = setup_windows(RANDOM, words.max_cpos().unwrap() as usize, 20, 50);

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

fn z_headlocal_decode(b: &mut Bencher, jumplen: isize) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();
    let jumps = setup_jumps(RANDOM, words.len(), 10, jumplen);

    b.iter(|| {
        for cpos in jumps.iter() {
            black_box(words.get(*cpos));
        }
    })
}

fn c_headlocal_decode(b: &mut Bencher, jumplen: isize) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();
    let jumps = setup_jumps(RANDOM, words.max_cpos().unwrap() as usize, 10, jumplen);

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
    let positions = setup_rand(RANDOM, s.len());

    b.iter(|| {
        for pos in positions.iter() {
            black_box(s.get(*pos).unwrap());
        }
    })
}

fn c_rnd_seg_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let s = corpus.get_s_attribute("s").unwrap();
    let positions = setup_rand(RANDOM, s.max_struc().unwrap() as usize);

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
    let positions = setup_rand(RANDOM, words.len());


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
    let positions = setup_rand(RANDOM, words.max_cpos().unwrap() as usize);

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
    let windows = setup_windows(RANDOM, words.len(), 20, 50);

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
    let windows = setup_windows(RANDOM, words.max_cpos().unwrap() as usize, 20, 50);

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
    let positions = setup_rand(RANDOM, words.len());

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
    let positions = setup_rand(RANDOM, words.max_cpos().unwrap() as usize);

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

// Layer Scan Baseline:
// Find matching positions in the value stream

fn z_baseline_layer_scan(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for s in words {
            black_box(s == "ziggurat");
        }
    })
}

fn c_baseline_layer_scan(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    b.iter(|| {
        for cpos in 0..words.max_cpos().unwrap() {
            let s = words.cpos2str(cpos).unwrap();
            black_box(s == c"ziggurat");
        }
    })
}


// RegEx Lexicon Lookup:
// Generate a type id list from scanning the lexicon

fn z_regex_lexicon_lookup(b: &mut Bencher, regex: &str) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let r = "^".to_string() + regex + "$";

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

fn z_regex_layer_scan(b: &mut Bencher, regex: &str) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let r = "^".to_string() + regex + "$";

    b.iter(|| {
        let regex = Regex::new(&r).unwrap();
        for s in words {
            black_box(regex.is_match(s));
        }
    })
}

fn c_regex_layer_scan(b: &mut Bencher, regex: &str) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let cstr = &CString::new(regex).unwrap();

    b.iter(|| {
        let regex = ClRegex::new(&cstr, 0, corpus.charset()).unwrap();
        for cpos in 0..words.max_cpos().unwrap() {
            let s = words.cpos2str(cpos).unwrap();
            black_box(regex.is_match(s));
        }
    })
}


// Postings Decode:
// Identify a list of types and decode their whole postings lists individually

#[inline(always)]
fn z_postings_decode(words: &IndexedStringVariable, tids: &[usize]) {
    for tid in tids {
        // explicitly decode the postings list, instead of getting it from the cache
        black_box(words.inverted_index().decode_postings(*tid).unwrap());
    }
}

#[inline(always)]
fn c_postings_decode(words: &PositionalAttribute, tids: &[i32]) {
    for tid in tids {
        // decodes the specified postings list
        black_box(words.id2cpos(*tid).unwrap());
    }
}

fn z_typelist_postings_decode(b: &mut Bencher, types: &[&str]) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let tids: Option<Vec<usize>> = types.iter()
        .map(|s| words.lexicon().iter().position(|t| t == *s))
        .collect();
    let tids = tids.unwrap();

    b.iter(|| {
        z_postings_decode(words, &tids);
    })
}

fn c_typelist_postings_decode(b: &mut Bencher, types: &[&str]) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let tids: Result<Vec<i32>, _> = types.iter()
        .map(|s| words.str2id(&CString::new(*s).unwrap()))
        .collect();
    let tids = tids.unwrap();

    b.iter(|| {
        c_postings_decode(&words, &tids);
    })
}

// RegEx Postings Decode

fn z_regex_postings_decode(b: &mut Bencher, regex: &str) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let r = "^".to_string() + regex + "$";
    let tids = words.lexicon().get_all_matching_regex(&r);
    assert!(tids.len() > 0);

    b.iter(|| {
        z_postings_decode(words, &tids);
    })
}

fn c_regex_postings_decode(b: &mut Bencher, regex: &str) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let cstr = &CString::new(regex).unwrap();
    let tids = words.regex2id(&cstr, 0).unwrap();

    b.iter(|| {
        c_postings_decode(&words, &tids);
    })
}

// All Postings Decode

fn z_all_postings_decode(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let tids: Vec<usize> = (0..words.lexicon().len()).collect();

    b.iter(|| {
        z_postings_decode(words, &tids);
    })
}

fn c_all_postings_decode(b: &mut Bencher) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let tids: Vec<i32> = (0..words.max_id().unwrap()).collect();

    b.iter(|| {
        c_postings_decode(&words, &tids);
    })
}

// Gather Postings:
// Get a combined, sorted postings list for a set of types

#[inline(always)]
fn z_postings_gather(words: &IndexedStringVariable, tids: &[usize]) {
    // explicitly decode the postings list, instead of getting it from the cache
    black_box(words.inverted_index().decode_combined_postings(tids));
}

#[inline(always)]
fn c_postings_gather(words: &PositionalAttribute, tids: &[i32]) {
    // decodes the specified postings list
    black_box(words.idlist2cpos(tids, true).unwrap());
}

fn z_typelist_postings_gather(b: &mut Bencher, types: &[&str]) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let tids: Option<Vec<usize>> = types.iter()
        .map(|s| words.lexicon().iter().position(|t| t == *s))
        .collect();
    let tids = tids.unwrap();

    b.iter(|| {
        z_postings_gather(words, &tids);
    })
}

fn c_typelist_postings_gather(b: &mut Bencher, types: &[&str]) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let tids: Result<Vec<i32>, _> = types.iter()
        .map(|s| words.str2id(&CString::new(*s).unwrap()))
        .collect();
    let tids = tids.unwrap();

    b.iter(|| {
        c_postings_gather(&words, &tids);
    })
}

// RegEx Postings Gather

fn z_regex_postings_gather(b: &mut Bencher, regex: &str) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    let r = "^".to_string() + regex + "$";
    let tids = words.lexicon().get_all_matching_regex(&r);
    assert!(tids.len() > 0);

    b.iter(|| {
        z_postings_gather(words, &tids);
    })
}

fn c_regex_postings_gather(b: &mut Bencher, regex: &str) {
    let corpus = open_cwb();
    let words = corpus.get_p_attribute("word").unwrap();

    let cstr = &CString::new(regex).unwrap();
    let tids = words.regex2id(&cstr, 0).unwrap();

    b.iter(|| {
        c_postings_gather(&words, &tids);
    })
}

//
// Criterion Main
//

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison tests");
    group.sample_size(100);
    group.measurement_time(Duration::new(600, 0));
    // group.measurement_time(Duration::new(600, 0));
    group.sampling_mode(criterion::SamplingMode::Flat);

    // Datastore/Corpus instantiation
    group.bench_function("ziggurat instantiation", z_instantiation);
    group.bench_function("libcl instantiation", c_instantiation);


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
    group.bench_function("ziggurat head locally random layer decode 10", |b| z_headlocal_decode(b, 10));
    group.bench_function("ziggurat head locally random layer decode 50", |b| z_headlocal_decode(b, 50));
    group.bench_function("ziggurat head locally random layer decode 100", |b| z_headlocal_decode(b, 100));
    group.bench_function("libcl head locally random layer decode 10", |b| c_headlocal_decode(b, 10));
    group.bench_function("libcl head locally random layer decode 50", |b| c_headlocal_decode(b, 50));
    group.bench_function("libcl head locally random layer decode 100", |b| c_headlocal_decode(b, 100));


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


    //
    // Concordance lookup:
    //

    //
    // Step 1
    // gather a list of type IDs

    // Lexicon Lookup Baseline (single type)
    group.bench_function("ziggurat baseline lexicon lookup", z_baseline_lexicon_lookup);
    group.bench_function("ziggurat baseline lexicon lookup index", z_baseline_lexicon_index_lookup); // measures actual lexicon index performance
    group.bench_function("libcl baseline lexicon lookup", c_baseline_lexicon_lookup);

    // Lexicon Lookup using RegEx
    for regex in REGEX_TESTS {
        group.bench_function(format!("ziggurat regex lexicon lookup \"{}\"", regex), |b| z_regex_lexicon_lookup(b, regex));
        group.bench_function(format!("libcl regex lexicon lookup \"{}\"", regex), |b| c_regex_lexicon_lookup(b, regex));
    }

    // Layer Scan Baseline (single type)
    group.bench_function("ziggurat regex layer scan", z_baseline_layer_scan);
    group.bench_function("libcl regex layer scan", c_baseline_layer_scan);

    // Layer Scan using RegEx
    for regex in REGEX_TESTS {
        group.bench_function(format!("ziggurat regex layer scan \"{}\"", regex), |b| z_regex_layer_scan(b, regex));
        group.bench_function(format!("libcl regex layer scan \"{}\"", regex), |b| c_regex_layer_scan(b, regex));
    }


    //
    // Step 2
    // decode postings lists

    // decoding only withouth compiling a complete list/sorting
    //

    // Decoding of example type lists
    group.bench_function("ziggurat mixed postings decode", |b| z_typelist_postings_decode(b, &MIXED_TYPES));
    group.bench_function("ziggurat top postings decode", |b| z_typelist_postings_decode(b, &TOP_TYPES));
    group.bench_function("ziggurat med postings decode", |b| z_typelist_postings_decode(b, &MEDFREQ_TYPES));
    group.bench_function("ziggurat low postings decode", |b| z_typelist_postings_decode(b, &LOWFREQ_TYPES));
    group.bench_function("ziggurat hapax postings decode", |b| z_typelist_postings_decode(b, &HAPAX_TYPES));

    group.bench_function("libcl mixed postings decode", |b| c_typelist_postings_decode(b, &MIXED_TYPES));
    group.bench_function("libcl top postings decode", |b| c_typelist_postings_decode(b, &TOP_TYPES));
    group.bench_function("libcl med postings decode", |b| c_typelist_postings_decode(b, &MEDFREQ_TYPES));
    group.bench_function("libcl low postings decode", |b| c_typelist_postings_decode(b, &LOWFREQ_TYPES));
    group.bench_function("libcl hapax postings decode", |b| c_typelist_postings_decode(b, &HAPAX_TYPES));

    // Decode the postings lists produced by the RegEx examples
    for regex in REGEX_TESTS {
        group.bench_function(format!("ziggurat regex postings decode \"{}\"", regex), |b| z_regex_postings_decode(b, regex));
        group.bench_function(format!("libcl regex postings decode \"{}\"", regex), |b| c_regex_postings_decode(b, regex));
    }

    // Decode ALL postings lists
    group.bench_function("ziggurat all postings decode", z_all_postings_decode);
    group.bench_function("libcl all postings decode", c_all_postings_decode);

    
    // decoding plus gathering
    //

    // gathering of example type lists
    group.bench_function("ziggurat mixed postings gather", |b| z_typelist_postings_gather(b, &MIXED_TYPES));
    group.bench_function("ziggurat top postings gather", |b| z_typelist_postings_gather(b, &TOP_TYPES));
    group.bench_function("ziggurat med postings gather", |b| z_typelist_postings_gather(b, &MEDFREQ_TYPES));
    group.bench_function("ziggurat low postings gather", |b| z_typelist_postings_gather(b, &LOWFREQ_TYPES));
    group.bench_function("ziggurat hapax postings gather", |b| z_typelist_postings_gather(b, &HAPAX_TYPES));

    group.bench_function("libcl mixed postings gather", |b| c_typelist_postings_gather(b, &MIXED_TYPES));
    group.bench_function("libcl top postings gather", |b| c_typelist_postings_gather(b, &TOP_TYPES));
    group.bench_function("libcl med postings gather", |b| c_typelist_postings_gather(b, &MEDFREQ_TYPES));
    group.bench_function("libcl low postings gather", |b| c_typelist_postings_gather(b, &LOWFREQ_TYPES));
    group.bench_function("libcl hapax postings gather", |b| c_typelist_postings_gather(b, &HAPAX_TYPES));

    // gather the postings lists produced by the RegEx examples
    for regex in REGEX_TESTS {
        group.bench_function(format!("ziggurat regex postings gather \"{}\"", regex), |b| z_regex_postings_gather(b, regex));
        group.bench_function(format!("libcl regex postings gather \"{}\"", regex), |b| c_regex_postings_gather(b, regex));
    }
}
