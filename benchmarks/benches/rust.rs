use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use etemenanki::Datastore;
use regex::Regex;

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn open_ziggurat() -> Datastore<'static> {
    // open ziggurat datastore
    Datastore::open("ziggurat").unwrap()
}

//
// Rust Tests
//

fn pattern_prefix(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for i in words.lexicon().all_starting_with("be") {
            black_box(i);
        }
    })
}

fn regex_prefix(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for i in words.lexicon().all_matching_regex("^be").unwrap() {
            black_box(i);
        }
    })
}

fn pattern_contains(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for i in words.lexicon().all_containing("beam") {
            black_box(i);
        }
    })
}

fn regex_contains(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for i in words.lexicon().all_matching_regex("beam").unwrap() {
            black_box(i);
        }
    })
}

// Lookup Strategy 1:
// Sequentially scanning the whole variable while regex-matching each token 
fn layer_scan(b: &mut Bencher) {
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

// Lookup Strategy 2:
// Scanning the variable's lexicon for all matching strings and then using the variable's ReverseIndex component for actual result lookup
fn lexicon_scan(b: &mut Bencher) {
    let datastore = open_ziggurat();
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
    let mut group = c.benchmark_group("rust tests");
    group.sample_size(10);
    group.measurement_time(Duration::new(60, 0));
    // group.measurement_time(Duration::new(600, 0));
    group.sampling_mode(criterion::SamplingMode::Flat);

    // Prefix Search
    group.bench_function("rust pattern prefix", pattern_prefix);
    group.bench_function("regex prefix", regex_prefix);

    // Containment Search
    group.bench_function("rust pattern containment", pattern_contains);
    group.bench_function("regex containment", regex_contains);

    // Lookup Strategy
    group.bench_function("regex lookup 1", layer_scan);
    group.bench_function("regex lookup 2", lexicon_scan);
}
