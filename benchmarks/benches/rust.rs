use std::{hash::{DefaultHasher, Hash, Hasher}, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use etemenanki::{components::FnvHash, Datastore};

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

include!("common.rs");
use common::*;

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

fn lexhash_fnv(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for t in words.lexicon() {
            black_box(t.fnv_hash());
        }
    })
}

fn lexhash_rust(b: &mut Bencher) {
    let datastore = open_ziggurat();
    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    b.iter(|| {
        for t in words.lexicon() {
            let mut hasher = DefaultHasher::new();
            t.hash(&mut hasher);
            black_box(hasher.finish());
        }
    })
}

//
// Criterion Main
//

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("rust tests");
    group.sample_size(50);
    group.measurement_time(Duration::new(60, 0));
    // group.measurement_time(Duration::new(600, 0));
    group.sampling_mode(criterion::SamplingMode::Flat);

    // Prefix Search
    group.bench_function("rust pattern prefix", pattern_prefix);
    group.bench_function("regex prefix", regex_prefix);

    // Containment Search
    group.bench_function("rust pattern containment", pattern_contains);
    group.bench_function("regex containment", regex_contains);

    // Hash Performance
    group.bench_function("lexicon hash fnv", lexhash_fnv);
    group.bench_function("lexicon hash rust", lexhash_rust);
}
