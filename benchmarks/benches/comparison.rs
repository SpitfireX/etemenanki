use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng};
use etemenanki::Datastore;
use libcl_rs::Corpus;

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn setup_rand(len: usize, max: usize) -> Vec<usize> {
    let rng = StdRng::seed_from_u64(42);
    let dist = Uniform::new(0, max);
    Vec::from_iter(dist.sample_iter(rng).take(len))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison tests");
    group.sample_size(100);
    group.measurement_time(Duration::new(60, 0));
    // group.measurement_time(Duration::new(600, 0));

    // open ziggurat datastore
    let datastore = Datastore::open("../etemenanki/testdata/simpledickens").unwrap();
    // open CWB corpus
    let corpus = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");


    // Sequential Layer Decode
    group.bench_function("ziggurat sequential layer decode", | b | {
        let words = datastore["primary"]["word"]
            .as_indexed_string()
            .unwrap();
        
        b.iter(|| {
            for s in words.iter() {
                black_box(s);
            }
        })
    });

    group.bench_function("libcl sequential layer decode", | b | {
        let words = corpus.get_p_attribute("word").unwrap();

        b.iter(|| {
            for cpos in 0..words.max_cpos().unwrap() {
                black_box(words.cpos2str(cpos).unwrap());
            }
        })
    });

    // Random Layer Decode
    group.bench_function("ziggurat random layer decode", | b | {
        let words = datastore["primary"]["word"]
            .as_indexed_string()
            .unwrap();
        let positions = setup_rand(words.len(), words.len());

        b.iter(|| {
            for cpos in positions.iter() {
                black_box(words.get(*cpos).unwrap());
            }
        })
    });

    group.bench_function("libcl random layer decode", | b | {
        let words = corpus.get_p_attribute("word").unwrap();
        let positions = setup_rand(words.max_cpos().unwrap() as usize, words.max_cpos().unwrap() as usize);

        b.iter(|| {
            for cpos in positions.iter() {
                black_box(words.cpos2str(*cpos as i32).unwrap());
            }
        })
    });
}
