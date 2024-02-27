use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use etemenanki::{variables::IndexedStringVariable, Datastore};

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n-1) + fibonacci(n-2),
    }
}

fn linear_scan(svar: &IndexedStringVariable, b: &mut Bencher) {

    b.iter(|| {
        for s in svar.iter() {
            black_box(s);
        }
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| b.iter(|| fibonacci(black_box(20))));

    // open ziggurat datastore
    let datastore = Datastore::open("../etemenanki/testdata/simpledickens").unwrap();

    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    c.bench_function("ziggurat linear scan", |b| linear_scan(words, b));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
