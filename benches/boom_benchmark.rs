use criterion::{black_box, criterion_group, criterion_main, Criterion};
use boom::utils::*;

pub fn deg2dms_benchmark(c: &mut Criterion) {
    c.bench_function("deg2dms", |b| b.iter(|| deg2dms(black_box(50f64))));
}

criterion_group!(benches, deg2dms_benchmark);
criterion_main!(benches);
