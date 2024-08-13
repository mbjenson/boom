# Benchmarking
The Criterion rust library is used for benchmarking.

## usage
### Synopsis
`cargo bench` [options][benchname][-- bench-options]

### Run all benchmarks
`cargo bench`

### Run specific benchmark
run all benchmarks with specific string sequence in benchmark name:\
`cargo bench` [benchname]

run only the benchmark called 'foo' and skip other similarly named benchmarks:\
`cargo bench -- foo --exact`

### View Results
After running a benchmark, detailed results and information about the benchmark can be found in
`boom/target/criterion/<name_of_benchmark>`. Here you will find an `index.html` which contains graphs, stats, and other useful stuff.

## info
To learn about criterion-rs, visit: https://bheisler.github.io/criterion.rs/book/getting_started.html
