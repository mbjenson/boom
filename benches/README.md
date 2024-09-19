# Benchmarks for boom
## Usage
### Run
To run a benchmark\
`cargo bench <benchmark_name> [-- ARGS]`

### Benchmarks

#### filtering benchmark

`cargo bench filter_benchmark -- <num_iterations> <filter_id>`\
*if one or both of the arguments are not supplied the benchmark will run 20 iterations on a default filter*

 - **Example**:\
Run filter benchmark on 200 iterations of candids with filter_id of 2
`cargo bench filter_benchmark -- 200 2`
