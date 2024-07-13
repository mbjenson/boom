## BOOM: distributed

On this branch, we experiment with distributed systems for distributed/parallel computing.

##### Run the queue
```bash
cargo run --bin queue
```

##### Run the worker
```bash
cargo run --bin worker
```

##### Log output
```bash
cargo run --bin watch_log <log_file>
```

where `<log_file>` is the name of the file to read from (e.g. `worker.log`, or `queue.log`). The `.log` extension can be omitted.
