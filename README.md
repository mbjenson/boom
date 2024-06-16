## BOOM

### *Disclaimer ⚠️: This is a work in progress, not a usable system for alert brokering.*
#### System dependencies:
- [rust](https://www.rust-lang.org/tools/install)
- [rabbitmq](https://www.rabbitmq.com/docs/download#installation-guides)

#### Run it:
1. In a terminal, run `cargo build`
2. In another terminal, run `cargo run --bin publisher`. This will start pushing the demo alert (from `data/alert.avro`) to a work queue.
3. In yet another terminal, run `cargo run --bin consumer`. This will spawn one consumer that will read from the queue and print the candid + objectId of the alert.

##### TODOs:
- [ ] Add unit tests, which requires to have rabbitmq installed (and later mongodb) in a github workflow.
- [ ] Have each worker process the alert and push them to mongo.
- [ ] Write a `babysitter` process that given a min and max number of workers, will add/remove workers based on the "load", here being the number of alerts pending in the rabbitmq queue.
