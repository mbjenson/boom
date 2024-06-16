## BOOM

### *Disclaimer ⚠️: This is a work in progress, not a usable system for alert brokering.*
#### System dependencies:
- [rust](https://www.rust-lang.org/tools/install)
- [rabbitmq](https://www.rabbitmq.com/docs/download#installation-guides)
- [mongodb](https://www.mongodb.com/docs/manual/installation/)

##### Installation

On macOS, we recommend [Homebrew](https://brew.sh/)
* `brew install rabbitmq`
* `brew services start rabbitmq`
* `/opt/homebrew/sbin/rabbitmqctl enable_feature_flag all`
* brew install mongodb-community
* brew services start mongodb-community OR mongod --config /opt/homebrew/etc/mongod.conf (NB: storage.dbPath must be a writeable location)

#### Run it:
1. Add the rabbitmq_management plugin with `sudo rabbitmq-plugins enable rabbitmq_management` (Linux) or `/opt/homebrew/sbin/rabbitmq-plugins enable rabbitmq_management` (macOs). Then head to the [dashboard's exchanges section](http://localhost:15672/#/exchanges), where the default username and password combination for a new install is guest, guest, and add an exchange called `ztf_alerts`.
2. In a terminal, run `cargo build`
3. In another terminal, run `cargo run --bin producer`. This will start pushing the demo alert (from `data/alerts/alert.avro`) to a work queue.
4. In yet another terminal, run `cargo run --bin consumer`. This will spawn one consumer that will read from the queue and print the candid + objectId of the alert.
You should see that the first alert is processed successfully, and thereafter the consumer should tell you that the alert already exists in the database.

##### TODOs:
- [ ] Add unit tests, which requires to have rabbitmq + mongodb installed in a github workflow.
- [X] Have each worker process the alert and push them to mongo.
- [ ] Write a `babysitter` process that given a min and max number of workers, will add/remove workers based on the "load", here being the number of alerts pending in the rabbitmq queue.
