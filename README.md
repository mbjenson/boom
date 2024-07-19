# Boom:unit
## Purpose
This branch is a development space for several units of the Kowalski rust backend

## build
compile with `cargo build`
\
\- or \-
\
compile and run: `cargo run`

### note:

the program is, by default, configured to run on the 10 sample_alert files. \
to run the program on the other alerts, change the alert_path string in main.rs to "./data/alerts".

## Test Data
10 sample alert data files are located in `./data/sample_alerts`.
To download more alerts, run the `download-alerts.py` script in ./data:

`python ./data/download-alerts.py`

More testing alert data can be found here https://ztf.uw.edu/alerts/public/.
