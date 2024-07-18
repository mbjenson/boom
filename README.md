# Boom:unit
## Purpose
This branch is a development space for several units of the Kowalski rust backend

## build
compile and run: `cargo run`

## Test Data
10 sample alert data files are located in `./data/sample_alerts`.

If more alerts are needed for testing, a compressed tar.gz file with ~4.5k alerts is located in the `./data` directory. To extract these files, run the following command from the project base directory:

`tar -xvzf ./data/ztf_public_20240312.tar.gz --directory ./data/alerts`

More testing alert data can be found here https://ztf.uw.edu/alerts/public/.

