use criterion::{black_box, criterion_group, criterion_main};
use criterion::Criterion;
use boom::utils;
use boom::alerts;
mod bench_utils;
use bench_utils as bu;

// benchmarking function with single input
pub fn deg2dms_benchmark(c: &mut Criterion) {
    c.bench_function("deg2dms", |b| b.iter(|| utils::deg2dms(black_box(50f64))));
}

// run benchmark on alert::process_record() with loaded alerts from file
pub fn process_record_benchmark(c: &mut Criterion) {

    let runtime = tokio::runtime::Runtime::new().unwrap();
    // connect to mongodb
    let client = runtime.block_on(async {
        bu::setup_mongo_client().await.unwrap()
    });
    runtime.block_on( async {
        let _ = bu::drop_test_alert_collections(client.clone()).await;
    });
    
    let collections = bu::get_alert_collections(client.clone());
    let crossmatching = bu::get_test_crossmatching_vecs(client.clone());

    // load alerts into queue
    let num_alerts = 10; // number of alerts in queue
    let mut queue = bu::build_alert_queue(
        String::from("./data/sample_alerts"), num_alerts).unwrap();
    
    let mut group = c.benchmark_group("process_record");
    for i in 0..queue.len() {
        group.bench_with_input(
            format!("record {:?}", i), 
            &queue[i],
            |b, record| b.iter(|| alerts::process_record(
                record.clone(),
                &collections.0, 
                &collections.1, 
                &crossmatching.0, 
                &crossmatching.1))
        );   
    }
    group.finish()
    
    // async syntax that needs to be saved for later
    // c.bench_function(
    //     "process_record", 
    //     |b| {
    //         b.to_async(&runtime).iter( || alerts::process_record(
    //         queue.pop().unwrap(), 
    //         &collections.0,
    //         &collections.1,
    //         &crossmatching.0,
    //         &crossmatching.1,
    //     )) }
    // );
}

criterion_group!(benches, deg2dms_benchmark, process_record_benchmark);
criterion_main!(benches);
