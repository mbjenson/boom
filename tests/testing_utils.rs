use boom::structs;
use boom::structs::Candidate;
use boom::structs::Detection;
use boom::utils;
use futures::stream::{FuturesUnordered, StreamExt};
use mongodb::{
    options::ClientOptions,
    Client,
    Collection,
    bson::Document,
};
use apache_avro::Reader;
use std::{
    error::Error,
    fs::File,
    io::BufReader,
};

pub const EPS: f64 = 0.00000005;
// utilities for writting tests for boom


pub async fn test_helper_crossmatch_parallel(
    ra: f64, ra_geojson: f64, dec: f64,
    collections: &Vec<(&str, Collection<mongodb::bson::Document>)>,
    crossmatch_configs: &Vec<(&str, structs::CrossmatchConfig)>
) -> Result<std::collections::HashMap<String, Vec<mongodb::bson::Document>>, Box<dyn std::error::Error>> {
    
    // 1. run crossmatch_parallel() on data
    let crossmatch_result = utils::crossmatch_parallel(ra, ra_geojson, dec, collections, crossmatch_configs).await?;

    // 2. manually perform operations on data here and compare the results
    let mut query_futures = FuturesUnordered::new();
    for (collection_name, collection) in collections {
        let crossmatch_config = &crossmatch_configs
            .iter()
            .find(|(name, _)| name == collection_name)
            .unwrap().1;

        let query_future = utils::cone_search_named(
            collection_name, ra_geojson, dec, 
            crossmatch_config.radius.clone(), collection);
        query_futures.push(query_future);
    }

    let mut results = std::collections::HashMap::new();
    while let Some(result) = query_futures.next().await {
        match result {
            Ok((collection_name, documents)) => {
                let crossmatch_config = &crossmatch_configs
                    .iter()
                    .find(|(name, _)| name == &collection_name)
                    .unwrap().1;
                if crossmatch_config.use_distance {
                    let distance_unit = &crossmatch_config.distance_unit;
                    let distance_key = &crossmatch_config.distance_key;
                    let max_distance = crossmatch_config.distance_max;
                    let max_distance_near = crossmatch_config.distance_max_near;
                    let mut new_documents: Vec<mongodb::bson::Document> = Vec::new();
                    for doc in documents {
                        let doc_ra_option = match doc.get("ra") {
                            Some(ra) => ra.as_f64(),
                            _ => {
                                continue;
                            }
                        };
                        if doc_ra_option.is_none() {
                            continue;
                        }
                        let doc_ra = doc_ra_option.unwrap();
                        let doc_dec_option = match doc.get("dec") {
                            Some(dec) => dec.as_f64(),
                            _ => {
                                continue;
                            }
                        };
                        if doc_dec_option.is_none() {
                            continue;
                        }
                        let doc_dec = doc_dec_option.unwrap();
                        if distance_unit == "redshift" {

                            let result = utils::distance_filter_redshift(
                                doc.clone(), distance_key, 
                                max_distance_near, max_distance, 
                                (doc_ra, doc_dec), (ra, dec));
                            match result {
                                Some(x) => new_documents.push(x),
                                _ => continue,
                            }
                            println!("doc:\n{:?}, distance_key:\n{:?},
                                max_distance_near:\n{:?}, max_distance:\n{:?}, 
                                doc_ra:\n{:?}, doc_dec:\n{:?}, ra:\n{:?}, dec:\n{:?}", 
                                doc.clone(), distance_key, max_distance_near, 
                                max_distance, doc_ra, doc_dec, ra, dec
                            );
                            let _ = test_helper_distance_filter_redshift(
                                doc, distance_key, 
                                max_distance_near, max_distance, 
                                (doc_ra, doc_dec), (ra, dec));

                        } else if distance_unit == "mpc" {
                            let result = utils::distance_filter_mpc(
                                doc.clone(), distance_key, max_distance_near, max_distance, 
                                (doc_ra, doc_dec), (ra, dec));
                            match result {
                                Some(x) => new_documents.push(x),
                                _ => continue,
                            }

                            let _ = test_helper_distance_filter_mpc(
                                doc, distance_key,
                                max_distance_near, max_distance,
                                (doc_ra, doc_dec), (ra, dec));

                        } else {
                            unimplemented!();
                        }
                    }
                    results.insert(collection_name, new_documents);
                } else {
                    results.insert(collection_name, documents);
                }
            }
            Err(e) => {
                panic!("Error: {:?}", e);
            }
        }
    }

    // 3. compare results
    assert_eq!(results, crossmatch_result);

    Ok(results)
}


pub async fn test_helper_distance_filter_mpc(
    doc: mongodb::bson::Document, distance_key: &String,
    max_distance_near: f64, max_distance: f64,
    doc_celestial: (f64, f64), celestial: (f64, f64),
) -> Result<(), Box<dyn Error>> {
    // 1. run func on input
    let result = utils::distance_filter_mpc(
        doc.clone(), distance_key, 
        max_distance_near, max_distance, 
        doc_celestial, celestial);
    // 2. manually calculate the result
    let result_man: Option<mongodb::bson::Document> = loop {            
        let doc_ra = doc_celestial.0;
        let doc_dec = doc_celestial.1;

        let ra = celestial.0;
        let dec = celestial.1;

        let doc_mpc_option = match doc.get(&distance_key) {
            // mpc could be f64 or i32, so try both
            Some(mpc) => {
                let mpc_f64 = mpc.as_f64();
                if mpc_f64.is_none() {
                    let mpc_i32 = mpc.as_i32();
                    if mpc_i32.is_none() {
                        None
                    } else {
                        Some(mpc_i32.unwrap() as f64)
                    }
                } else {
                    mpc_f64
                }
            }
            _ => {
                println!("No mpc");
                break None;
            }
        };
        if doc_mpc_option.is_none() {
            // also print the distance key we are using
            println!("Mpc is none using {}", distance_key);
            // print the document _id
            println!("{:?}", doc.get("_id"));
            break None;
        }
        let doc_mpc = doc_mpc_option.unwrap();
        let cm_radius = if doc_mpc < 40.0 {
            max_distance_near / 3600.0 // to degrees
        } else {
            (max_distance / (doc_mpc * 1000.0)) // 10**3
                .atan()
                .to_degrees()
        };
        if utils::in_ellipse(ra, dec, doc_ra, doc_dec, cm_radius, 1.0, 0.0) {
            // here we don't * 3600.0 yet because we need to calculate the distance in kpc first
            let angular_separation = utils::great_circle_distance(ra, dec, doc_ra, doc_dec);
            // calculate the distance between objs in kpc
            let distance_kpc = if doc_mpc > 0.005 {
                angular_separation.to_radians() * (doc_mpc * 1000.0)
            } else {
                -1.0
            };
            // overwrite doc_copy with doc_copy + the angular separation and the distance in kpc
            // multiply by 3600.0 to get the angular separation in arcseconds
            let mut doc_copy = doc.clone();
            doc_copy.insert("angular_separation", angular_separation * 3600.0);
            doc_copy.insert("distance_kpc", distance_kpc);
            break Some(doc_copy);
        }
        break None;
    };

    // 3. compare the results
    assert_eq!(result.unwrap(), result_man.unwrap());

    Ok(())
}

pub async fn test_helper_distance_filter_redshift(
    doc: mongodb::bson::Document, distance_key: &String,
    max_distance_near: f64, max_distance: f64,
    doc_celestial: (f64, f64), celestial: (f64, f64),
    // take in the params that distance_filter_redshift would and 
    // compare function output to the desired, manually calculated result
) -> Result<(), Box<dyn Error>> {
    // 1. run func on input
    let result = utils::distance_filter_redshift(
        doc.clone(), distance_key, 
        max_distance_near, max_distance, 
        doc_celestial, celestial);
    
    // 2. manually calculate result

    // using a loop hack which allows the simulation of a 
    // function call with various potential exit points
    let result_man: Option<mongodb::bson::Document> = loop {
        let ra = celestial.0;
        let dec = celestial.1;
        let doc_ra = doc_celestial.0;
        let doc_dec = doc_celestial.1;

        let doc_z_option = match doc.get(&distance_key) {
            Some(z) => z.as_f64(),
            _ => {
                break None;
            }
        };
        if doc_z_option.is_none() {
            break None;
        }
        let doc_z = doc_z_option.unwrap();

        let cm_radius = if doc_z < 0.01 {
            max_distance_near / 3600.0
        } else {
            max_distance * (0.05 / doc_z) / 3600.0
        };

        if utils::in_ellipse(
            ra, dec, doc_ra, 
            doc_dec, cm_radius, 1.0, 0.0)
        {
            let angular_seperation = 
                utils::great_circle_distance(
                    ra, dec, 
                    doc_ra, doc_dec) * 3600.0;
            // calculate distance between objs in kpc
            let distance_kpc = if doc_z > 0.005 {
                angular_seperation * (doc_z / 0.05)
            } else {
                -1.0
            };
            
            let mut doc_copy = doc.clone();
            // overwrite doc_copy with doc_copy + the angular seperation
            // and the distance in kpc
            doc_copy.insert("angular_seperation", angular_seperation);
            doc_copy.insert("distance_kpc", distance_kpc);
            break Some(doc_copy);
        }
        break None;
    };

    // 3. compare results
    assert_eq!(result.unwrap(), result_man.unwrap());
    
    Ok(())
}


pub fn build_alert_queue(
    alert_path: String,
    num_alerts: usize,
) -> Result<Vec<apache_avro::types::Value>, Box<dyn Error>> {
    let mut index = 0 as usize;
    let files = utils::get_file_names(String::from(alert_path));
    let mut queue = Vec::<apache_avro::types::Value>::new();

    while index < files.len() && index < num_alerts {
        let file_name = files[index].clone();
        let file = File::open(file_name).unwrap();
        let reader = Reader::new(BufReader::new(file)).unwrap();
        for record in reader {
            let record = record.unwrap();
            queue.push(record);
        }
        index += 1;
    }
    Ok(queue)
}


pub fn get_test_crossmatching_vecs(client: mongodb::Client) -> (
    Vec<(&'static str, Collection<Document>)>, 
    Vec<(&'static str, structs::CrossmatchConfig)>
) {
    let milliquas_v8: Collection<mongodb::bson::Document> =
        client.database("kowalski").collection("milliquas_v8");
    let clu: Collection<mongodb::bson::Document> =
        client.database("kowalski").collection("CLU");
    let ned: Collection<mongodb::bson::Document> =
        client.database("kowalski").collection("NED");

    let crossmatching_collections =
        vec![("milliquas_v8", milliquas_v8), ("CLU", clu), ("NED", ned)];

    let milliquas_config = structs::CrossmatchConfig {
        radius: 2.0,
        use_distance: false,
        ..Default::default()
    };
    let clu_config = structs::CrossmatchConfig {
        radius: 10800.0, // 3 degrees in arcseconds
        use_distance: true,
        distance_key: "z".to_string(),
        distance_max: 30.0,       // 30 Kpc
        distance_max_near: 300.0, // 5 arcsec for objects that are too close (z < 0.01)
        distance_unit: "redshift".to_string(),
    };
    let ned_config = structs::CrossmatchConfig {
        radius: 10800.0, // 3 degrees in arcseconds
        use_distance: true,
        distance_key: "DistMpc".to_string(),
        distance_max: 30.0,       // 30 Kpc
        distance_max_near: 300.0, // 5 arcsec for objects that are too close (z < 0.01)
        distance_unit: "Mpc".to_string(),
    };
    // hashmap with the config per crossmatching collection
    let crossmatching_config = vec![
        ("milliquas_v8", milliquas_config),
        ("CLU", clu_config),
        ("NED", ned_config),
    ];
    (crossmatching_collections, crossmatching_config)
}

pub async fn create_test_alert_collections(
    client: mongodb::Client
) -> Result<(), Box<dyn Error>> {
    let _ = client.database("kowalski")
        .create_collection("test_alerts", None).await?;
    let _ = client.database("kowalski")
        .create_collection("test_alerts_aux", None).await?;
    Ok(())
}

pub fn get_test_alert_collections(client: mongodb::Client) -> (
    mongodb::Collection<structs::AlertWithCoords>, 
    mongodb::Collection<structs::AlertAux>
) {
    let collection = client.database("kowalski").collection("test_alerts");
    let collection_aux = client.database("kowalski").collection("test_alerts_aux");
    (collection, collection_aux)
}

pub async fn drop_test_alert_collections(
    client: mongodb::Client
) -> Result<(), Box<dyn Error>> {
    let _ = client.database("kowalski")
        .collection::<structs::AlertWithCoords>("test_alerts").drop(None).await;
    let _ = client.database("kowalski")
        .collection::<structs::AlertAux>("test_alerts_aux").drop(None).await;
    Ok(())
}

pub async fn setup_mongo_client() -> Result<mongodb::Client, mongodb::error::Error> {
    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    Ok(client)
}

// does the same as the regular alert worker but uses the test database collections
// pub async fn test_alert_worker(
//     queue: Arc<Mutex<Vec<apache_avro::types::Value>>>
// ) -> Result<(), Box<dyn Error>> {
//     let client = setup_mongo_client().await?;
//     let collections = get_test_alert_collections(client.clone());
//     let crossmatching_vecs = get_test_crossmatching_vecs(client.clone());
    
//     let mut object_id: String;

//     loop {
//         let record = queue.lock().unwrap().pop();
//         match record {
//             Some(record) => {

//                 let alert: structs::AlertPacket = from_value(&record).unwrap();
//                 object_id = alert.objectId.clone();

//                 alerts::process_record(
//                     record,
//                     &collections.0,
//                     &collections.1,
//                     &crossmatching_vecs.0,
//                     &crossmatching_vecs.1,
//                 ).await?;

//                 let _ = client.database("kowalski")
//                     .collection::<structs::AlertWithCoords>("test_alerts")
//                     .find_one(doc!{ "_id": object_id }, None).await;
//             }
//             None => {
//                 continue;
//                 //panic!("process_alert(record, ...) record was None inside test_worker")
//             }
//         }
//     }
//     Ok(())
// }

pub fn are_candidates_eq(c1: Candidate, c2: Candidate) {
    are_detections_eq(c1.detection, c2.detection);
    assert_eq!(c1.ssdistnr, c2.ssdistnr);
    assert_eq!(c1.ssmagnr, c2.ssmagnr);
    assert_eq!(c1.ssnamenr, c2.ssnamenr);
    assert_eq!(c1.ranr, c2.ranr);
    assert_eq!(c1.decnr, c2.decnr);
    assert_eq!(c1.ndethist, c2.ndethist);
    assert_eq!(c1.ncovhist, c2.ncovhist);
    assert_eq!(c1.jdstarthist, c2.jdstarthist);
    assert_eq!(c1.jdendhist, c2.jdendhist);
    assert_eq!(c1.tooflag, c2.tooflag);
    assert_eq!(c1.objectidps1, c2.objectidps1);
    assert_eq!(c1.sgmag1, c2.sgmag1);
    assert_eq!(c1.srmag1, c2.srmag1);
    assert_eq!(c1.simag1, c2.simag1);
    assert_eq!(c1.szmag1, c2.szmag1);
    assert_eq!(c1.sgscore1, c2.sgscore1);
    assert_eq!(c1.distpsnr1, c2.distpsnr1);
    assert_eq!(c1.objectidps2, c2.objectidps2);
    assert_eq!(c1.sgmag2, c2.sgmag2);
    assert_eq!(c1.srmag2, c2.srmag2);
    assert_eq!(c1.simag2, c2.simag2);
    assert_eq!(c1.szmag2, c2.szmag2);
    assert_eq!(c1.sgscore2, c2.sgscore2);
    assert_eq!(c1.distpsnr2, c2.distpsnr2);
    assert_eq!(c1.objectidps3, c2.objectidps3);
    assert_eq!(c1.sgmag3, c2.sgmag3);
    assert_eq!(c1.srmag3, c2.srmag3);
    assert_eq!(c1.simag3, c2.simag3);
    assert_eq!(c1.szmag3, c2.szmag3);
    assert_eq!(c1.sgscore3, c2.sgscore3);
    assert_eq!(c1.distpsnr3, c2.distpsnr3);
    assert_eq!(c1.nmtchps, c2.nmtchps);
    assert_eq!(c1.rfid, c2.rfid);
    assert_eq!(c1.jdstartref, c2.jdstartref);
    assert_eq!(c1.jdendref, c2.jdendref);
    assert_eq!(c1.nframesref, c2.nframesref);
    assert_eq!(c1.dsnrms, c2.dsnrms);
    assert_eq!(c1.ssnrms, c2.ssnrms);
    assert_eq!(c1.dsdiff, c2.dsdiff);
    assert_eq!(c1.nmatches, c2.nmatches);
    assert_eq!(c1.zpclrcov, c2.zpclrcov);
    assert_eq!(c1.zpmed, c2.zpmed);
    assert_eq!(c1.clrmed, c2.clrmed);
    assert_eq!(c1.clrrms, c2.clrrms);
    assert_eq!(c1.neargaia, c2.neargaia);
    assert_eq!(c1.neargaiabright, c2.neargaiabright);
    assert_eq!(c1.maggaia, c2.maggaia);
    assert_eq!(c1.maggaiabright, c2.maggaiabright);
    assert_eq!(c1.drb, c2.drb);
}

pub fn are_detections_eq(d1: Detection, d2: Detection) {
    assert_eq!(d1.candid, d2.candid);
    assert_eq!(d1.isdiffpos, d2.isdiffpos);
    assert_eq!(d1.tblid, d2.tblid);
    assert_eq!(d1.xpos, d2.xpos);
    assert_eq!(d1.ypos, d2.ypos);
    assert_eq!(d1.ra, d2.ra);
    assert_eq!(d1.dec, d2.dec);
    assert_eq!(d1.magpsf, d2.magpsf);
    assert_eq!(d1.sigmapsf, d2.sigmapsf);
    assert_eq!(d1.chipsf, d2.chipsf);
    assert_eq!(d1.magap, d2.magap);
    assert_eq!(d1.sigmagap, d2.sigmagap);
    assert_eq!(d1.distnr, d2.distnr);
    assert_eq!(d1.magnr, d2.magnr);
    assert_eq!(d1.sigmagnr, d2.sigmagnr);
    assert_eq!(d1.chinr, d2.chinr);
    assert_eq!(d1.sharpnr, d2.sharpnr);
    assert_eq!(d1.sky, d2.sky);
    assert_eq!(d1.magdiff, d2.magdiff);
    assert_eq!(d1.fwhm, d2.fwhm);
    assert_eq!(d1.classtar, d2.classtar);
    assert_eq!(d1.mindtoedge, d2.mindtoedge);
    assert_eq!(d1.magfromlim, d2.magfromlim);
    assert_eq!(d1.seeratio, d2.seeratio);
    assert_eq!(d1.aimage, d2.aimage);
    assert_eq!(d1.bimage, d2.bimage);
    assert_eq!(d1.aimagerat, d2.aimagerat);
    assert_eq!(d1.bimagerat, d2.bimagerat);
    assert_eq!(d1.elong, d2.elong);
    assert_eq!(d1.nneg, d2.nneg);
    assert_eq!(d1.nbad, d2.nbad);
    assert_eq!(d1.rb, d2.rb);
    assert_eq!(d1.drb, d2.drb);
    assert_eq!(d1.drbversion, d2.drbversion);
    assert_eq!(d1.sumrat, d2.sumrat);
    assert_eq!(d1.magapbig, d2.magapbig);
    assert_eq!(d1.sigmagapbig, d2.sigmagapbig);
    assert_eq!(d1.ranr, d2.ranr);
    assert_eq!(d1.decnr, d2.decnr);
    assert_eq!(d1.scorr, d2.scorr);
    assert_eq!(d1.exptime, d2.exptime);
    are_nondetections_eq(d1.non_detection.unwrap(), d2.non_detection.unwrap())
}

pub fn are_nondetections_eq(a: structs::NonDetection, b: structs::NonDetection) {
    assert_eq!(a.jd, b.jd);
    assert_eq!(a.fid, b.fid);
    assert_eq!(a.pid, b.pid);
    assert_eq!(a.diffmaglim, b.diffmaglim);
    assert_eq!(a.pdiffimfilename, b.pdiffimfilename);
    assert_eq!(a.programpi, b.programpi);
    assert_eq!(a.programid, b.programid);
    assert_eq!(a.nid, b.nid);
    assert_eq!(a.rcid, b.rcid);
    assert_eq!(a.field, b.field);
    assert_eq!(a.rbversion, b.rbversion);
    assert_eq!(a.magzpsci, b.magzpsci);
    assert_eq!(a.magzpsciunc, b.magzpsciunc);
    assert_eq!(a.magzpscirms, b.magzpscirms);
    assert_eq!(a.clrcoeff, b.clrcoeff);
    assert_eq!(a.clrcounc, b.clrcounc);
}

pub fn are_coordinates_eq(a: structs::AlertCoordinates, b: structs::AlertCoordinates) {
    assert_eq!(a.radec_str.0, b.radec_str.0);
    assert_eq!(a.radec_str.1, b.radec_str.1);
    assert_eq!(a.radec_geojson.coordinates.0, b.radec_geojson.coordinates.0);
    assert_eq!(a.radec_geojson.coordinates.1, b.radec_geojson.coordinates.1);
    assert_eq!(a.galactic.l, b.galactic.l);
    assert_eq!(a.galactic.b, b.galactic.b);
}

pub fn are_cutouts_eq(a: structs::Cutout, b: structs::Cutout) {
    assert_eq!(a.fileName, b.fileName);
    assert_eq!(a.stampData, b.stampData);
}