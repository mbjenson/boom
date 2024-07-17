#[allow(non_snake_case)]

use futures::stream::{FuturesUnordered, StreamExt};
use mongodb::{
    bson::doc,
    Collection
};
use crate::structs;


// grab the list of all files from the provided directory
pub fn get_files(dir_path: String) -> Vec<String> {
    let paths = std::fs::read_dir(dir_path).unwrap();
    let mut files = Vec::new();
    for path in paths {
        let path = path.unwrap().path();
        let path = path.to_str().unwrap().to_string();
        files.push(path);
    }
    files
}


// Rotation matrix for the conversion : x_galactic = R * x_equatorial (J2000)
// http://adsabs.harvard.edu/abs/1989A&A...218..325M
const RGE: [[f64; 3]; 3] = [
    [0.0524767519, 0.9976931946, 0.0543758425],
    [0.9939326351, -0.0548018445, -0.0969076596],
    [-0.0966835603, 0.0464756311, -0.9949210238],
];

const DEGRA: f64 = std::f64::consts::PI / 180.0;

// implement this great_circle_distance function in Rust
pub fn great_circle_distance(ra1_deg: f64, dec1_deg: f64, ra2_deg: f64, dec2_deg: f64) -> f64 {
    let ra1 = ra1_deg * DEGRA;
    let dec1 = dec1_deg * DEGRA;
    let ra2 = ra2_deg * DEGRA;
    let dec2 = dec2_deg * DEGRA;
    let delta_ra = (ra2 - ra1).abs();
    let mut distance = (dec2.sin() * delta_ra.cos()).powi(2)
        + (dec1.cos() * dec2.sin() - dec1.sin() * dec2.cos() * delta_ra.cos()).powi(2);
    distance = distance
        .sqrt()
        .atan2(dec1.sin() * dec2.sin() + dec1.cos() * dec2.cos() * delta_ra.cos());
    distance * 180.0 / std::f64::consts::PI
}

pub fn radec2lb(ra: f64, dec: f64) -> (f64, f64) {
    let ra_rad = ra.to_radians();
    let dec_rad = dec.to_radians();
    let u = vec![
        ra_rad.cos() * dec_rad.cos(),
        ra_rad.sin() * dec_rad.cos(),
        dec_rad.sin(),
    ];
    let ug = vec![
        RGE[0][0] * u[0] + RGE[0][1] * u[1] + RGE[0][2] * u[2],
        RGE[1][0] * u[0] + RGE[1][1] * u[1] + RGE[1][2] * u[2],
        RGE[2][0] * u[0] + RGE[2][1] * u[1] + RGE[2][2] * u[2],
    ];
    let x = ug[0];
    let y = ug[1];
    let z = ug[2];
    let galactic_l = y.atan2(x);
    let galactic_b = z.atan2((x * x + y * y).sqrt());
    (galactic_l.to_degrees(), galactic_b.to_degrees())
}

pub fn deg2hms(deg: f64) -> String {
    if deg <= 0.0 || deg > 360.0 {
        panic!("Invalid RA input: {}", deg);
    }

    let h = deg * 12.0 / 180.0;
    let hours = h.floor() as i32;
    let m = (h - hours as f64) * 60.0;
    let minutes = m.floor() as i32;
    let seconds = (m - minutes as f64) * 60.0;
    let hms = format!("{:02.0}:{:02.0}:{:07.4}", hours, minutes, seconds);
    hms
}

pub fn deg2dms(deg: f64) -> String {
    if deg <= -90.0 || deg >= 90.0 {
        panic!("Invalid DEC input: {}", deg);
    }

    let degrees = deg.signum() * deg.abs().floor();
    let m = (deg - degrees).abs() * 60.0;
    let minutes = m.floor();
    let seconds = (m - minutes).abs() * 60.0;
    let dms = format!("{:02.0}:{:02.0}:{:06.3}", degrees, minutes, seconds);
    dms
}

pub fn in_ellipse(
    alpha: f64,
    delta0: f64,
    alpha1: f64,
    delta01: f64,
    d0: f64,
    axis_ratio: f64,
    PAO: f64,
) -> bool {
    let d_alpha = (alpha1 - alpha) * DEGRA;
    let delta1 = delta01 * DEGRA;
    let delta = delta0 * DEGRA;
    let PA = PAO * DEGRA;
    let d = d0 * DEGRA;
    // e is the sqrt of 1.0 - axis_ratio^2
    let e = (1.0 - axis_ratio.powi(2)).sqrt();

    let t1 = d_alpha.cos();
    let t22 = d_alpha.sin();
    let t3 = delta1.cos();
    let t32 = delta1.sin();
    let t6 = delta.cos();
    let t26 = delta.sin();
    let t9 = d.cos();
    let t55 = d.sin();

    if t3 * t6 * t1 + t32 * t26 < 0.0 {
        return false;
    }

    let t2 = t1 * t1;
    let t4 = t3 * t3;
    let t5 = t2 * t4;
    let t7 = t6 * t6;
    let t8 = t5 * t7;
    let t10 = t9 * t9;
    let t11 = t7 * t10;
    let t13 = PA.cos();
    let t14 = t13 * t13;
    let t15 = t14 * t10;
    let t18 = t7 * t14;
    let t19 = t18 * t10;

    let t24 = PA.sin();

    let t31 = t1 * t3;

    let t36 = 2.0 * t31 * t32 * t26 * t6;
    let t37 = t31 * t32;
    let t38 = t26 * t6;
    let t45 = t4 * t10;

    let t56 = t55 * t55;
    let t57 = t4 * t7;

    let t60 = -t8 + t5 * t11 + 2.0 * t5 * t15
        - t5 * t19
        - 2.0 * t1 * t4 * t22 * t10 * t24 * t13 * t26
        - t36
        + 2.0 * t37 * t38 * t10
        - 2.0 * t37 * t38 * t15
        - t45 * t14
        - t45 * t2
        + 2.0 * t22 * t3 * t32 * t6 * t24 * t10 * t13
        - t56
        + t7
        - t11
        + t4
        - t57
        + t57 * t10
        + t19
        - t18 * t45;

    let t61 = e * e;
    let t63 = t60 * t61 + t8 + t57 - t4 - t7 + t56 + t36;

    let inside = t63 > 0.0;
    inside
}


// this method takes ra, dec, a collection and a radius
// and performs a cone search on the collection
// returning a vector of documents
// !not currently used
// pub async fn cone_search(
//     ra_geojson: f64,
//     dec_geojson: f64,
//     radius: &f64,
//     collection: &Collection<mongodb::bson::Document>,
// ) -> Result<Vec<mongodb::bson::Document>, Box<dyn std::error::Error>> {
//     let filter = doc! {
//         "coordinates.radec_geojson": {
//             "$geoWithin": {
//                 "$centerSphere": [[ra_geojson, dec_geojson], (radius * std::f64::consts::PI / 180.0 / 3600.0)]
//             }
//         }
//     };
//     // find all the documents that are within the radius
//     let mut cursor = collection.find(filter, None).await?;
//     let mut documents = Vec::new();
//     while let Some(doc) = cursor.next().await {
//         documents.push(doc?);
//     }
//     Ok(documents)
// }


pub async fn cone_search_named(
    collection_name: &str,
    ra_geojson: f64,
    dec_geojson: f64,
    radius: f64,
    collection: &Collection<mongodb::bson::Document>,
) -> Result<(String, Vec<mongodb::bson::Document>), Box<dyn std::error::Error>> {
    let filter = doc! {
        "coordinates.radec_geojson": {
            "$geoWithin": {
                "$centerSphere": [[ra_geojson, dec_geojson], (radius * std::f64::consts::PI / 180.0 / 3600.0)]
            }
        }
    };
    // find all the documents that are within the radius
    let mut cursor = collection.find(filter, None).await?;
    let mut documents = Vec::new();
    while let Some(doc) = cursor.next().await {
        documents.push(doc?);
    }
    Ok((collection_name.to_string(), documents))
}


// this method takes ra, dec, ra_geojson, a collection, and a collection's crossmatch config and performs 
// a cross match on the collection  based on the crossmatch config
// TODO: (https://github.com/Theodlz/boom/pull/11#discussion_r1679983922)
/*
pub async fn crossmatch(
    ra: f64,
    ra_geojson: f64,
    dec: f64,
    collection: &Collection<mongodb::bson::Document>,
    crossmatch_config: &structs::CrossmatchConfig,
) -> Result<Vec<mongodb::bson::Document>, Box<dyn std::error::Error>> {
    let documents = cone_search(ra_geojson, dec, &crossmatch_config.radius, collection).await?;
    if crossmatch_config.use_distance {
        let distance_key = crossmatch_config.distance_key.clone();
        let distance_unit = crossmatch_config.distance_unit.clone();
        let max_distance = crossmatch_config.distance_max;
        let max_distance_near = crossmatch_config.distance_max_near;
        let mut new_documents: Vec<mongodb::bson::Document> = Vec::new();
        for doc in documents {
            // be careful if ra and dec are not present or invalid
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
                let doc_z_option = match doc.get(&distance_key) {
                    Some(z) => z.as_f64(),
                    _ => {
                        continue;
                    }
                };
                // check if it's not none instead of just unwrapping
                if doc_z_option.is_none() {
                    continue;
                }
                let doc_z = doc_z_option.unwrap();

                let cm_radius = if doc_z < 0.01 {
                    max_distance_near / 3600.0 // to degrees
                } else {
                    max_distance * (0.05 / doc_z) / 3600.0 // to degrees
                };
                if in_ellipse(ra, dec, doc_ra, doc_dec, cm_radius, 1.0, 0.0) {
                    // calculate the angular separation
                    let angular_separation =
                        great_circle_distance(ra, dec, doc_ra, doc_dec) * 3600.0;
                    // calculate the distance between objs in kpc
                    //let distance_kpc = angular_separation * (doc_z / 0.05);
                    let distance_kpc = if doc_z > 0.005 {
                        angular_separation * (doc_z / 0.05)
                    } else {
                        -1.0
                    };
                    let mut doc_copy = doc.clone();
                    // overwrite doc_copy with doc_copy + the angular separation and the distance in kpc
                    doc_copy.insert("angular_separation", angular_separation);
                    doc_copy.insert("distance_kpc", distance_kpc);
                    new_documents.push(doc_copy);
                }
                // } else {
                //     println!("Not in ellipse with cm_radius {}", cm_radius);
                // }
            } else if distance_unit == "Mpc" {
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
                        continue;
                    }
                };
                if doc_mpc_option.is_none() {
                    // also print the distance key we are using
                    println!("Mpc is none using {}", distance_key);
                    // print the document _id
                    println!("{:?}", doc.get("_id"));
                    continue;
                }
                let doc_mpc = doc_mpc_option.unwrap();
                let cm_radius = if doc_mpc < 40.0 {
                    max_distance_near / 3600.0 // to degrees
                } else {
                    (max_distance / (doc_mpc * 1000.0)) // 10**3
                        .atan()
                        .to_degrees()
                };
                if in_ellipse(ra, dec, doc_ra, doc_dec, cm_radius, 1.0, 0.0) {
                    // here we don't * 3600.0 yet because we need to calculate the distance in kpc first
                    let angular_separation = great_circle_distance(ra, dec, doc_ra, doc_dec);
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
                    new_documents.push(doc_copy);
                }
                // } else {
                //     println!("Not in ellipse with cm_radius {}", cm_radius);
                // }
            } else {
                // not implemented error
                unimplemented!();
            }
        }
        Ok(new_documents)
    } else {
        Ok(documents)
    }
}
*/


// TODO: rewrite/delete this method (https://github.com/Theodlz/boom/pull/11#discussion_r1679985165)
pub async fn crossmatch_parallel(
    ra: f64,
    ra_geojson: f64,
    dec: f64,
    collections: &Vec<(&str, Collection<mongodb::bson::Document>)>,
    crossmatch_configs: &Vec<(&str, structs::CrossmatchConfig)>
    ) -> Result<std::collections::HashMap<String, Vec<mongodb::bson::Document>>, Box<dyn std::error::Error>> {
    let mut query_futures = FuturesUnordered::new();

    // Start all queries concurrently
    for (collection_name, collection) in collections {
        let crossmatch_config = &crossmatch_configs.iter().find(|(name, _)| name == collection_name).unwrap().1;
        let query_future = cone_search_named(collection_name, ra_geojson, dec, crossmatch_config.radius.clone(), collection);
        query_futures.push(query_future);
    }

    let mut results = std::collections::HashMap::new();
    while let Some(result) = query_futures.next().await {
        match result {
            Ok((collection_name, documents)) => {
                // if the collection uses distance, use it
                let crossmatch_config = &crossmatch_configs.iter().find(|(name, _)| name == &collection_name).unwrap().1;
                if crossmatch_config.use_distance {
                    let distance_unit = &crossmatch_config.distance_unit;
                    let distance_key = &crossmatch_config.distance_key;
                    let max_distance = crossmatch_config.distance_max;
                    let max_distance_near = crossmatch_config.distance_max_near;
                    let mut new_documents: Vec<mongodb::bson::Document> = Vec::new();
                    for doc in documents {
                        // be careful if ra and dec are not present or invalid
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
                            let doc_z_option = match doc.get(&distance_key) {
                                Some(z) => z.as_f64(),
                                _ => {
                                    continue;
                                }
                            };
                            // check if it's not none instead of just unwrapping
                            if doc_z_option.is_none() {
                                continue;
                            }
                            let doc_z = doc_z_option.unwrap();

                            let cm_radius = if doc_z < 0.01 {
                                max_distance_near / 3600.0 // to degrees
                            } else {
                                max_distance * (0.05 / doc_z) / 3600.0 // to degrees
                            };
                            if in_ellipse(ra, dec, doc_ra, doc_dec, cm_radius, 1.0, 0.0) {
                                // calculate the angular separation
                                let angular_separation =
                                    great_circle_distance(ra, dec, doc_ra, doc_dec) * 3600.0;
                                // calculate the distance between objs in kpc
                                //let distance_kpc = angular_separation * (doc_z / 0.05);
                                let distance_kpc = if doc_z > 0.005 {
                                    angular_separation * (doc_z / 0.05)
                                } else {
                                    -1.0
                                };
                                let mut doc_copy = doc.clone();
                                // overwrite doc_copy with doc_copy + the angular separation and the distance in kpc
                                doc_copy.insert("angular_separation", angular_separation);
                                doc_copy.insert("distance_kpc", distance_kpc);
                                new_documents.push(doc_copy);
                            }
                            // } else {
                            //     println!("Not in ellipse with cm_radius {}", cm_radius);
                            // }
                        } else if distance_unit == "Mpc" {
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
                                    continue;
                                }
                            };
                            if doc_mpc_option.is_none() {
                                // also print the distance key we are using
                                println!("Mpc is none using {}", distance_key);
                                // print the document _id
                                println!("{:?}", doc.get("_id"));
                                continue;
                            }
                            let doc_mpc = doc_mpc_option.unwrap();
                            let cm_radius = if doc_mpc < 40.0 {
                                max_distance_near / 3600.0 // to degrees
                            } else {
                                (max_distance / (doc_mpc * 1000.0)) // 10**3
                                    .atan()
                                    .to_degrees()
                            };
                            if in_ellipse(ra, dec, doc_ra, doc_dec, cm_radius, 1.0, 0.0) {
                                // here we don't * 3600.0 yet because we need to calculate the distance in kpc first
                                let angular_separation = great_circle_distance(ra, dec, doc_ra, doc_dec);
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
                                new_documents.push(doc_copy);
                            }
                            // } else {
                            //     println!("Not in ellipse with cm_radius {}", cm_radius);
                            // }
                        } else {
                            // not implemented error
                            unimplemented!();
                        }
                    }
                    results.insert(collection_name, new_documents);
                } else {
                    results.insert(collection_name, documents);
                }
            }
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(e);
            }
        }
    }
    Ok(results)
}
