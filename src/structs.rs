#![allow(non_snake_case)]

use serde::{Deserialize, Serialize};

use crate::utils::{deg2dms, deg2hms, radec2lb};

// schema: https://zwickytransientfacility.github.io/ztf-avro-alert/schema.html
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Union {
    Int(i64),
    String(String),
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NonDetection {
    pub jd: f64,
    pub fid: i64,
    pub pid: i64,
    pub diffmaglim: f64,
    pub pdiffimfilename: String,
    pub programpi: String,
    pub programid: i64,
    pub nid: i64,
    pub rcid: i64,
    pub field: i64,
    pub rbversion: Union,
    pub magzpsci: f64,
    pub magzpsciunc: f64,
    pub magzpscirms: f64,
    pub clrcoeff: f64,
    pub clrcounc: f64,
}
// we also need the trait to conver to BSON
#[serde_with::skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Detection {
    pub candid: Option<i64>,
    pub isdiffpos: Option<String>,
    pub tblid: Option<i64>,
    pub xpos: Option<f64>,
    pub ypos: Option<f64>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub magpsf: Option<f64>,
    pub sigmapsf: Option<f64>,
    pub chipsf: Option<f64>,
    pub magap: Option<f64>,
    pub sigmagap: Option<f64>,
    pub distnr: Option<f64>,
    pub magnr: Option<f64>,
    pub sigmagnr: Option<f64>,
    pub chinr: Option<f64>,
    pub sharpnr: Option<f64>,
    pub sky: Option<f64>,
    pub magdiff: Option<f64>,
    pub fwhm: Option<f64>,
    pub classtar: Option<f64>,
    pub mindtoedge: Option<f64>,
    pub magfromlim: Option<f64>,
    pub seeratio: Option<f64>,
    pub aimage: Option<f64>,
    pub bimage: Option<f64>,
    pub aimagerat: Option<f64>,
    pub bimagerat: Option<f64>,
    pub elong: Option<f64>,
    pub nneg: Option<i64>,
    pub nbad: Option<i64>,
    pub rb: Option<f64>,
    pub drb: Option<f64>,
    pub drbversion: Option<String>,
    pub sumrat: Option<f64>,
    pub magapbig: Option<f64>,
    pub sigmagapbig: Option<f64>,
    pub ranr: Option<f64>,
    pub decnr: Option<f64>,
    pub scorr: Option<f64>,
    pub exptime: Option<f64>,
    #[serde(flatten)]
    pub non_detection: Option<NonDetection>,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Candidate {
    #[serde(flatten)]
    pub detection: Detection,
    pub ssdistnr: f64,
    pub ssmagnr: f64,
    pub ssnamenr: String,
    pub ranr: f64,
    pub decnr: f64,
    pub ndethist: i64,
    pub ncovhist: i64,
    pub jdstarthist: f64,
    pub jdendhist: f64,
    pub tooflag: i64,
    pub objectidps1: i64,
    pub sgmag1: f64,
    pub srmag1: f64,
    pub simag1: f64,
    pub szmag1: f64,
    pub sgscore1: f64,
    pub distpsnr1: f64,
    pub objectidps2: i64,
    pub sgmag2: f64,
    pub srmag2: f64,
    pub simag2: f64,
    pub szmag2: f64,
    pub sgscore2: f64,
    pub distpsnr2: f64,
    pub objectidps3: i64,
    pub sgmag3: f64,
    pub srmag3: f64,
    pub simag3: f64,
    pub szmag3: f64,
    pub sgscore3: f64,
    pub distpsnr3: f64,
    pub nmtchps: i64,
    pub rfid: i64,
    pub jdstartref: f64,
    pub jdendref: f64,
    pub nframesref: i64,
    pub dsnrms: f64,
    pub ssnrms: f64,
    pub dsdiff: f64,
    pub nmatches: i64,
    pub zpclrcov: f64,
    pub zpmed: f64,
    pub clrmed: f64,
    pub clrrms: f64,
    pub neargaia: f64,
    pub neargaiabright: f64,
    pub maggaia: f64,
    pub maggaiabright: f64,
    pub drb: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Cutout {
    pub fileName: String,
    // stampdata is a base64 encoded image
    #[serde(with = "serde_bytes")]
    pub stampData: Vec<u8>,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Alert {
    pub schemavsn: String,
    pub publisher: String,
    pub objectId: String,
    pub candid: i64,
    pub candidate: Candidate,
    //#[serde(skip_serializing_if = "Vec::is_empty")]
    //fp_hists: Vec<FpHist>,
    pub cutoutScience: Cutout,
    pub cutoutTemplate: Cutout,
    pub cutoutDifference: Cutout,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AlertPacket {
    pub schemavsn: String,
    pub publisher: String,
    pub objectId: String,
    pub candid: i64,
    pub candidate: Candidate,
    pub cutoutScience: Cutout,
    pub cutoutTemplate: Cutout,
    pub cutoutDifference: Cutout,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub prv_candidates: Vec<Detection>,
}

impl AlertPacket {
    // this method is used to remove the prv_candidates from the alert
    // we basically recreate the alert without the prv_candidates
    // taking advantage of serde's skip_serializing_if = "Vec::is_empty"
    // to not serialize the prv_candidates if it's empty
    pub fn remove_prv_candidates(&self) -> AlertPacket {
        let mut alert = self.clone();
        alert.prv_candidates = vec![];
        alert
    }

    // we have a method that generates the AlertCoordinates from the AlertPacket
    pub fn get_coordinates(&self) -> AlertCoordinates {
        let ra = self.candidate.detection.ra.unwrap();
        let dec = self.candidate.detection.dec.unwrap();
        let lb = radec2lb(ra, dec);
        let alert_coordinates = AlertCoordinates {
            // radec_str is a tuple of strings
            // we use the deg2hms and deg2dms functions to convert the ra and dec to hms and dms
            // we then create a vector with the two strings
            radec_str: (deg2hms(ra), deg2dms(dec)),
            radec_geojson: GeoJSONCoordinates {
                r#type: "Point".to_string(),
                coordinates: (ra - 180.0, dec),
            },
            galactic: GalacticCoordinates { l: lb.0, b: lb.1 },
        };
        alert_coordinates
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Classifications {
    pub braai: f32,
    pub acai_b: f32,
    pub acai_h: f32,
    pub acai_n: f32,
    pub acai_v: f32,
    pub acai_o: f32,
    pub braai_version: String,
    pub acai_b_version: String,
    pub acai_h_version: String,
    pub acai_n_version: String,
    pub acai_v_version: String,
    pub acai_o_version: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AlertCrossmatches {
    pub milliquas_v8: Vec<mongodb::bson::Document>,
    pub clu: Vec<mongodb::bson::Document>,
    pub ned: Vec<mongodb::bson::Document>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AlertAux {
    pub _id: String,
    pub prv_candidates: Vec<Detection>,
    pub cross_matches: AlertCrossmatches,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GeoJSONCoordinates {
    pub r#type: String, // "Point"
    pub coordinates: (f64, f64),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GalacticCoordinates {
    pub l: f64,
    pub b: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AlertCoordinates {
    pub radec_str: (String, String),
    pub radec_geojson: GeoJSONCoordinates,
    pub galactic: GalacticCoordinates,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AlertWithClassificationsAndCoords {
    pub schemavsn: String,
    pub publisher: String,
    pub objectId: String,
    pub candid: i64,
    pub candidate: Candidate,
    //#[serde(skip_serializing_if = "Vec::is_empty")]
    //fp_hists: Vec<FpHist>, //TODO: implement this
    pub cutoutScience: Cutout,
    pub cutoutTemplate: Cutout,
    pub cutoutDifference: Cutout,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classifications: Option<Classifications>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinates: Option<AlertCoordinates>,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AlertWithCoords {
    pub schemavsn: String,
    pub publisher: String,
    pub objectId: String,
    pub candid: i64,
    pub candidate: Candidate,
    pub cutoutScience: Cutout,
    pub cutoutTemplate: Cutout,
    pub cutoutDifference: Cutout,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinates: Option<AlertCoordinates>,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CrossmatchConfig {
    pub radius: f64,
    pub use_distance: bool,
    // key, unit, and max are not mandatory, defaults to "z", "redshift", and "30"
    pub distance_key: String, // "z", "Mpc", ... the column in the collection that contains the distance
    pub distance_unit: String, // "redshift", "Mpc", or "kpc"
    pub distance_max: f64,    // in kpc
    pub distance_max_near: f64, // in arcsec
}

impl Default for CrossmatchConfig {
    fn default() -> Self {
        CrossmatchConfig {
            radius: 1.0,
            use_distance: false,
            distance_key: "z".to_string(),
            distance_unit: "redshift".to_string(),
            distance_max: 30.0,
            distance_max_near: 5.0,
        }
    }
}
