use std::error::Error;
use boom::{
    structs,
    utils,
};
use apache_avro::from_value;
mod testing_utils;
use testing_utils as tu;


mod test_structs {
    use super::*;

    // test get coordinates
    #[test]
    fn test_get_coordinates() -> Result<(), Box<dyn Error>> {
        // create queue with one record inside from file
        let mut queue = tu::build_alert_queue(
            String::from("./data/sample_alerts"), 1).unwrap();
        // get record from top of queue
        let record = queue.pop().unwrap();
        // get alert packet from record
        let alert: structs::AlertPacket = from_value(&record).unwrap();
        // get coords using function
        let coords = alert.get_coordinates();
        
        let ra = alert.candidate.detection.ra.unwrap();
        let dec = alert.candidate.detection.dec.unwrap();
        let lb = utils::radec2lb(ra, dec);
        let _coords = structs::AlertCoordinates {
            radec_str: (utils::deg2hms(ra), utils::deg2dms(dec)),
            radec_geojson: structs::GeoJSONCoordinates {
                r#type: "Point".to_string(),
                coordinates: (ra - 180.0, dec),
            },
            galactic: structs::GalacticCoordinates { l: lb.0, b: lb.1 },
        };
        tu::are_coordinates_eq(coords, _coords);
        Ok(())
    }
}