use boom::utils;
use config::{
    Config,
    ConfigError,
    Environment,
    File,
};

#[cfg(test)]
mod tests {

    const EPS: f64 = 0.00000005;

    use std::f32::EPSILON;

    use boom::structs::CrossmatchConfig;
    use float_cmp::approx_eq;

    use super::*;

    // TODO: fill in tests with realistic values and answers

    #[test]
    fn test_utils_great_circle_distance() {
        // let x = utils::great_circle_distance(ra1_deg, dec1_deg, ra2_deg, dec2_deg);
        // assert_eq!(x, answer);
    }

    #[test]
    fn test_utils_radec2lb() {
        // let x = utils::radec2lb(ra, dec);
        // assert_eq!(x, answer);
    }

    #[test]
    fn test_utils_deg2hms() {
        // let x = utils::deg2hms(deg);
        // assert_eq!(x, answer)
    }

    #[test]
    fn test_utils_deg2dms() {
        // let x = utils::deg2dms(deg);
        // assert_eq!(x, answer);
    }

    #[test]
    fn test_utils_in_ellipse() {
        // let x = utils::in_ellipse(alpha, delta0, alpha1, delta01, d0, axis_ratio, PAO);
        // assert_eq!(x, answer);
    }

    #[test]
    fn test_utils_cone_search_named() {
        // let x = utils::cone_search_named(collection_name, ra_geojson, dec_geojson, radius, collection);
        // assert_eq!(x, answer);
    }

    #[test]
    fn test_utils_crossmatch_parallel() {
        // let x = utils::crossmatch_parallel(ra, ra_geojson, dec, collections, crossmatch_configs);
        // assert_eq(x, answer):
    }

    #[test]
    fn test_utils_get_clu_config() {
        let conf = Config::builder()
            .add_source(File::with_name("./tests/test_config"))
            .build()
            .unwrap();
        
        let clu_config = utils::get_clu_config(conf.clone()).unwrap();
        let _clu_config = CrossmatchConfig {
            radius: 10800.0, // 3 degrees in arcseconds
            use_distance: true,
            distance_key: "z".to_string(),
            distance_max: 30.0,       // 30 Kpc
            distance_max_near: 300.0, // 5 arcsec for objects that are too close (z < 0.01)
            distance_unit: "redshift".to_string(),
        };
        assert!(approx_eq!(f64, clu_config.radius, _clu_config.radius, epsilon = EPS));
        assert_eq!(clu_config.use_distance, _clu_config.use_distance);
        assert_eq!(clu_config.distance_key, _clu_config.distance_key);
        assert_eq!(clu_config.distance_unit, _clu_config.distance_unit);
        assert!(approx_eq!(f64, clu_config.distance_max, _clu_config.distance_max, epsilon = EPS));
        assert!(approx_eq!(f64, clu_config.distance_max_near, _clu_config.distance_max_near, epsilon = EPS));
    }

    #[test]
    fn test_utils_get_ned_config() {
        let conf = Config::builder()
            .add_source(File::with_name("./tests/test_config"))
            .build()
            .unwrap();
        
        let ned_config = utils::get_ned_config(conf.clone()).unwrap();
        let _ned_config = CrossmatchConfig {
            radius: 10800.0, // 3 degrees in arcseconds
            use_distance: true,
            distance_key: "DistMpc".to_string(),
            distance_max: 30.0,       // 30 Kpc
            distance_max_near: 300.0, // 5 arcsec for objects that are too close (z < 0.01)
            distance_unit: "Mpc".to_string(),
        };
        assert!(approx_eq!(f64, ned_config.radius, _ned_config.radius, epsilon = EPS));
        assert_eq!(ned_config.use_distance, _ned_config.use_distance);
        assert_eq!(ned_config.distance_key, _ned_config.distance_key);
        assert_eq!(ned_config.distance_unit, _ned_config.distance_unit);
        assert!(approx_eq!(f64, ned_config.distance_max, _ned_config.distance_max, epsilon = EPS));
        assert!(approx_eq!(f64, ned_config.distance_max_near, _ned_config.distance_max_near, epsilon = EPS));
    }

    #[test]
    fn test_utils_get_milliquas_config() {
        let conf = Config::builder()
            .add_source(File::with_name("./tests/test_config"))
            .build()
            .unwrap();
        
        let milliquas_config = utils::get_milliquas_config(conf.clone()).unwrap();
        let _milliquas_config = CrossmatchConfig {
            radius: 2.0,
            use_distance: false,
            ..Default::default()
        };
        assert!(approx_eq!(f64, milliquas_config.radius, _milliquas_config.radius, epsilon = EPS));
        assert_eq!(milliquas_config.use_distance, _milliquas_config.use_distance);
        assert_eq!(milliquas_config.distance_key, _milliquas_config.distance_key);
        assert_eq!(milliquas_config.distance_unit, _milliquas_config.distance_unit);
        assert!(approx_eq!(f64, milliquas_config.distance_max, _milliquas_config.distance_max, epsilon = EPS));
        assert!(approx_eq!(f64, milliquas_config.distance_max_near, _milliquas_config.distance_max_near, epsilon = EPS));
    }

}
