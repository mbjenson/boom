use boom::utils;
use config::{
    Config,
    File,
};
mod testing_utils;
use testing_utils as tu;
use boom::structs::CrossmatchConfig;
use float_cmp::approx_eq;

#[cfg(test)]
mod test_utils {
    use super::*;

    #[test]
    fn test_utils_great_circle_distance() {
        let x = utils::great_circle_distance(10.0, 12.0, 5.2, 8.5);
        assert!(approx_eq!(f64, x, 5.878131520996996, epsilon = tu::EPS));
    }

    #[test]
    fn test_utils_radec2lb() {
        let x = utils::radec2lb(5.67, 35.2);
        let answer = (116.32324022484094, -27.302450971293737);
        assert!(approx_eq!(f64, x.0, answer.0, epsilon = tu::EPS));
        assert!(approx_eq!(f64, x.1, answer.1, epsilon = tu::EPS));
    }

    #[test]
    fn test_utils_deg2hms() {
        let x = utils::deg2hms(5.64);
        assert_eq!(x, String::from("00:22:33.6000"));
    }

    #[test]
    fn test_utils_deg2dms() {
        let x = utils::deg2dms(5.64);
        assert_eq!(x, String::from("05:38:24.000"));
    }

    #[test]
    fn test_utils_in_ellipse() {
        let is_in_ellipse = utils::in_ellipse(5.64, 4.65, 10.5, 95.5, 35.3, 1.0, 0.0);
        assert_eq!(false, is_in_ellipse);
        let is_not_in_ellipse = utils::in_ellipse(5.64, 4.65, 10.5, 10.5, 35.3, 1.0, 0.0);
        assert_eq!(true, is_not_in_ellipse);
    }

    #[test]
    fn test_utils_get_clu_config() {
        let conf = Config::builder()
            .add_source(File::with_name("./tests/test_config"))
            .build()
            .unwrap();
        
        let clu_config = utils::get_clu_config(&conf).unwrap();
        let _clu_config = CrossmatchConfig {
            radius: 10800.0, // 3 degrees in arcseconds
            use_distance: true,
            distance_key: "z".to_string(),
            distance_max: 30.0,       // 30 Kpc
            distance_max_near: 300.0, // 5 arcsec for objects that are too close (z < 0.01)
            distance_unit: "redshift".to_string(),
        };
        assert!(approx_eq!(f64, clu_config.radius, _clu_config.radius, epsilon = tu::EPS));
        assert_eq!(clu_config.use_distance, _clu_config.use_distance);
        assert_eq!(clu_config.distance_key, _clu_config.distance_key);
        assert_eq!(clu_config.distance_unit, _clu_config.distance_unit);
        assert!(approx_eq!(f64, clu_config.distance_max, _clu_config.distance_max, epsilon = tu::EPS));
        assert!(approx_eq!(f64, clu_config.distance_max_near, _clu_config.distance_max_near, epsilon = tu::EPS));
    }

    #[test]
    fn test_utils_get_ned_config() {
        let conf = Config::builder()
            .add_source(File::with_name("./tests/test_config"))
            .build()
            .unwrap();
        
        let ned_config = utils::get_ned_config(&conf).unwrap();
        let _ned_config = CrossmatchConfig {
            radius: 10800.0, // 3 degrees in arcseconds
            use_distance: true,
            distance_key: "DistMpc".to_string(),
            distance_max: 30.0,       // 30 Kpc
            distance_max_near: 300.0, // 5 arcsec for objects that are too close (z < 0.01)
            distance_unit: "Mpc".to_string(),
        };
        assert!(approx_eq!(f64, ned_config.radius, _ned_config.radius, epsilon = tu::EPS));
        assert_eq!(ned_config.use_distance, _ned_config.use_distance);
        assert_eq!(ned_config.distance_key, _ned_config.distance_key);
        assert_eq!(ned_config.distance_unit, _ned_config.distance_unit);
        assert!(approx_eq!(f64, ned_config.distance_max, _ned_config.distance_max, epsilon = tu::EPS));
        assert!(approx_eq!(f64, ned_config.distance_max_near, _ned_config.distance_max_near, epsilon = tu::EPS));
    }

    #[test]
    fn test_utils_get_milliquas_config() {
        let conf = Config::builder()
            .add_source(File::with_name("./tests/test_config"))
            .build()
            .unwrap();
        
        let milliquas_config = utils::get_milliquas_config(&conf).unwrap();
        let _milliquas_config = CrossmatchConfig {
            radius: 2.0,
            use_distance: false,
            ..Default::default()
        };
        assert!(approx_eq!(f64, milliquas_config.radius, _milliquas_config.radius, epsilon = tu::EPS));
        assert_eq!(milliquas_config.use_distance, _milliquas_config.use_distance);
        assert_eq!(milliquas_config.distance_key, _milliquas_config.distance_key);
        assert_eq!(milliquas_config.distance_unit, _milliquas_config.distance_unit);
        assert!(approx_eq!(f64, milliquas_config.distance_max, _milliquas_config.distance_max, epsilon = tu::EPS));
        assert!(approx_eq!(f64, milliquas_config.distance_max_near, _milliquas_config.distance_max_near, epsilon = tu::EPS));
    }
}
