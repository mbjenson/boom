// Rotation matrix for the conversion : x_galactic = R * x_equatorial (J2000)
// http://adsabs.harvard.edu/abs/1989A&A...218..325M
const RGE: [[f64; 3]; 3] = [
    [0.0524767519, 0.9976931946, 0.0543758425],
    [0.9939326351, -0.0548018445, -0.0969076596],
    [-0.0966835603, 0.0464756311, -0.9949210238],
];

// const DEGRA: f64 = std::f64::consts::PI / 180.0;

pub fn radec2lb(ra: f64, dec: f64) -> (f64, f64) {
    let ra_rad = ra.to_radians();
    let dec_rad = dec.to_radians();
    let u = [
        ra_rad.cos() * dec_rad.cos(),
        ra_rad.sin() * dec_rad.cos(),
        dec_rad.sin(),
    ];
    let ug = [
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
