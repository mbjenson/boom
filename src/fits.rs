use flate2::read::GzDecoder;
use std::io::{Cursor, Read};

const NAXIS1_BYTES: &[u8] = "NAXIS1  =".as_bytes();
const NAXIS2_BYTES: &[u8] = "NAXIS2  =".as_bytes();
const NAXIS1_BYTES_LEN: usize = NAXIS1_BYTES.len();
const NAXIS2_BYTES_LEN: usize = NAXIS2_BYTES.len();

fn u8_to_f32_vec(v: &[u8]) -> Vec<f32> {
    v.chunks_exact(4)
        .map(TryInto::try_into)
        .map(Result::unwrap)
        .map(f32::from_be_bytes)
        .collect()
}

pub fn buffer_to_image(buffer: Vec<u8>) -> Vec<f32> {
    let cursor = Cursor::new(buffer);

    let mut decoder = GzDecoder::new(cursor);

    let mut decompressed_data = Vec::new();
    let _ = decoder.read_to_end(&mut decompressed_data);

    let subset = &decompressed_data[0..2880];

    let mut naxis1_start = 0;
    let mut naxis1_end = 0;
    let mut digits_end = 0;
    for i in 0..subset.len() - NAXIS1_BYTES_LEN {
        if &subset[i..(i + NAXIS1_BYTES_LEN)] == NAXIS1_BYTES {
            naxis1_start = i + NAXIS1_BYTES_LEN;
            break;
        }
    }
    for i in naxis1_start..subset.len() {
        if subset[i] != b' ' {
            naxis1_end = i;
            break;
        }
    }
    for i in naxis1_end..subset.len() {
        if subset[i] == b' ' {
            digits_end = i;
            break;
        }
    }
    let naxis1 = String::from_utf8_lossy(&subset[naxis1_end..digits_end])
        .parse::<i32>()
        .unwrap();

    let mut naxis2_start = digits_end;
    let mut naxis2_end = 0;
    let mut digits2_end = 0;
    for i in naxis2_start..subset.len() - NAXIS2_BYTES_LEN {
        if &subset[i..(i + NAXIS2_BYTES_LEN)] == NAXIS2_BYTES {
            naxis2_start = i + NAXIS2_BYTES_LEN;
            break;
        }
    }
    for i in naxis2_start..subset.len() {
        if subset[i] != b' ' {
            naxis2_end = i;
            break;
        }
    }
    for i in naxis2_end..subset.len() {
        if subset[i] == b' ' {
            digits2_end = i;
            break;
        }
    }
    let naxis2 = String::from_utf8_lossy(&subset[naxis2_end..digits2_end])
        .parse::<i32>()
        .unwrap();

    let mut image_data =
        u8_to_f32_vec(&decompressed_data[2880..(2880 + (naxis1 * naxis2 * 4) as usize)]); // 32 BITPIX / 8 bits per byte = 4

    // if NAXIS1 and NAXIS2 are not 63, we need to pad the image with zeros
    // we can't just add zeros to the end of the image_data vector, because it's a 2D array we flattened into a 1D vector
    // so if NAXIS1 is not 63, we need to add 63 - NAXIS1 zeros to the start and end of each row
    // and if NAXIS2 is not 63, we need to add 63 - NAXIS2 zeros to the start and end of the vector

    if naxis1 != 63 {
        let mut new_image_data = vec![];
        for row in image_data.chunks_exact(naxis1 as usize) {
            let mut new_row = vec![0.0; 63];
            let start = (63 - naxis1) / 2;
            for i in 0..naxis1 as i32 {
                new_row[(start + i) as usize] = row[i as usize];
            }
            new_image_data.extend(new_row);
        }
        image_data = new_image_data;
    }

    if naxis2 != 63 {
        let mut new_image_data = vec![0.0; 63 * 63];
        let start = (63 - naxis2) / 2;
        for i in 0..naxis2 as i32 {
            for j in 0..63 {
                new_image_data[((start + i) * 63 + j) as usize] =
                    image_data[(i * naxis1 + j) as usize];
            }
        }
        image_data = new_image_data;
    }

    image_data
}

pub fn normalize_image(image: Vec<f32>) -> Vec<f32> {
    // first, replace all NaNs with 0s
    // and infinities with the maximum finite value
    let mut normalized = vec![];
    for pixel in image {
        if pixel.is_nan() {
            //println!("Found a NaN");
            normalized.push(0.0);
        } else if pixel.is_infinite() {
            println!("Found an infinity");
            // f32MIN if the number is negative, f32MAX if the number is positive
            normalized.push(if pixel.is_sign_negative() {
                f32::MIN
            } else {
                f32::MAX
            });
        } else {
            normalized.push(pixel);
        }
    }
    // then, compute the norm of the vector, which is the Frobenius norm of this array
    // so a 2-norm of a vector is the square root of the sum of the squares of the elements (in absolute value)
    let norm: f32 = normalized.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();

    normalized = normalized.iter().map(|x| x / norm).collect();
    normalized
}
