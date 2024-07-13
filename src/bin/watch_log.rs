// Follow the log file(s) in real time
use std::io::BufRead;

fn find_last_line(filename: &str) -> usize {
    let file = std::fs::File::open(filename).unwrap();
    let reader = std::io::BufReader::new(file);
    let mut last_line = 0;
    for _ in reader.lines() {
        last_line += 1;
    }
    last_line
}

fn read_from_line(filename: &str, line: usize) -> (String, usize) {
    // be very efficient and don't read anything before the line we want
    // and be non blocking, as other processes may be writing to the file
    let file = std::fs::File::open
    (filename).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let mut content = String::new();
    let mut current_line = 0;
    loop {
        let mut line_content = String::new();
        match reader.read_line(&mut line_content) {
            Ok(0) => break,
            Ok(_) => {
                if current_line >= line {
                    content.push_str(&line_content);
                }
                current_line += 1;
            }
            Err(e) => {
                eprintln!("Error reading file: {:?}", e);
                break;
            }
        }
    }
    (content, current_line)
}

fn main() {
    // create the log directory if it does not exist
    let basedir = std::env::current_dir().unwrap();
    let logdir = basedir.join("log");
    if !logdir.exists() {
        std::fs::create_dir(&logdir).unwrap();
    }
    // get the file name as an argument
    let mut filename = std::env::args().nth(1).expect("No filename provided");
    // if there is no .log extension, add it
    if !filename.ends_with(".log") {
        filename.push_str(".log");
    }
    filename = logdir.join(filename).to_str().unwrap().to_string();

    // if the file does not exist, throw an error
    if !std::path::Path::new(&filename).exists() {
        eprintln!("File {} does not exist", filename);
        std::process::exit(1);
    }

    let mut last_line = find_last_line(&filename);
    loop {
        let (content, line) = read_from_line(&filename, last_line);
        print!("{}", content);
        last_line = line;
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}