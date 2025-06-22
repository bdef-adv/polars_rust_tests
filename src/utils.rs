use std::{fs::File, io::Read};

#[macro_export]
macro_rules! logger_elapsed {
    ($timer:expr, $($arg:tt)*) => {
        let millis_limit = 2;
        let duration = $timer.elapsed();
        let time_unit: char = if duration.as_millis() <= millis_limit {'µ'} else {'m'};
        let duration_displayed: u128 = if duration.as_millis() <= millis_limit {
            duration.as_micros()
        } else {
            duration.as_millis()
        };

        let text = format!($($arg)*);
        let formatted_msg = format!("{} {}{}s", text, duration_displayed, time_unit);

        println!("{formatted_msg}");
    };
}

pub fn read_config_file(filename: &str) -> serde_json::error::Result<String> {
    let mut file = File::open(filename).expect(format!("Failed to open config file {filename}").as_str());
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Failed to read config file");
    Ok(contents)
}
