//! The 'io' macros support reading and writing DataFrames data to and from
//! the file system or STDIN/STDOUT.

// dependencies
// use crate::data_frame::DataFrame;
// use crate::throw;

/* -----------------------------------------------------------------------------
DataFrame `read` and `write` macros for row-major IO to/from STDIN/STDOUT or text files
----------------------------------------------------------------------------- */
/// Read data into a DataFrame from a row-major file or standard input.
/// 
/// The target DataFrame can be an empty DataFrame schema or a DataFrame with 
/// existing data to which new data will be appended. In either case, the data
/// being read must conform to the schema of the target DataFrame.
/// 
/// # Examples
/// ```
/// // Establish a DataFrame schema
/// let mut df = df_new!(col1:i32, col2:f64, col3:bool);
/// 
/// // Read from a gzipped file
/// df_read!(&mut df, "data.csv.gz");
/// 
/// // Read from a regular file
/// df_read!(&mut df, "data.csv");
/// df_read!(&mut df, file = "data.csv", header = true, sep = b',', capacity = 5000);
/// 
/// // Read from stdin
/// df_read!(&mut df);
/// ```
#[macro_export]
macro_rules! df_read {
    ($df:expr, file = $path:expr, header = $header:expr, sep = $sep:expr, capacity = $capacity:expr) => {
        {
            let path_str = $path;
            let file = std::fs::File::open(path_str)
                .unwrap_or_else(|e| panic!("could not open {}: {}", path_str, e));
            if path_str.ends_with(".gz") || path_str.ends_with(".bgz") {
                let reader = std::io::BufReader::new(flate2::read::GzDecoder::new(file));
                $df.read(reader, $header, $sep, $capacity);
            } else {
                let reader = std::io::BufReader::new(file);
                $df.read(reader, $header, $sep, $capacity);
            };
        }
    };
    ($df:expr, file = $path:expr) => {
        df_read!($df, file = $path, header = false, sep = b'\t', capacity = 10000);
    };
    ($df:expr, header = $header:expr, sep = $sep:expr, capacity = $capacity:expr) => {
        {
            let reader = std::io::BufReader::new(std::io::stdin());
            $df.read(reader, $header, $sep, $capacity);
        }
    };
    ($df:expr) => {
        df_read!($df, header = false, sep = b'\t', capacity = 10000);
    };
}
/// Write data from a DataFrame into a row-major file or standard output.  
/// 
/// # Examples
/// ```
/// // Write to a file
/// df_write!(&df, "output.csv");
/// df_write!(&df, "output.csv", header = true, sep = b',');
/// 
/// // Write to stdout
/// df_write!(&df);
/// ```
#[macro_export]
macro_rules! df_write {
    ($df:expr, file = $path:expr, header = $header:expr, sep = $sep:expr) => {
        {
            let path_str = $path;
            let file = std::fs::File::create(path_str)
                .unwrap_or_else(|e| panic!("could not create {}: {}", path_str, e));
            if path_str.ends_with(".gz") {
                let writer = std::io::BufWriter::new(file);
                let mut encoder = flate2::write::GzEncoder::new(writer, flate2::Compression::default());
                $df.write(&mut encoder, $header, $sep);
                encoder.finish().unwrap();
            } else {
                let mut writer = std::io::BufWriter::new(file);
                $df.write(&mut writer, $header, $sep);
            }
        }
    };
    ($df:expr, file = $path:expr) => {
        df_write!($df, file = $path, header = false, sep = b'\t');
    };
    ($df:expr, header = $header:expr, sep = $sep:expr) => {
        {
            let mut writer = std::io::BufWriter::new(std::io::stdout());
            $df.write(&mut writer, $header, $sep);
        }
    };
    ($df:expr) => {
        df_write!($df, header = false, sep = b'\t');
    };
}

// fn xyz(){
//     let mut df = DataFrame::new();
//     df_read!(&mut df, header = true, sep = b',', capacity = 5000);
//     df_read!(&mut df, file = "data.csv", header = true, sep = b',', capacity = 5000);
//     df_write!(&df, header = true, sep = b',');
//     df_write!(&df, file = "data.csv", header = true, sep = b',');
// }

/* -----------------------------------------------------------------------------
DataFrame `load` and `save` macros for column-major IO to/from STDIN/STDOUT or binary files
----------------------------------------------------------------------------- */
/// Load data into a DataFrame from a column-major binary file or standard input.
/// 
/// # Examples
/// ```
/// // Load from a file
/// let df = df_load!("data.rlike.df");
/// 
/// // Load from stdin
/// let df = df_load!();
/// ```
#[macro_export]
macro_rules! df_load {
    ($df:expr, file = $path:expr) => {
        {
            eprintln!("pending");
        }
    };
    ($df:expr) => {
        eprintln!("pending");
    };
}
/// Save data from a DataFrame into a column-major binary file or standard output.
/// 
/// # Examples
/// ```
/// // Save to a file
/// df_save!(&df, "data.rlike.df");
/// 
/// // Write to stdout
/// df_save!(&df);
/// ```
#[macro_export]
macro_rules! df_save {
    ($df:expr, file = $path:expr) => {
        {
            eprintln!("pending");
        }
    };
    ($df:expr) => {
        eprintln!("pending");
    };
}
