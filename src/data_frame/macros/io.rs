//! The 'io' macros support reading and writing DataFrame data to and from
//! the file system or STDIN/STDOUT.

/* -----------------------------------------------------------------------------
DataFrame `read` macro to fill or extend a data frame
----------------------------------------------------------------------------- */
/// Read data into a DataFrame from a file or standard input.  
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
/// 
/// // Read from stdin
/// df_read!(&mut df);
/// ```
#[macro_export]
macro_rules! df_read {
    ($df:expr, file = $path:expr, header = $header:expr, sep = $sep:expr, capacity = $capacity:expr) => {
        {
            let file = std::fs::File::open($path).unwrap_or_else(|e| throw!("could not open {}: {}", $path, e));
            if $path.ends_with(".gz") || $path.ends_with(".bgz") {
                $df.read( 
                    std::io::BufReader::new( flate2::read::GzDecoder::new(file) ), 
                    $header, $sep, $capacity
                );
            } else {
                $df.read( 
                    std::io::BufReader::new( file ), 
                    $header, $sep, $capacity 
                );
            }
        }
    };
    ($df:expr, file = $path:expr) => {
        df_read!($df, file = $path, header = false, sep = b'\t', capacity = 10000);
    };
    ($df:expr, header = $header:expr, sep = $sep:expr, capacity = $capacity:expr) => {
        $df.read( 
            std::io::BufReader::new( std::io::stdin() ), 
            $header, $sep, $capacity 
        );
    };
    ($df:expr) => {
        df_read!($df, header = false, sep = b'\t', capacity = 10000);
    };
}
