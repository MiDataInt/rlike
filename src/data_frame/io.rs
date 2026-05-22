//! DataFrame bulk data read/write from file or STDIN.

// dependencies
use std::io::{Read, BufReader, Write, BufWriter};
use csv::{StringRecord, ReaderBuilder, WriterBuilder, Trim};
use rayon::prelude::*;
use crate::data_frame::DataFrame;
use crate::throw;

// constants
const DF_MAGIC_KEY: &[u8; 8] = b"RLIKEDF1";

impl DataFrame {

    /* -----------------------------------------------------------------------------
    Row-major (CSV) read and write
    ----------------------------------------------------------------------------- */
    /// Fill or extend a data frame from a buffered reader applied to a row-major
    /// (CSV) input file or stream.
    pub fn read<R: Read>(
        &mut self, 
        reader:   BufReader<R>, 
        header:   bool,
        sep:      u8,
        capacity: usize
    ) -> &mut Self {

        // accept multiple types of readers from df_read! macro
        let mut rdr = ReaderBuilder::new()
            .has_headers(header)
            .delimiter(sep)
            .trim(Trim::All)
            .from_reader(reader); 

        // pre-allocate a buffer of StringRecords to hold the input data
        let mut records: Vec<StringRecord> = (0..capacity).map(|_| StringRecord::new()).collect();

        // read records from the input stream and process them in buffered chunks
        let mut load_i: usize = 0;
        loop {
            match rdr.read_record(&mut records[load_i]) {
                Ok(true) => {
                    load_i += 1;
                    if load_i == capacity {
                        self.process_read_records(&records[0..load_i]);
                        load_i = 0;
                    }
                }
                Ok(false) => break, // End of file
                Err(e) => throw!("DataFrame::read error: {}", e),
            }
        }

        // finish the last buffer chunk as needed
        if load_i > 0 {
            self.process_read_records(&records[0..load_i]);
        }
        self.status.reset();
        self.row_index.reset();
        self
    }
    // Fill or extend a data frame from one buffer of StringRecord, in parallel by column.
    fn process_read_records(&mut self, records: &[StringRecord]) {
        self.columns.par_iter_mut().for_each(|(col_name, col)| {
            let j = self.col_names.iter().position(|name| name == col_name).unwrap();
            let str_refs: Vec<&str> = records.iter().map(|record| {
                match record.get(j) {
                    Some(str_ref) => str_ref,
                    _ => throw!("DataFrame::read error: column {col_name} not found in input stream.")
                }
            }).collect();
            col.deserialize(str_refs);
        });
        self.n_row += records.len();
    }

    /// Write a DataFrame to a buffered writer as a row-major (CSV) output file or stream.
    pub fn write<W: Write>(
        &self, 
        writer: BufWriter<W>, 
        header: bool,
        sep:    u8
    ) {
        let mut wtr = WriterBuilder::new()
            .has_headers(header)
            .delimiter(sep)
            .from_writer(writer);

        // write header
        if header {
            let headers: Vec<&str> = self.col_names.iter().map(|name| name.as_str()).collect();
            wtr.write_record(&headers).unwrap_or_else(|e| throw!("DataFrame::write error writing header: {}", e));
        }

        // write rows
        for i in 0..self.n_row {
            let row: Vec<String> = self.col_names.iter().map(|col_name| {
                let col = self.columns.get(col_name).unwrap();
                col.cell_string(i)
            }).collect();
            wtr.write_record(&row).unwrap_or_else(|e| throw!("DataFrame::write error writing row {}: {}", i, e));
        }
        wtr.flush().unwrap_or_else(|e| throw!("DataFrame::write error flushing output: {}", e));
    }

    /* -----------------------------------------------------------------------------
    Column-major (binary) load and save
    ----------------------------------------------------------------------------- */
    /// Load a DataFrame from a DataFrame binary file previously written by `save()`.
    /// A caller-provided magic key is used to identify the DataFrame type in the file.
    pub fn load<R: Read>(
        &mut self, 
        _reader:   BufReader<R>, 
        _magic_key: &str,
    ) -> &mut Self {
        self
    }
    /// Save a DataFrame to a column-major binary file for later loading.
    /// A caller-provided magic key is used to identify the DataFrame type in the file.
    pub fn save<W: Write>(
        &self, 
        mut writer: BufWriter<W>, 
        magic_key: &str,
    ) {
        writer.write(DF_MAGIC_KEY).unwrap_or_else(|e| throw!(
            "DataFrame::save error writing magic key: {}", e)
        );
        writer.write(magic_key.as_bytes()).unwrap_or_else(|e| throw!(
            "DataFrame::save error writing magic key: {}", e)
        );
        // number of rows as usize
        writer.write(&self.n_row.to_le_bytes()).unwrap_or_else(|e| throw!(
            "DataFrame::save error writing n_row: {}", e)
        );
        // number of columns as usize
        writer.write(&self.n_col.to_le_bytes()).unwrap_or_else(|e| throw!(
            "DataFrame::save error writing n_col: {}", e)
        );

    }

}
