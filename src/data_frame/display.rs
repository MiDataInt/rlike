/* -----------------------------------------------------------------------------
DataFrame Display implementation
----------------------------------------------------------------------------- */

// dependencies
use std::fmt::{Display, Formatter};
use super::DataFrame;

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {

        // Calculate column widths based on column names and data, build header type info
        let mut widths: Vec<usize>  = Vec::new();
        let mut labels: Vec<String> = Vec::new();
        for j in 0..self.n_col {
            let col_name = &self.col_names[j];
            let label = format!(
                "{} <{}>", 
                &self.col_names[j], 
                &self.col_types[col_name].replace("alloc::string::", "")
            );
            labels.push(label.clone());
            let mut width = label.len();
            for i in 0..self.n_row.min(self.print_max_rows) {
                let val_as_str= self.cell_string(&self.col_names[j], i);
                width = width.max(val_as_str.len());
            }
            widths.push(width.min(self.print_max_col_width));
        }
        
        // Write header including DataFrame dimensions, column names, and separator
        writeln!(f, "\nDataFrame: {} rows × {} columns", self.n_row, self.n_col)?;
        for (label, width) in labels.iter().zip(&widths) {
            if label.len() > *width {
                write!(f, "{:.width$}… ", &label, width = width-1)?;
            } else {
                write!(f, "{:width$} ", label, width = width)?;
            }
        }
        writeln!(f)?;
        for width in &widths {
            write!(f, "{:-<width$} ", "", width = width)?;
        }
        writeln!(f)?;
        
        // Write data rows
        let n_show = self.n_row.min(self.print_max_rows);
        for i in 0..n_show {
            for (col_name, width) in self.col_names.iter().zip(&widths) {
                let val_as_str= self.cell_string(col_name, i);
                if val_as_str.len() > *width {
                    write!(f, "{:.width$}… ", &val_as_str, width = width-1)?;
                } else {
                    write!(f, "{:width$} ", val_as_str, width = width)?;
                }
            }
            writeln!(f)?;
        }
        
        // Show ellipsis if more rows exist
        if self.n_row > self.print_max_rows { writeln!(f, "...")?; }
        Ok(())
    }
}
