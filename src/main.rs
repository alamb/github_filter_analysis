use std::fmt::{Display, Formatter};
use arrow::array::BooleanArray;
use std::path::PathBuf;
use parquet::arrow::arrow_reader::RowSelection;

fn main() {
    analyze_query("q30")
}

fn analyze_query(query_name: &str ) {
    // reads all filters for a given query and prints some statistics
    // the query name is the directory where the filters are stored
    println!("Analyzing query: {}", query_name);
    let dir = PathBuf::from("filters").join(query_name);
    if !dir.exists() {
        println!("Directory {:?} does not exist", dir);
        return;
    }
    let filters = read_all_filters(&dir);
    if filters.is_empty() {
        println!("No filters found in directory {:?}", dir);
        return;
    }
    let total_rows: usize = filters.iter().map(|f| f.total_rows()).sum();
    let total_true: usize = filters.iter().map(|f| f.total_true()).sum();
    let selectivity: f64 = if total_rows > 0 {
        (total_true as f64 / total_rows as f64) * 100.0
    } else {
        0.0
    };
    println!("Total filters: {}", filters.len());
    println!("Total rows across all filters: {}", total_rows);
    println!("Total true values across all filters: {}", total_true);
    println!("Overall selectivity: {:.2}%", selectivity);

    for filter in filters {
        println!("{}", filter);
    }
}

 fn read_all_filters(dir: impl Into<PathBuf>) -> Vec<QueryFilters> {
    // reads in filters from files in a directory for a given query
    // for each file in the directory, read the contents and parse them into a Vec<BooleanArray>
    let dir = dir.into();
    dir.read_dir()
        .unwrap()
        .filter_map(|file| {
            let file = file.unwrap();
            if !file.metadata().unwrap().is_file() {
                println!("Skipping non-file entry: {:?}", file.path());
                None
            } else {
                Some(QueryFilters::try_new(file.path()).unwrap())
            }
        })
        .collect()
}

/// The data in this directory comes from running a certain clickbench query and recording
/// the results of evaluating the `ArrowPredicate` expression
struct QueryFilters {
    /// Location where the filters came form
    file_name: PathBuf,
    /// The filters themselves (result of evaluating the `ArrowPredicate` expression)
    filters: Vec<BooleanArray>,
}
impl Display for QueryFilters {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {

        writeln!(f, "{}", self.file_name())?;

        writeln!(f, "  {} filters ({}/{}, selectivity {})",
               self.len(),
                self.total_true(),
                self.total_rows(),
            self.selectivity(),
        )?;

        // compute how many selections there are with the row selection representation
        let selections = self.row_selection();
        let mut total_taken_rows = 0;
        let mut total_skipped_rows = 0;
        for s in selections.iter() {
            if s.skip {
                total_skipped_rows += s.row_count;
            } else {
                total_taken_rows += s.row_count;
            }
        };
        let avg_taken_rows = if total_taken_rows + total_skipped_rows > 0 {
            total_taken_rows as f64 / (total_taken_rows + total_skipped_rows) as f64
        } else {
            0.0
        };
        let avg_skipped_rows = if total_taken_rows + total_skipped_rows > 0 {
            total_skipped_rows as f64 / (total_taken_rows + total_skipped_rows) as f64
        } else {
            0.0
        };


        write!(f, "  RowSelection: {} selections, avg taken row count: {:.2}, avg skipped rows: {:.2}",
            selections.iter().count(),
            avg_taken_rows,
            avg_skipped_rows)?;

        Ok(())
    }
}

impl QueryFilters {

    /// return the file name where the filters were read from
    pub fn file_name(&self) -> &str {
        self.file_name.as_os_str().to_str().unwrap()
    }

    /// Returns the number of filters
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Returns the total number of rows across all filters
    pub fn total_rows(&self) -> usize {
        self.filters.iter().map(|filter| filter.len()).sum()
    }

    /// return the total number of true values across all filters
    pub fn total_true(&self) -> usize {
        self.filters.iter().map(|filter| filter.true_count()).sum()
    }

    /// Return the selectivity of the filters as a percentage
    pub fn selectivity(&self) -> f64 {
        let total_rows = self.total_rows() as f64;
        if total_rows == 0.0 {
            return 0.0; // Avoid division by zero
        }
        let total_true = self.total_true() as f64;
        (total_true / total_rows) * 100.0
    }

    /// returns the filters as a Vec of RowSelection
    pub fn row_selection(&self) -> RowSelection {
        RowSelection::from_filters(&self.filters)
    }

}


impl QueryFilters {
    /// reads all filter results from a file (a serialized arrow stream)
    pub fn try_new(file_name: impl Into<PathBuf>) -> Result<Self, String> {
        let file_name = file_name.into();
        let mut filters = vec![];

        let file = std::fs::File::open(&file_name)
            .map_err(|e| format!("Failed to open file {:?}: {}", file_name, e))?;
        let projection = None;  // read all columns
        let mut reader = arrow::ipc::reader::FileReader::try_new(file, projection)
            .map_err(|e| format!("Error opening file  {:?}: {}", file_name, e))?;
        while let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| format!("Failed to read batch from file {:?}: {}", file_name, e))?;
            assert_eq!(batch.num_columns(), 1, "Expected exactly one column in the batch from file {:?}", file_name);
            let column = batch.column(0);
            let Some(boolean_array) = column.as_any().downcast_ref::<BooleanArray>() else {
                    return Err(format!(
                        "Expected BooleanArray but found {:?} in file {:?}",
                        column.data_type(),
                        file_name
                    ));
                };
            filters.push(boolean_array.clone());
        }

        Ok(QueryFilters { file_name, filters })
    }
}
