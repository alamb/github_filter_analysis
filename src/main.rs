use std::fmt::{Display, Formatter};
use arrow::array::BooleanArray;
use std::path::PathBuf;

fn main() {
    let filters = read_all_filters("filters/q30");
    for filter in filters {
        println!("{}", filter);
    }
}

pub fn read_all_filters(dir: impl Into<PathBuf>) -> Vec<QueryFilters> {
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
    /// The filters themselves
    filters: Vec<BooleanArray>,
}
impl Display for QueryFilters {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let total_rows = self.filters
            .iter()
            .map(|filter| filter.len())
            .sum::<usize>();

        // Calculate total number of true values across all filters
        let total_true: usize = self.filters.iter()
            .map(|filter| filter.true_count())
            .sum();
        
        write!(f, "{} filters ({}/{}, selectivity {}) from file: {:?}",
               self.filters.len(),
               total_true, total_rows,
               f64::round((total_true as f64 / total_rows as f64) * 100.0) / 100.0,
               self.file_name)?;



        Ok(())
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
