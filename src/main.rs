use arrow::array::BooleanArray;
use std::path::PathBuf;

fn main() {
    let filters = read_all_filters("filters/q30");
}

pub fn read_all_filters(dir: impl Into<PathBuf>) -> Vec<QueryFilters> {
    // reads in filters from files in a directory for a given query
    // for each file in the directory, read the contents and parse them into a Vec<BooleanArray>
    let dir = dir.into();
    dir.read_dir().unwrap()
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

impl QueryFilters {
    /// reads all filter results from a file (a serialized arrow stream)
    pub fn try_new(file_name: impl Into<PathBuf>) -> Result<Self, String> {
        let file_name = file_name.into();
        let filters = vec![];
            println!("Reading file: {:?}", file_name);
        Ok(QueryFilters { file_name, filters })
    }
}
