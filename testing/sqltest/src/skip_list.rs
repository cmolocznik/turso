//! A skip list: tests a run leaves out, each with its reason, kept in one file beside the corpora
//! rather than in the test files, for a storage mode whose departures are ruled in its design
//! (Atoll's, `sqlite/conformance/atoll-skips.tsv`).
//!
//! A row is `file<TAB>test<TAB>reason`: the test file's path as the run loads it, the test's name
//! and why it does not run. Blank lines and lines starting with `#` are comments. A row whose file
//! the run loaded without a test of its name is stale, and so is the run: a row outlives its test
//! only in silence otherwise.

use crate::parser::ast::Skip;
use crate::runner::LoadedTests;
use std::path::Path;

/// The rows of a skip list.
#[derive(Debug)]
pub struct SkipList {
    rows: Vec<Row>,
}

#[derive(Debug)]
struct Row {
    line: usize,
    file: String,
    test: String,
    reason: String,
}

impl SkipList {
    /// Parses a skip list, refusing a row without its three fields or with an empty one, and a
    /// test listed twice.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut rows: Vec<Row> = Vec::new();
        for (i, raw) in text.lines().enumerate() {
            let line = i + 1;
            if raw.trim().is_empty() || raw.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = raw.split('\t').collect();
            let [file, test, reason] = fields[..] else {
                return Err(format!(
                    "line {line}: {} fields, not file, test and reason",
                    fields.len()
                ));
            };
            if [file, test, reason].iter().any(|f| f.trim().is_empty()) {
                return Err(format!("line {line}: an empty field"));
            }
            if let Some(first) = rows.iter().find(|r| r.file == file && r.test == test) {
                return Err(format!(
                    "line {line}: {file} {test} is listed at line {} already",
                    first.line
                ));
            }
            rows.push(Row {
                line,
                file: file.to_string(),
                test: test.to_string(),
                reason: reason.to_string(),
            });
        }
        Ok(Self { rows })
    }

    /// Marks each listed test of `loaded` skipped, with its row's reason, and returns how many
    /// it marked. An error names every stale row: one whose file is loaded without its test.
    pub fn apply(&self, loaded: &mut LoadedTests) -> Result<usize, Vec<String>> {
        let mut marked = 0;
        let mut stale = Vec::new();
        for row in &self.rows {
            let mut file_loaded = false;
            let mut found = false;
            for (path, file) in loaded.files.iter_mut() {
                if !path.ends_with(Path::new(&row.file)) {
                    continue;
                }
                file_loaded = true;
                for test in file.tests.iter_mut().filter(|t| t.name == row.test) {
                    test.modifiers.skip.push(Skip {
                        reason: row.reason.clone(),
                        condition: None,
                    });
                    found = true;
                    marked += 1;
                }
            }
            if file_loaded && !found {
                stale.push(format!(
                    "line {}: {} has no test {}",
                    row.line, row.file, row.test
                ));
            }
        }
        if stale.is_empty() {
            Ok(marked)
        } else {
            Err(stale)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;
    use std::path::PathBuf;

    const FILE: &str = "@database :memory:

test kept {
    SELECT 1;
}
expect {
    1
}

test listed {
    SELECT 2;
}
expect {
    2
}
";

    fn loaded() -> LoadedTests {
        LoadedTests {
            files: vec![(PathBuf::from("corpus/sub/a.sqltest"), parse(FILE).unwrap())],
            errors: Vec::new(),
        }
    }

    fn skips(loaded: &LoadedTests, name: &str) -> Vec<String> {
        loaded.files[0]
            .1
            .tests
            .iter()
            .find(|t| t.name == name)
            .unwrap()
            .modifiers
            .skip
            .iter()
            .map(|s| s.reason.clone())
            .collect()
    }

    /// A listed test is skipped with its reason, matched by the file's trailing path; the rest
    /// run, and a row for a file the run did not load is no error.
    #[test]
    fn a_listed_test_is_skipped_with_its_reason() {
        let list = SkipList::parse(
            "# comment\n\nsub/a.sqltest\tlisted\truled out\nother/b.sqltest\tany\telsewhere\n",
        )
        .unwrap();
        let mut loaded = loaded();
        assert_eq!(list.apply(&mut loaded), Ok(1));
        assert_eq!(skips(&loaded, "listed"), vec!["ruled out".to_string()]);
        assert!(skips(&loaded, "kept").is_empty());
    }

    /// A row whose file is loaded without its test is stale, and the run with it.
    #[test]
    fn a_row_for_a_test_its_file_lacks_is_stale() {
        let list =
            SkipList::parse("sub/a.sqltest\tlisted\tok\nsub/a.sqltest\tgone\twas here\n").unwrap();
        let mut loaded = loaded();
        assert_eq!(
            list.apply(&mut loaded),
            Err(vec!["line 2: sub/a.sqltest has no test gone".to_string()])
        );
    }

    /// A row short of a field, with an empty one, or listing a test twice is refused.
    #[test]
    fn a_malformed_row_is_refused() {
        assert!(SkipList::parse("sub/a.sqltest\tlisted\n").is_err());
        assert!(SkipList::parse("sub/a.sqltest\tlisted\t \n").is_err());
        assert!(SkipList::parse("a\tt\tr\na\tt\tagain\n").is_err());
    }
}
