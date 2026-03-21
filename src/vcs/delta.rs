use std::collections::BTreeMap;

use crate::vcs::Error;

/// A single row-level change between two world states.
pub struct RowDelta {
    pub relation_name: String,
    pub operation: String,
    pub row_key: String,
    pub row_data: String,
}

impl RowDelta {
    /// Compute row-level deltas between old and new states of a relation.
    ///
    /// `key_count` is the number of leading columns that form the key.
    /// Both `old` and `new` are CozoDB result values with `rows` arrays.
    /// Returns a sorted vec of deltas.
    pub fn from_diff(
        relation_name: &str,
        key_count: usize,
        old: &serde_json::Value,
        new: &serde_json::Value,
    ) -> Result<Vec<Self>, Error> {
        let old_map = rows_by_key(old, key_count);
        let new_map = rows_by_key(new, key_count);

        let mut deltas = Vec::new();

        // Inserts and updates
        for (key, new_row) in &new_map {
            match old_map.get(key) {
                None => deltas.push(Self {
                    relation_name: relation_name.to_string(),
                    operation: "insert".to_string(),
                    row_key: key.clone(),
                    row_data: new_row.clone(),
                }),
                Some(old_row) if old_row != new_row => deltas.push(Self {
                    relation_name: relation_name.to_string(),
                    operation: "update".to_string(),
                    row_key: key.clone(),
                    row_data: new_row.clone(),
                }),
                _ => {} // unchanged
            }
        }

        // Deletes
        for key in old_map.keys() {
            if !new_map.contains_key(key) {
                deltas.push(Self {
                    relation_name: relation_name.to_string(),
                    operation: "delete".to_string(),
                    row_key: key.clone(),
                    row_data: String::new(),
                });
            }
        }

        // Sort by key for determinism
        deltas.sort_by(|a, b| a.row_key.cmp(&b.row_key));
        Ok(deltas)
    }

    /// Apply deltas to a base state, returning the reconstructed state.
    pub fn apply(
        base: &serde_json::Value,
        deltas: &[Self],
        key_count: usize,
    ) -> serde_json::Value {
        let mut map = rows_by_key(base, key_count);

        for d in deltas {
            match d.operation.as_str() {
                "insert" | "update" => {
                    map.insert(d.row_key.clone(), d.row_data.clone());
                }
                "delete" => {
                    map.remove(&d.row_key);
                }
                _ => {}
            }
        }

        let headers = base
            .get("headers")
            .cloned()
            .unwrap_or(serde_json::Value::Array(vec![]));

        let rows: Vec<serde_json::Value> = map
            .values()
            .filter_map(|row_str| serde_json::from_str(row_str).ok())
            .collect();

        serde_json::json!({
            "headers": headers,
            "rows": rows
        })
    }
}

/// Build a BTreeMap from key (JSON string of key columns) to value (JSON string of full row).
fn rows_by_key(value: &serde_json::Value, key_count: usize) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Some(rows) = value.get("rows").and_then(|v| v.as_array()) {
        for row in rows {
            if let Some(arr) = row.as_array() {
                let key_vals: Vec<&serde_json::Value> = arr.iter().take(key_count).collect();
                let key = serde_json::to_string(&key_vals).unwrap_or_default();
                let full_row = serde_json::to_string(row).unwrap_or_default();
                map.insert(key, full_row);
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_insert_delete_update() {
        let old = serde_json::json!({
            "headers": ["id", "val"],
            "rows": [
                [{"Str": "a"}, {"Str": "1"}],
                [{"Str": "b"}, {"Str": "2"}],
                [{"Str": "c"}, {"Str": "3"}]
            ]
        });
        let new = serde_json::json!({
            "headers": ["id", "val"],
            "rows": [
                [{"Str": "a"}, {"Str": "1"}],
                [{"Str": "b"}, {"Str": "CHANGED"}],
                [{"Str": "d"}, {"Str": "4"}]
            ]
        });

        let deltas = RowDelta::from_diff("test_rel", 1, &old, &new).unwrap();

        let ops: Vec<(&str, &str)> = deltas
            .iter()
            .map(|d| (d.operation.as_str(), d.row_key.as_str()))
            .collect();

        assert!(ops.iter().any(|(op, _)| *op == "update"));
        assert!(ops.iter().any(|(op, _)| *op == "delete"));
        assert!(ops.iter().any(|(op, _)| *op == "insert"));
        assert_eq!(deltas.len(), 3);
    }

    #[test]
    fn apply_reconstructs() {
        let base = serde_json::json!({
            "headers": ["id", "val"],
            "rows": [
                [{"Str": "a"}, {"Str": "1"}],
                [{"Str": "b"}, {"Str": "2"}]
            ]
        });

        let deltas = vec![
            RowDelta {
                relation_name: "t".into(),
                operation: "delete".into(),
                row_key: r#"[{"Str":"b"}]"#.into(),
                row_data: String::new(),
            },
            RowDelta {
                relation_name: "t".into(),
                operation: "insert".into(),
                row_key: r#"[{"Str":"c"}]"#.into(),
                row_data: r#"[{"Str":"c"},{"Str":"3"}]"#.into(),
            },
        ];

        let result = RowDelta::apply(&base, &deltas, 1);
        let rows = result["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
    }
}
