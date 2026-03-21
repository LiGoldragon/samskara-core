use base64::{Engine, engine::general_purpose::STANDARD};

use crate::vcs::Error;

/// A compressed, base64-encoded snapshot of relation rows.
pub struct Snapshot {
    pub encoded: String,
    pub byte_count: usize,
}

impl Snapshot {
    /// Serialize CozoDB query result rows into a compressed snapshot.
    ///
    /// Pipeline: JSON bytes → zstd compress (level 3) → base64 encode.
    pub fn from_rows(rows: &serde_json::Value) -> Result<Self, Error> {
        let json_bytes = serde_json::to_vec(rows)
            .map_err(|e| Error::Serialization { detail: format!("json encode: {e}") })?;

        let compressed = zstd::encode_all(&json_bytes[..], 3)
            .map_err(|e| Error::Serialization { detail: format!("zstd compress: {e}") })?;

        let byte_count = compressed.len();
        let encoded = STANDARD.encode(&compressed);

        Ok(Self { encoded, byte_count })
    }

    /// Deserialize a snapshot back to JSON rows.
    ///
    /// Pipeline: base64 decode → zstd decompress → JSON parse.
    pub fn to_rows(encoded: &str) -> Result<serde_json::Value, Error> {
        let compressed = STANDARD
            .decode(encoded)
            .map_err(|e| Error::Deserialization { detail: format!("base64 decode: {e}") })?;

        let json_bytes = zstd::decode_all(&compressed[..])
            .map_err(|e| Error::Deserialization { detail: format!("zstd decompress: {e}") })?;

        serde_json::from_slice(&json_bytes)
            .map_err(|e| Error::Deserialization { detail: format!("json decode: {e}") })
    }
}

/// A CozoDB query result with rows sorted by key columns for deterministic hashing.
pub struct SortedRows;

impl SortedRows {
    /// Sort rows by their first `key_count` columns.
    ///
    /// Input: CozoDB result `{"headers": [...], "rows": [[...], ...]}`.
    /// Returns new Value with same headers, rows sorted lexicographically by key columns.
    pub fn from_query(value: &serde_json::Value, key_count: usize) -> serde_json::Value {
        let mut result = value.clone();
        if let Some(rows) = result.get_mut("rows").and_then(|v| v.as_array_mut()) {
            rows.sort_by(|a, b| {
                let a_keys: Vec<String> = a
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .take(key_count)
                            .map(|v| serde_json::to_string(v).unwrap_or_default())
                            .collect()
                    })
                    .unwrap_or_default();
                let b_keys: Vec<String> = b
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .take(key_count)
                            .map(|v| serde_json::to_string(v).unwrap_or_default())
                            .collect()
                    })
                    .unwrap_or_default();
                a_keys.cmp(&b_keys)
            });
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let rows = serde_json::json!({
            "headers": ["id", "name"],
            "rows": [
                [{"Str": "a"}, {"Str": "Alice"}],
                [{"Str": "b"}, {"Str": "Bob"}]
            ]
        });

        let snapshot = Snapshot::from_rows(&rows).unwrap();
        assert!(snapshot.byte_count > 0);
        assert!(!snapshot.encoded.is_empty());

        let decoded = Snapshot::to_rows(&snapshot.encoded).unwrap();
        assert_eq!(rows, decoded);
    }

    #[test]
    fn deterministic() {
        let rows = serde_json::json!({
            "headers": ["x"],
            "rows": [[{"Num": {"Int": 1}}], [{"Num": {"Int": 2}}]]
        });

        let snap1 = Snapshot::from_rows(&rows).unwrap();
        let snap2 = Snapshot::from_rows(&rows).unwrap();
        assert_eq!(snap1.encoded, snap2.encoded);
    }

    #[test]
    fn sort_by_keys() {
        let rows = serde_json::json!({
            "headers": ["id", "val"],
            "rows": [
                [{"Str": "c"}, {"Str": "3"}],
                [{"Str": "a"}, {"Str": "1"}],
                [{"Str": "b"}, {"Str": "2"}]
            ]
        });

        let sorted = SortedRows::from_query(&rows, 1);
        let sorted_rows = sorted["rows"].as_array().unwrap();
        let first_key = &sorted_rows[0].as_array().unwrap()[0];
        assert_eq!(first_key, &serde_json::json!({"Str": "a"}));
    }
}
