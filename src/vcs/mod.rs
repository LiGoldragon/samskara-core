pub mod commit;
pub mod delta;
pub mod error;
pub mod restore;
pub mod snapshot;

pub use error::Error;

use criome_cozo::CriomeDb;

/// Infrastructure relations excluded from versioning.
/// These track versioned state but are not themselves versioned.
const INFRASTRUCTURE_RELATIONS: &[&str] = &[
    "meta", "world_commit", "world_commit_ref", "world_delta",
    "world_manifest", "world_schema", "world_snapshot", "world_snapshot_index",
];

/// Number of commits between full snapshots.
pub const SNAPSHOT_INTERVAL: u32 = 10;

/// The world version control system. Owns a reference to the CozoDB instance
/// and provides commit (saṅkalpa) and restore (pratiṣṭhā) operations.
pub struct WorldVcs<'a> {
    db: &'a CriomeDb,
    fallback_versioned: Option<&'a [&'a str]>,
}

impl<'a> WorldVcs<'a> {
    pub fn new(db: &'a CriomeDb) -> Self {
        Self { db, fallback_versioned: None }
    }

    /// Create a WorldVcs with a fallback list of versioned relations
    /// for when world_schema is empty (e.g. tests or first boot).
    pub fn with_fallback(db: &'a CriomeDb, fallback: &'a [&'a str]) -> Self {
        Self { db, fallback_versioned: Some(fallback) }
    }

    /// Query world_schema for all manifest-phase versioned relations.
    /// Excludes infrastructure relations. Falls back to the provided
    /// fallback list when world_schema is empty or absent.
    pub fn versioned_relations(&self) -> Result<Vec<String>, Error> {
        let result = self.db.run_script(
            r#"?[name] := *world_schema{relation_name: name, phase: "manifest"} :order name"#
        );

        let names: Vec<String> = result.ok()
            .and_then(|v| v.get("rows")?.as_array().cloned())
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        let name = row.as_array()?.first()
                            .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()))?;
                        if INFRASTRUCTURE_RELATIONS.contains(&name) {
                            None
                        } else {
                            Some(name.to_string())
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        if names.is_empty() {
            match self.fallback_versioned {
                Some(fallback) => Ok(fallback.iter().map(|s| s.to_string()).collect()),
                None => Err(Error::Db {
                    detail: "world_schema is empty and no fallback versioned list provided".into(),
                }),
            }
        } else {
            Ok(names)
        }
    }

    /// Check if a relation has a `phase` column by introspecting `::columns`.
    pub fn has_phase_column(&self, rel: &str) -> bool {
        self.columns(rel)
            .map(|(cols, _)| cols.iter().any(|c| c == "phase"))
            .unwrap_or(false)
    }

    /// Escape a string for embedding in CozoScript.
    pub fn esc(s: &str) -> String {
        s.replace('\\', "\\\\").replace('"', "\\\"")
    }

    /// Get column names and key count for a relation via `::columns`.
    pub fn columns(&self, rel_name: &str) -> Result<(Vec<String>, usize), Error> {
        let result = self.db.run_script(&format!("::columns {rel_name}"))?;
        let rows = result
            .get("rows")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Error::Db { detail: format!("no columns for {rel_name}") })?;

        let mut names = Vec::new();
        let mut key_count = 0;

        for row in rows {
            let arr = row.as_array().ok_or_else(|| Error::Db { detail: "bad column row".into() })?;
            let name = arr
                .first()
                .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()))
                .ok_or_else(|| Error::Db { detail: "missing column name".into() })?;
            let is_key = arr
                .get(1)
                .and_then(|v| v.get("Bool").and_then(|b| b.as_bool()).or(v.as_bool()))
                .unwrap_or(false);

            names.push(name.to_string());
            if is_key {
                key_count += 1;
            }
        }

        Ok((names, key_count))
    }

    /// Build the `:put`/`:rm` clause with key=>value separation.
    pub fn kv_clause(col_names: &[String], key_count: usize) -> String {
        let key_part = col_names[..key_count].join(", ");
        let val_part = col_names[key_count..].join(", ");
        if val_part.is_empty() {
            format!("{{{key_part}}}")
        } else {
            format!("{{{key_part} => {val_part}}}")
        }
    }
}

/// Convert a CozoDB DataValue JSON to a CozoScript literal.
pub fn datavalue_to_cozo_literal(v: &serde_json::Value) -> String {
    if let Some(s) = v.get("Str").and_then(|s| s.as_str()) {
        let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
        return format!("\"{escaped}\"");
    }
    if let Some(b) = v.get("Bool").and_then(|b| b.as_bool()) {
        return if b { "true".into() } else { "false".into() };
    }
    if let Some(num) = v.get("Num") {
        if let Some(i) = num.get("Int").and_then(|i| i.as_i64()) {
            return i.to_string();
        }
        if let Some(f) = num.get("Float").and_then(|f| f.as_f64()) {
            return f.to_string();
        }
    }
    if let Some(s) = v.as_str() {
        let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
        return format!("\"{escaped}\"");
    }
    if let Some(b) = v.as_bool() {
        return if b { "true".into() } else { "false".into() };
    }
    if let Some(i) = v.as_i64() {
        return i.to_string();
    }
    if let Some(f) = v.as_f64() {
        return f.to_string();
    }
    "null".into()
}
