use crate::vcs::{Error, WorldVcs, datavalue_to_cozo_literal};
use crate::vcs::snapshot::Snapshot;

/// Result of a successful restore operation.
pub struct RestoreResult {
    pub commit_id: String,
    pub relations_restored: usize,
}

impl WorldVcs<'_> {
    /// Restore the world state to a specific commit (pratiṣṭhā).
    /// Loads the nearest snapshot and replaces all versioned relations.
    pub fn restore(&self, target_commit_id: &str) -> Result<RestoreResult, Error> {
        let exists = self.db.run_script(&format!(
            "?[found] := *world_commit{{id: \"{target_commit_id}\"}}, found = true"
        ))?;
        if exists.get("rows").and_then(|v| v.as_array()).map(|a| a.is_empty()).unwrap_or(true) {
            return Err(Error::NoSuchCommit { id: target_commit_id.to_string() });
        }

        let snap_result = self.db.run_script(&format!(
            "?[snapshot_exists, nearest_snapshot_id, delta_depth] := \
             *world_snapshot_index{{commit_id: \"{target_commit_id}\", \
             snapshot_exists, nearest_snapshot_id, delta_depth}}"
        ))?;
        let snap_row = snap_result
            .get("rows")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|r| r.as_array())
            .ok_or_else(|| Error::Db { detail: "no snapshot index for commit".into() })?;

        let nearest_snapshot_id = snap_row
            .get(1)
            .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()))
            .unwrap_or(target_commit_id);

        let mut restored_count = 0;
        let versioned = self.versioned_relations()?;

        for rel_name in &versioned {
            let snap_data = self.db.run_script(&format!(
                "?[data] := *world_snapshot{{commit_id: \"{nearest_snapshot_id}\", \
                 relation_name: \"{rel_name}\", data}}"
            ))?;

            let encoded = snap_data
                .get("rows")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|row| row.as_array())
                .and_then(|r| r.first())
                .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()));

            let rows = match encoded {
                Some(enc) => Snapshot::to_rows(enc)?,
                None => continue,
            };

            let (col_names, key_count) = self.columns(rel_name)?;
            let col_list = col_names.join(", ");
            let kv_clause = Self::kv_clause(&col_names, key_count);

            let rm_script = format!(
                "?[{col_list}] := *{rel_name}{{{col_list}}} :rm {rel_name} {kv_clause}"
            );
            let _ = self.db.run_script(&rm_script);

            if let Some(row_arr) = rows.get("rows").and_then(|v| v.as_array()) {
                for row in row_arr {
                    if let Some(vals) = row.as_array() {
                        let val_strs: Vec<String> = vals
                            .iter()
                            .map(|v| datavalue_to_cozo_literal(v))
                            .collect();
                        let val_list = val_strs.join(", ");
                        let script = format!(
                            "?[{col_list}] <- [[{val_list}]] :put {rel_name} {kv_clause}"
                        );
                        if let Err(e) = self.db.run_script(&script) {
                            return Err(Error::Db { detail: format!(
                                "restore put {rel_name} failed: {e}\nScript: {script}"
                            ) });
                        }
                    }
                }
            }

            restored_count += 1;
        }

        self.upsert_head(target_commit_id)?;

        Ok(RestoreResult {
            commit_id: target_commit_id.to_string(),
            relations_restored: restored_count,
        })
    }
}
