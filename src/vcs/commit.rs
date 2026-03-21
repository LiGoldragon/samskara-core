use crate::vcs::{SNAPSHOT_INTERVAL, Error, WorldVcs,
                  datavalue_to_cozo_literal};
use crate::vcs::delta::RowDelta;
use crate::vcs::snapshot::{Snapshot, SortedRows};

/// Input for a world commit operation.
pub struct CommitInput<'a> {
    pub message: &'a str,
    pub agent_id: &'a str,
    pub session_id: &'a str,
    pub now: &'a str,
}

/// Result of a successful world commit.
pub struct CommitResult {
    pub world_hash: String,
    pub parent_id: String,
    pub manifest: Vec<(String, usize, String)>,
    pub snapshot_taken: bool,
    pub delta_count: usize,
}

impl WorldVcs<'_> {
    /// Perform a full world commit (saṅkalpa): materialize pending schema,
    /// promote becoming→manifest, hash state, record commit + manifest,
    /// optionally snapshot, compute deltas.
    pub fn commit(&self, input: CommitInput) -> Result<CommitResult, Error> {
        self.materialize_pending_schema()?;
        self.promote_becoming_to_manifest()?;

        let versioned = self.versioned_relations()?;
        let (manifest_entries, relation_data, world_hash) = self.hash_world_state(&versioned)?;

        let parent_id = self.find_parent_commit();

        let (parent_delta_depth, parent_nearest_snapshot) = if parent_id.is_empty() {
            (0u32, String::new())
        } else {
            self.snapshot_index(&parent_id)
        };

        let is_genesis = parent_id.is_empty();
        let take_snapshot = is_genesis || (parent_delta_depth + 1 >= SNAPSHOT_INTERVAL);
        let new_delta_depth = if take_snapshot { 0 } else { parent_delta_depth + 1 };
        let nearest_snapshot_id = if take_snapshot {
            world_hash.clone()
        } else {
            parent_nearest_snapshot
        };

        self.store_commit(&world_hash, &parent_id, &input)?;
        self.store_manifest(&world_hash, &manifest_entries)?;
        self.store_snapshot_index(&world_hash, take_snapshot, &nearest_snapshot_id, new_delta_depth)?;

        if take_snapshot {
            self.store_snapshots(&world_hash, &relation_data)?;
        }

        let mut delta_count = 0;
        if !take_snapshot && !parent_id.is_empty() {
            delta_count = self.store_deltas(
                &world_hash, &parent_id, &nearest_snapshot_id,
                &manifest_entries, &relation_data,
            )?;
        }

        self.upsert_head(&world_hash)?;

        Ok(CommitResult {
            world_hash,
            parent_id,
            manifest: manifest_entries,
            snapshot_taken: take_snapshot,
            delta_count,
        })
    }

    /// Execute :create for any becoming-phase world_schema entries (where dignity != delusion).
    fn materialize_pending_schema(&self) -> Result<(), Error> {
        let result = self.db.run_script(
            r#"?[relation_name, create_script] := *world_schema{relation_name, create_script, phase: "becoming", dignity},
               dignity != "delusion""#
        ).map_err(|e| Error::Db { detail: format!("query pending schema: {e}") })?;

        if let Some(rows) = result.get("rows").and_then(|v| v.as_array()) {
            for row in rows {
                if let Some(arr) = row.as_array() {
                    let name = arr.first()
                        .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()))
                        .unwrap_or("");
                    let script = arr.get(1)
                        .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()))
                        .unwrap_or("");
                    if !script.is_empty() {
                        tracing::info!("materializing schema: {name}");
                        self.db.run_script(script).map_err(|e| Error::Db {
                            detail: format!("materialize schema {name}: {e}\nScript: {script}")
                        })?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Promote all becoming-phase rows to manifest across phase-aware relations.
    fn promote_becoming_to_manifest(&self) -> Result<(), Error> {
        let all_phase_relations = {
            let versioned = self.versioned_relations()?;
            let mut rels: Vec<String> = versioned.into_iter()
                .filter(|r| self.has_phase_column(r))
                .collect();
            rels.push("world_schema".to_string());
            rels
        };

        for rel in &all_phase_relations {
            let (col_names, key_count) = self.columns(rel)?;
            let col_list = col_names.join(", ");

            let query = format!(
                "?[{col_list}] := *{rel}{{{col_list}}}, phase == \"becoming\""
            );
            let luna_rows = self.db.run_script(&query)?;

            let rows = luna_rows
                .get("rows")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            if rows.is_empty() {
                continue;
            }

            let phase_idx = col_names.iter().position(|c| c == "phase").unwrap();
            let kv_clause = Self::kv_clause(&col_names, key_count);

            for row in &rows {
                if let Some(arr) = row.as_array() {
                    let vals: Vec<String> = arr
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            if i == phase_idx {
                                "\"manifest\"".to_string()
                            } else {
                                datavalue_to_cozo_literal(v)
                            }
                        })
                        .collect();
                    let val_list = vals.join(", ");

                    let put = format!(
                        "?[{col_list}] <- [[{val_list}]] :put {rel} {kv_clause}"
                    );
                    self.db.run_script(&put).map_err(|e| {
                        Error::Db { detail: format!("promote {rel} failed: {e}\nScript: {put}") }
                    })?;
                }
            }
        }
        Ok(())
    }

    /// Query and hash all versioned relations. Returns (manifest, relation_data, world_hash).
    fn hash_world_state(&self, versioned: &[String]) -> Result<(
        Vec<(String, usize, String)>,
        Vec<(String, serde_json::Value, usize)>,
        String,
    ), Error> {
        let mut manifest = Vec::new();
        let mut data = Vec::new();
        let mut hasher = blake3::Hasher::new();

        for rel_name in versioned {
            let (col_names, key_count) = self.columns(rel_name)?;
            let col_list = col_names.join(", ");

            let query = if self.has_phase_column(rel_name) {
                format!("?[{col_list}] := *{rel_name}{{{col_list}}}, phase == \"manifest\"")
            } else {
                format!("?[{col_list}] := *{rel_name}{{{col_list}}}")
            };

            let rows = self.db.run_script(&query).map_err(|e| {
                Error::Db { detail: format!("query {rel_name} failed: {e}") }
            })?;
            let sorted = SortedRows::from_query(&rows, key_count);

            let rows_str = serde_json::to_string(&sorted).unwrap_or_default();
            let row_count = sorted
                .get("rows")
                .and_then(|r| r.as_array())
                .map(|a| a.len())
                .unwrap_or(0);

            let rel_hash = blake3::hash(rows_str.as_bytes());
            hasher.update(rel_name.as_bytes());
            hasher.update(rel_hash.as_bytes());

            data.push((rel_name.to_string(), sorted, key_count));
            manifest.push((rel_name.to_string(), row_count, rel_hash.to_hex().to_string()));
        }

        Ok((manifest, data, hasher.finalize().to_hex().to_string()))
    }

    fn find_parent_commit(&self) -> String {
        self.db.run_script("?[id, ts] := *world_commit{id, ts} :order -ts :limit 1")
            .ok()
            .and_then(|v| {
                v.get("rows")?.as_array()?.first()?.as_array()?.first()
                    .and_then(|id| id.get("Str").and_then(|s| s.as_str()).or(id.as_str()))
                    .map(String::from)
            })
            .unwrap_or_default()
    }

    fn snapshot_index(&self, commit_id: &str) -> (u32, String) {
        self.db.run_script(&format!(
            "?[delta_depth, nearest_snapshot_id] := \
             *world_snapshot_index{{commit_id: \"{commit_id}\", delta_depth, nearest_snapshot_id}}"
        ))
        .ok()
        .and_then(|v| {
            let row = v.get("rows")?.as_array()?.first()?.as_array()?;
            let depth = row.first()
                .and_then(|v| v.get("Num").and_then(|n| n.get("Int")).and_then(|i| i.as_i64()).or(v.as_i64()))
                .unwrap_or(0) as u32;
            let nearest = row.get(1)
                .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()))
                .unwrap_or("")
                .to_string();
            Some((depth, nearest))
        })
        .unwrap_or((0, String::new()))
    }

    fn store_commit(&self, hash: &str, parent: &str, input: &CommitInput) -> Result<(), Error> {
        let script = format!(
            r#"?[id, parent_id, agent_id, session_id, message, ts, manifest_hash] <- [[
                "{}", "{}", "{}", "{}", "{}", "{}", "{}"
            ]]
            :put world_commit {{ id => parent_id, agent_id, session_id, message, ts, manifest_hash }}"#,
            Self::esc(hash), Self::esc(parent), Self::esc(input.agent_id),
            Self::esc(input.session_id), Self::esc(input.message), Self::esc(input.now), Self::esc(hash),
        );
        self.db.run_script(&script).map_err(|e| Error::Db { detail: format!("store commit: {e}") })?;
        Ok(())
    }

    fn store_manifest(&self, hash: &str, entries: &[(String, usize, String)]) -> Result<(), Error> {
        for (rel, count, content_hash) in entries {
            let script = format!(
                r#"?[commit_id, relation_name, row_count, content_hash] <- [[
                    "{}", "{}", {}, "{}"
                ]]
                :put world_manifest {{ commit_id, relation_name => row_count, content_hash }}"#,
                Self::esc(hash), rel, count, content_hash,
            );
            self.db.run_script(&script).map_err(|e| Error::Db { detail: format!("store manifest: {e}") })?;
        }
        Ok(())
    }

    fn store_snapshot_index(&self, hash: &str, exists: bool, nearest: &str, depth: u32) -> Result<(), Error> {
        let script = format!(
            r#"?[commit_id, snapshot_exists, nearest_snapshot_id, delta_depth] <- [[
                "{}", {}, "{}", {}
            ]]
            :put world_snapshot_index {{ commit_id => snapshot_exists, nearest_snapshot_id, delta_depth }}"#,
            Self::esc(hash), exists, Self::esc(nearest), depth,
        );
        self.db.run_script(&script).map_err(|e| Error::Db { detail: format!("store snapshot_index: {e}") })?;
        Ok(())
    }

    fn store_snapshots(&self, hash: &str, data: &[(String, serde_json::Value, usize)]) -> Result<(), Error> {
        for (rel, rows, _) in data {
            let snap = Snapshot::from_rows(rows)?;
            let script = format!(
                r#"?[commit_id, relation_name, data, reader_version, byte_count] <- [[
                    "{}", "{}", "{}", "json-zstd-b64-v1", {}
                ]]
                :put world_snapshot {{ commit_id, relation_name => data, reader_version, byte_count }}"#,
                Self::esc(hash), rel, Self::esc(&snap.encoded), snap.byte_count,
            );
            self.db.run_script(&script)?;
        }
        Ok(())
    }

    fn store_deltas(
        &self,
        hash: &str,
        parent_id: &str,
        nearest_snapshot_id: &str,
        manifest: &[(String, usize, String)],
        data: &[(String, serde_json::Value, usize)],
    ) -> Result<usize, Error> {
        let mut seq = 0;
        let mut count = 0;

        for (rel, new_rows, key_count) in data {
            let prev_hash = self.manifest_hash(parent_id, rel);
            let cur_hash = manifest.iter()
                .find(|(n, _, _)| n == rel)
                .map(|(_, _, h)| h.as_str())
                .unwrap_or("");

            if prev_hash == cur_hash {
                continue;
            }

            let prev_rows = self.load_snapshot(nearest_snapshot_id, rel)?;
            let deltas = RowDelta::from_diff(rel, *key_count, &prev_rows, new_rows)?;

            for d in &deltas {
                let key_b64 = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD, d.row_key.as_bytes(),
                );
                let data_b64 = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD, d.row_data.as_bytes(),
                );
                let script = format!(
                    "?[commit_id, seq, relation_name, operation, row_key, row_data] <- \
                     [[\"{}\", {}, \"{}\", \"{}\", \"{}\", \"{}\"]] \
                     :put world_delta {{commit_id, seq => relation_name, operation, row_key, row_data}}",
                    Self::esc(hash), seq, Self::esc(&d.relation_name),
                    Self::esc(&d.operation), key_b64, data_b64,
                );
                self.db.run_script(&script).map_err(|e| Error::Db { detail: format!("store delta: {e}") })?;
                seq += 1;
            }
            count += deltas.len();
        }
        Ok(count)
    }

    fn manifest_hash(&self, commit_id: &str, relation_name: &str) -> String {
        self.db.run_script(&format!(
            "?[content_hash] := *world_manifest{{commit_id: \"{commit_id}\", \
             relation_name: \"{relation_name}\", content_hash}}"
        ))
        .ok()
        .and_then(|v| {
            v.get("rows")?.as_array()?.first()?.as_array()?.first()
                .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()))
                .map(String::from)
        })
        .unwrap_or_default()
    }

    fn load_snapshot(&self, commit_id: &str, relation: &str) -> Result<serde_json::Value, Error> {
        let result = self.db.run_script(&format!(
            "?[data] := *world_snapshot{{commit_id: \"{commit_id}\", \
             relation_name: \"{relation}\", data}}"
        ))?;

        match result
            .get("rows")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|row| row.as_array())
            .and_then(|r| r.first())
            .and_then(|v| v.get("Str").and_then(|s| s.as_str()).or(v.as_str()))
        {
            Some(encoded) => Snapshot::to_rows(encoded),
            None => Ok(serde_json::json!({"headers": [], "rows": []})),
        }
    }

    pub fn upsert_head(&self, hash: &str) -> Result<(), Error> {
        let script = format!(
            r#"?[commit_id, ref_type, ref_value] <- [["{}", "HEAD", "{}"]]
            :put world_commit_ref {{ commit_id, ref_type => ref_value }}"#,
            Self::esc(hash), Self::esc(hash),
        );
        self.db.run_script(&script)?;
        Ok(())
    }
}
