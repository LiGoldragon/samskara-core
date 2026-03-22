use std::sync::Arc;

use rmcp::schemars;
use criome_cozo::CriomeDb;

use crate::vcs::WorldVcs;

// ── Param types (reusable by any agent) ──────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct QueryParams {
    /// CozoScript to execute against the world database
    pub script: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DescribeRelationParams {
    /// Name of the relation to describe
    pub name: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CommitWorldParams {
    /// Commit message describing what changed
    pub message: String,
    /// Agent ID recording the commit
    pub agent_id: String,
    /// Optional session ID
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct RestoreWorldParams {
    /// Commit ID to restore the world state to
    pub commit_id: String,
}

// ── Param types (read-only tools) ────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct QueryRulesParams {
    /// Filter by microtheory (e.g. "rust", "cozo", "nix")
    #[serde(default)]
    pub microtheory: Option<String>,
    /// Filter by rule type (e.g. "pattern", "constraint", "convention")
    #[serde(default)]
    pub rule_type: Option<String>,
    /// Filter by scope (e.g. "global", repo name)
    #[serde(default)]
    pub scope: Option<String>,
}

// ── Standalone tool implementations ──────────────────────────────
// Agents delegate to these from their own #[tool_router] impls.

pub async fn query(db: Arc<CriomeDb>, script: String) -> String {
    let result = tokio::task::spawn_blocking(move || {
        db.run_script_cozo(&script)
            .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}

pub async fn list_relations(db: Arc<CriomeDb>) -> String {
    let result = tokio::task::spawn_blocking(move || {
        db.run_script_cozo("::relations")
            .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}

pub async fn describe_relation(db: Arc<CriomeDb>, name: String) -> String {
    let result = tokio::task::spawn_blocking(move || {
        let script = format!("::columns {name}");
        db.run_script_cozo(&script)
            .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}

pub async fn commit_world(db: Arc<CriomeDb>, params: CommitWorldParams) -> String {
    let now = chrono::Utc::now().to_rfc3339();
    let result = tokio::task::spawn_blocking(move || {
        let session_id = params.session_id.unwrap_or_default();
        let vcs = WorldVcs::new(&db);
        let commit_result = vcs.commit(crate::vcs::commit::CommitInput {
            message: &params.message,
            agent_id: &params.agent_id,
            session_id: &session_id,
            now: &now,
        }).map_err(|e| e.to_string())?;

        let summary: Vec<String> = commit_result
            .manifest
            .iter()
            .map(|(name, count, hash)| {
                format!("  {name}: {count} rows ({hash:.12}…)")
            })
            .collect();

        let snap_info = if commit_result.snapshot_taken {
            " [snapshot taken]"
        } else {
            ""
        };

        Ok::<String, String>(format!(
            "World committed: {}{snap_info}\nParent: {}\nDeltas: {}\nManifest:\n{}",
            commit_result.world_hash,
            if commit_result.parent_id.is_empty() {
                "(genesis)"
            } else {
                &commit_result.parent_id
            },
            commit_result.delta_count,
            summary.join("\n")
        ))
    })
    .await;

    match result {
        Ok(Ok(msg)) => msg,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}

pub async fn restore_world(db: Arc<CriomeDb>, commit_id: String) -> String {
    let result = tokio::task::spawn_blocking(move || {
        let vcs = WorldVcs::new(&db);
        let restore_result = vcs.restore(&commit_id)
            .map_err(|e| e.to_string())?;

        Ok::<String, String>(format!(
            "Restored to commit: {}\nRelations restored: {}",
            restore_result.commit_id, restore_result.relations_restored,
        ))
    })
    .await;

    match result {
        Ok(Ok(msg)) => msg,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}

// ── Immutable (read-only) tool implementations ──────────────────

pub async fn query_immutable(db: Arc<CriomeDb>, script: String) -> String {
    let result = tokio::task::spawn_blocking(move || {
        db.run_script_cozo_immutable(&script)
            .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}

pub async fn list_relations_immutable(db: Arc<CriomeDb>) -> String {
    let result = tokio::task::spawn_blocking(move || {
        db.run_script_cozo_immutable("::relations")
            .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}

pub async fn describe_relation_immutable(db: Arc<CriomeDb>, name: String) -> String {
    let result = tokio::task::spawn_blocking(move || {
        let script = format!("::columns {name}");
        db.run_script_cozo_immutable(&script)
            .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}

pub async fn query_rules(db: Arc<CriomeDb>, params: QueryRulesParams) -> String {
    let result = tokio::task::spawn_blocking(move || {
        let mut conditions = Vec::new();

        if let Some(ref mt) = params.microtheory {
            conditions.push(format!("microtheory = \"{}\"", mt.replace('"', "\\\"")));
        }
        if let Some(ref rt) = params.rule_type {
            conditions.push(format!("rule_type = \"{}\"", rt.replace('"', "\\\"")));
        }
        if let Some(ref scope) = params.scope {
            conditions.push(format!("scope = \"{}\"", scope.replace('"', "\\\"")));
        }

        let filter = if conditions.is_empty() {
            String::new()
        } else {
            format!(", {}", conditions.join(", "))
        };

        let script = format!(
            "?[id, compact, rationale, microtheory, rule_type, scope] := \
             *rule{{id, compact, rationale, microtheory, rule_type, scope}}{filter}"
        );

        db.run_script_cozo_immutable(&script)
            .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) => format!("error: task join failed: {e}"),
    }
}
