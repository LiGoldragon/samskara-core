use criome_cozo::CriomeDb;

/// Returns true if the statement is only comments (no executable CozoScript).
pub fn is_comment_only(stmt: &str) -> bool {
    stmt.lines()
        .all(|line| {
            let trimmed = line.trim();
            trimmed.is_empty() || trimmed.starts_with('#') || trimmed == "//"
        })
}

/// Load a CozoScript file into CozoDB, skipping comment-only blocks.
pub fn load_cozo_script(
    db: &CriomeDb,
    script: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for stmt in criome_cozo::Script::from_str(script) {
        let trimmed = stmt.trim();
        if !trimmed.is_empty() && !is_comment_only(trimmed) {
            db.run_script(trimmed)?;
        }
    }
    Ok(())
}

/// Check if the database has already been initialized by looking for the meta relation.
pub fn is_initialized(db: &CriomeDb) -> bool {
    db.run_script("::columns meta").is_ok()
}

/// Reconstruct a `:create` statement from `::columns` output for a relation.
pub fn create_script_for(
    db: &CriomeDb,
    rel: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let result = db.run_script(&format!("::columns {rel}"))?;
    let rows = result["rows"]
        .as_array()
        .ok_or("no columns rows")?;

    let mut keys = Vec::new();
    let mut vals = Vec::new();

    for row in rows {
        let arr = row.as_array().ok_or("bad column row")?;
        let name = arr[0]
            .get("Str").and_then(|s| s.as_str())
            .or_else(|| arr[0].as_str())
            .ok_or("no column name")?;
        let is_key = arr[1]
            .get("Bool").and_then(|b| b.as_bool())
            .or_else(|| arr[1].as_bool())
            .unwrap_or(false);
        let col_type = arr[3]
            .get("Str").and_then(|s| s.as_str())
            .or_else(|| arr[3].as_str())
            .ok_or("no column type")?;

        let col_def = format!("{name}: {col_type}");
        if is_key {
            keys.push(col_def);
        } else {
            vals.push(col_def);
        }
    }

    let body = if vals.is_empty() {
        keys.join(", ")
    } else {
        format!("{} => {}", keys.join(", "), vals.join(", "))
    };

    Ok(format!(":create {rel} {{ {body} }}"))
}

/// Populate world_schema by introspecting all relations in the database.
/// `eternal_relations` is the list of relations that should be marked dignity=eternal.
/// `contract_relations` is the list of relations with origin=contract.
pub fn populate_world_schema(
    db: &CriomeDb,
    eternal_relations: &[&str],
    contract_relations: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let relations = db.run_script("::relations")?;
    let rows = relations["rows"]
        .as_array()
        .ok_or("no relations rows")?;

    for row in rows {
        let name = row.as_array()
            .and_then(|a| a[0].get("Str").and_then(|s| s.as_str()).or_else(|| a[0].as_str()))
            .ok_or("no relation name")?;

        if name == "world_schema" {
            continue;
        }

        let script = create_script_for(db, name)?;
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");

        let dignity = if eternal_relations.contains(&name) {
            "eternal"
        } else {
            "proven"
        };

        let origin = if contract_relations.contains(&name) {
            "contract"
        } else {
            "genesis"
        };

        let put = format!(
            r#"?[relation_name, create_script, origin, phase, dignity] <- [[
                "{}", "{}", "{}", "manifest", "{}"
            ]]
            :put world_schema {{ relation_name => create_script, origin, phase, dignity }}"#,
            esc(name), esc(&script), origin, dignity,
        );
        db.run_script(&put)?;
    }

    tracing::info!("world_schema populated with all relation definitions");
    Ok(())
}

/// Load core infrastructure relations (Phase, Dignity, world_schema, VCS).
pub fn core_genesis(db: &CriomeDb) -> Result<(), Box<dyn std::error::Error>> {
    load_cozo_script(db, include_str!("../schema/core-world-init.cozo"))?;
    tracing::info!("core relations created");
    load_cozo_script(db, include_str!("../schema/core-world-seed.cozo"))?;
    tracing::info!("core seeds loaded");
    Ok(())
}

/// Finalize genesis: populate world_schema and create meta sentinel.
pub fn finalize_genesis(
    db: &CriomeDb,
    eternal_relations: &[&str],
    contract_relations: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    populate_world_schema(db, eternal_relations, contract_relations)?;
    db.run_script(":create meta { key: String => value: String }")?;
    db.run_script(r#"?[key, value] <- [["schema_version", "1"]] :put meta { key => value }"#)?;
    tracing::info!("genesis complete — meta sentinel written");
    Ok(())
}
