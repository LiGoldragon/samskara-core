use std::process::Command;

/// Commit types — the typed enum for commit messages.
/// Stored as strings in CozoDB but validated in Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitType {
    Draft,
    Proposal,
    Implementation,
    Testing,
    Plan,
    Review,
    Refactor,
    Fix,
    Session,
    Intent,
    Release,
    Merge,
}

impl CommitType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Proposal => "proposal",
            Self::Implementation => "implementation",
            Self::Testing => "testing",
            Self::Plan => "plan",
            Self::Review => "review",
            Self::Refactor => "refactor",
            Self::Fix => "fix",
            Self::Session => "session",
            Self::Intent => "intent",
            Self::Release => "release",
            Self::Merge => "merge",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "draft" => Some(Self::Draft),
            "proposal" => Some(Self::Proposal),
            "implementation" => Some(Self::Implementation),
            "testing" => Some(Self::Testing),
            "plan" => Some(Self::Plan),
            "review" => Some(Self::Review),
            "refactor" => Some(Self::Refactor),
            "fix" => Some(Self::Fix),
            "session" => Some(Self::Session),
            "intent" => Some(Self::Intent),
            "release" => Some(Self::Release),
            "merge" => Some(Self::Merge),
            _ => None,
        }
    }

    /// Infer commit type from a commit description.
    pub fn infer(description: &str) -> Self {
        let lower = description.trim().to_lowercase();
        if lower.starts_with("intent:") { return Self::Intent; }
        if lower.starts_with("session:") { return Self::Session; }
        if lower.starts_with("fix:") || lower.starts_with("fix(") { return Self::Fix; }
        if lower.starts_with("plan:") { return Self::Plan; }
        if lower.starts_with("review:") { return Self::Review; }
        if lower.starts_with("test:") || lower.starts_with("testing:") { return Self::Testing; }
        if lower.starts_with("refactor:") { return Self::Refactor; }
        if lower.starts_with("release:") || lower.starts_with("release ") { return Self::Release; }
        if lower.starts_with("merge") { return Self::Merge; }
        if lower.starts_with("proposal:") || lower.starts_with("propose:") { return Self::Proposal; }
        if lower.starts_with("impl:") || lower.starts_with("implement:") { return Self::Implementation; }
        Self::Draft
    }

    /// All valid commit type names.
    pub fn all() -> &'static [&'static str] {
        &[
            "draft", "proposal", "implementation", "testing",
            "plan", "review", "refactor", "fix",
            "session", "intent", "release", "merge",
        ]
    }
}

/// A parsed jj commit ready to be stored in CozoDB.
#[derive(Debug, Clone)]
pub struct JjCommit {
    pub change_id: String,
    pub commit_id: String,
    pub parent_change_id: String,
    pub author: String,
    pub ts: String,
    pub commit_type: CommitType,
    pub title: String,
    pub body: String,
}

/// A file diff from a jj change.
#[derive(Debug, Clone)]
pub struct JjDiff {
    pub change_id: String,
    pub file_path: String,
    pub diff_content: String,
}

/// Fetch recent commits from jj and parse them into JjCommit structs.
pub fn fetch_commits(repo_path: &str, limit: usize) -> Result<Vec<JjCommit>, Box<dyn std::error::Error>> {
    let template = r#"change_id ++ "|" ++ commit_id ++ "|" ++ if(parents, parents.map(|p| p.change_id()).join(","), "") ++ "|" ++ author.email() ++ "|" ++ author.timestamp() ++ "|" ++ description.first_line() ++ "|" ++ description"#;

    let output = Command::new("jj")
        .args(["log", "--no-graph", "-T", template, "--limit", &limit.to_string()])
        .current_dir(repo_path)
        .env("JJ_EDITOR", ":")
        .output()?;

    if !output.status.success() {
        return Err(format!("jj log failed: {}", String::from_utf8_lossy(&output.stderr)).into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    let mut commits = Vec::new();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }

        let parts: Vec<&str> = line.splitn(7, '|').collect();
        if parts.len() < 6 { continue; }

        let description = parts.get(5).unwrap_or(&"").to_string();
        let full_desc = parts.get(6).unwrap_or(&"").to_string();
        let commit_type = CommitType::infer(&description);

        let title = description.clone();
        let body = if full_desc.len() > title.len() {
            full_desc[title.len()..].trim().to_string()
        } else {
            String::new()
        };

        commits.push(JjCommit {
            change_id: parts[0].to_string(),
            commit_id: parts[1].to_string(),
            parent_change_id: parts.get(2).unwrap_or(&"").to_string(),
            author: parts[3].to_string(),
            ts: parts[4].to_string(),
            commit_type,
            title,
            body,
        });
    }

    Ok(commits)
}

/// Fetch the git-format diff for a specific change.
pub fn fetch_diff(repo_path: &str, change_id: &str) -> Result<Vec<JjDiff>, Box<dyn std::error::Error>> {
    let output = Command::new("jj")
        .args(["diff", "--git", "-r", change_id])
        .current_dir(repo_path)
        .env("JJ_EDITOR", ":")
        .output()?;

    if !output.status.success() {
        return Err(format!("jj diff failed: {}", String::from_utf8_lossy(&output.stderr)).into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    let mut diffs = Vec::new();
    let mut current_path = String::new();
    let mut current_diff = String::new();

    for line in stdout.lines() {
        if line.starts_with("diff --git ") {
            if !current_path.is_empty() {
                diffs.push(JjDiff {
                    change_id: change_id.to_string(),
                    file_path: current_path.clone(),
                    diff_content: current_diff.clone(),
                });
            }
            current_path = line
                .strip_prefix("diff --git a/")
                .and_then(|s| s.split(" b/").next())
                .unwrap_or("")
                .to_string();
            current_diff = line.to_string();
            current_diff.push('\n');
        } else {
            current_diff.push_str(line);
            current_diff.push('\n');
        }
    }

    if !current_path.is_empty() {
        diffs.push(JjDiff {
            change_id: change_id.to_string(),
            file_path: current_path,
            diff_content: current_diff,
        });
    }

    Ok(diffs)
}

/// Jujutsu mirror — stores jj commits and diffs into CozoDB.
pub struct JjMirror<'a> {
    db: &'a criome_cozo::CriomeDb,
}

impl<'a> JjMirror<'a> {
    pub fn new(db: &'a criome_cozo::CriomeDb) -> Self {
        Self { db }
    }

    /// Write a JjCommit into the CozoDB as a `commit` relation.
    pub fn store_commit(&self, commit: &JjCommit) -> Result<(), Box<dyn std::error::Error>> {
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");

        let script = format!(
            r#"?[change_id, commit_id, parent_change_id, author, ts, commit_type, title, body, phase, dignity] <- [[
                "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "manifest", "seen"
            ]]
            :put commit {{ change_id => commit_id, parent_change_id, author, ts, commit_type, title, body, phase, dignity }}"#,
            esc(&commit.change_id), esc(&commit.commit_id), esc(&commit.parent_change_id),
            esc(&commit.author), esc(&commit.ts), esc(commit.commit_type.as_str()),
            esc(&commit.title), esc(&commit.body)
        );
        self.db.run_script(&script)?;
        Ok(())
    }

    /// Write a JjDiff into the CozoDB as a `commit_diff` relation.
    pub fn store_diff(&self, diff: &JjDiff) -> Result<(), Box<dyn std::error::Error>> {
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        let byte_count = diff.diff_content.len();

        let script = format!(
            r#"?[change_id, file_path, diff_content, diff_bytes] <- [[
                "{}", "{}", "{}", {}
            ]]
            :put commit_diff {{ change_id, file_path => diff_content, diff_bytes }}"#,
            esc(&diff.change_id), esc(&diff.file_path), esc(&diff.diff_content), byte_count
        );
        self.db.run_script(&script)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_type_infer_intent() {
        assert_eq!(CommitType::infer("intent: add new feature"), CommitType::Intent);
    }

    #[test]
    fn commit_type_infer_session() {
        assert_eq!(CommitType::infer("session: finalize prompt work"), CommitType::Session);
    }

    #[test]
    fn commit_type_infer_fix() {
        assert_eq!(CommitType::infer("fix: resolve compile error"), CommitType::Fix);
    }

    #[test]
    fn commit_type_infer_default_draft() {
        assert_eq!(CommitType::infer("some random commit message"), CommitType::Draft);
    }

    #[test]
    fn commit_type_roundtrip() {
        for name in CommitType::all() {
            let ct = CommitType::from_str(name).expect(&format!("{name} should parse"));
            assert_eq!(ct.as_str(), *name);
        }
    }
}
