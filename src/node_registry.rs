use std::collections::HashSet;
use std::path::Path;

/// Registry of Windows agent node IDs, loaded from `raws/node_id_win.md`.
/// Lookup is case-insensitive. Any node NOT in this set is treated as Linux.
pub struct NodeRegistry {
    windows_nodes: HashSet<String>,
}

impl NodeRegistry {
    /// Load from the markdown table in `node_id_win.md`.
    /// Lines that don't start with `|` or don't have at least two `|`-separated columns are skipped.
    pub fn load(path: &Path) -> std::io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let windows_nodes = parse_markdown_table(&content);
        Ok(Self { windows_nodes })
    }

    /// Build from a pre-parsed set of lowercase node IDs (used in tests).
    pub fn from_set(ids: HashSet<String>) -> Self {
        Self { windows_nodes: ids.into_iter().map(|s| s.to_ascii_lowercase()).collect() }
    }

    /// Returns `"windows"` if the node ID is in the Windows registry, `"linux"` otherwise.
    /// `None` NODEID → `"linux"` (safe default).
    pub fn agent_os(&self, nodeid: Option<&str>) -> &'static str {
        match nodeid {
            Some(id) if self.windows_nodes.contains(&id.to_ascii_lowercase()) => "windows",
            _ => "linux",
        }
    }
}

impl Default for NodeRegistry {
    /// Empty registry — every node defaults to Linux. Used as fallback when the file cannot be loaded.
    fn default() -> Self {
        Self { windows_nodes: HashSet::new() }
    }
}

fn parse_markdown_table(content: &str) -> HashSet<String> {
    let mut nodes = HashSet::new();
    for line in content.lines() {
        let line = line.trim();
        if !line.starts_with('|') {
            continue;
        }
        // Split on '|', skip the leading empty segment
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        // cols[0] = "" (before first |), cols[1] = node_id, cols[2] = OS
        if cols.len() < 3 {
            continue;
        }
        let node_id = cols[1];
        let os = cols[2];
        // Skip the header row and separator row
        if node_id.is_empty() || node_id == "Node ID" || node_id.starts_with('-') {
            continue;
        }
        if os.eq_ignore_ascii_case("Windows") {
            nodes.insert(node_id.to_ascii_lowercase());
        }
    }
    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_windows_nodes_from_table() {
        let md = "| Node ID | OS      |\n\
                  |---|------|\n\
                  | maple | Windows |\n\
                  | agent01 | Linux |\n\
                  | DRAGON | Windows |\n";
        let nodes = parse_markdown_table(md);
        assert!(nodes.contains("maple"));
        assert!(nodes.contains("dragon"));
        assert!(!nodes.contains("agent01"));
    }

    #[test]
    fn lookup_is_case_insensitive() {
        let mut set = HashSet::new();
        set.insert("maple".to_string());
        let reg = NodeRegistry::from_set(set);
        assert_eq!(reg.agent_os(Some("MAPLE")), "windows");
        assert_eq!(reg.agent_os(Some("Maple")), "windows");
        assert_eq!(reg.agent_os(Some("agent01")), "linux");
        assert_eq!(reg.agent_os(None), "linux");
    }
}
