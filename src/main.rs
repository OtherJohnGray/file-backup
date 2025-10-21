use clap::Parser;
use rusqlite::{Connection, Result as SqliteResult};
use serde::Deserialize;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, exit};

#[derive(Parser, Debug)]
#[command(name = "file-backup")]
#[command(about = "Backup ZFS filesystems, ZVOLs, and Restic repositories", long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "/etc/file-backup/backup-config.toml")]
    config: PathBuf,    
    
    /// Path to database file
    #[arg(short, long, default_value = "/var/lib/file-backup/backup.db")]
    database: PathBuf,
}


#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default)]
    dataset: Vec<DatasetConfig>,
    #[serde(default)]
    restic: Vec<ResticConfig>,
}


#[derive(Debug, Deserialize)]
struct DatasetConfig {
    name: String,
    target_dir: PathBuf,
}


#[derive(Debug, Deserialize)]
struct ResticConfig {
    repository: String,
    target_dir: PathBuf,
}


fn main() {
    let args = Args::parse();

    // Check if rsync is installed
    if let Err(e) = check_rsync_installed() {
        eprintln!("Error: {}", e);
        exit(1);
    }    

    // Check if restic is installed
    if let Err(e) = check_restic_installed() {
        eprintln!("Error: {}", e);
        exit(1);
    }    

    // Initialize database
    let conn = match init_database(&args.database) {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("Error initializing database '{}': {}", args.database.display(), e);
            exit(1);
        }
    };


    // Load configuration
    let config = match load_config(&args.config) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error loading config file '{}': {}", args.config.display(), e);
            exit(1);
        }
    };   

    println!("Processing {} dataset{} and {} restic repositor{}...\n", 
        config.dataset.len(), 
        if config.dataset.len() == 1 { "" } else { "s" },
        config.restic.len(),
        if config.restic.len() == 1 { "y" } else { "ies" }
    );
            
    // Process each dataset
    for dataset_config in &config.dataset {
        match backup_dataset(dataset_config, &conn) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error: {}", e);
                eprintln!("Skipping dataset '{}'\n", dataset_config.name);
            }
        }
    }

    // Process each restic repository
    for restic_config in &config.restic {
        match backup_restic(restic_config, &conn) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error: {}", e);
                eprintln!("Skipping restic repository '{}'\n", restic_config.repository);
            }
        }
    }
    
    println!("Done!");
}


fn init_database(db_path: &PathBuf) -> Result<Connection, String> {
    // Create parent directory if it doesn't exist
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create database directory: {}", e))?;
    }
    
    let conn = Connection::open(db_path)
        .map_err(|e| format!("Failed to open database: {}", e))?;
    
    // Create the backup_history table if it doesn't exist
    conn.execute(
        "CREATE TABLE IF NOT EXISTS backup_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backup_type TEXT NOT NULL,
            source_name TEXT NOT NULL,
            snapshot_name TEXT NOT NULL,
            backup_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            target_dir TEXT NOT NULL,
            UNIQUE(backup_type, source_name, snapshot_name)
        )",
        [],
    ).map_err(|e| format!("Failed to create table: {}", e))?;
    
    // Create index for faster lookups
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_source_lookup 
         ON backup_history(backup_type, source_name)",
        [],
    ).map_err(|e| format!("Failed to create index: {}", e))?;
    
    Ok(conn)
}


fn get_last_backed_up_snapshot(
    conn: &Connection, 
    backup_type: &str, 
    source_name: &str
) -> SqliteResult<Option<String>> {
    let mut stmt = conn.prepare(
        "SELECT snapshot_name, backup_timestamp 
         FROM backup_history 
         WHERE backup_type = ?1 AND source_name = ?2 
         ORDER BY backup_timestamp DESC"
    )?;
    
    let mut rows = stmt.query([backup_type, source_name])?;
    
    // Walk through backup history until we find a snapshot that still exists
    while let Some(row) = rows.next()? {
        let snapshot_name: String = row.get(0)?;
        let timestamp: String = row.get(1)?;
        
        // Check if this snapshot still exists
        match snapshot_exists(&snapshot_name, backup_type, source_name) {
            Ok(true) => {
                println!("Last successful backup: {} (at {})", snapshot_name, timestamp);
                return Ok(Some(snapshot_name));
            }
            Ok(false) => {
                println!("Snapshot {} no longer exists, checking older backups...", snapshot_name);
                continue;
            }
            Err(e) => {
                eprintln!("Warning: Failed to check if snapshot exists: {}", e);
                continue;
            }
        }
    }
    
    println!("No previous backup found with existing snapshot");
    Ok(None)
}

fn snapshot_exists(snapshot: &str, backup_type: &str, source_name: &str) -> Result<bool, String> {
    match backup_type {
        "dataset" => {
            let output = Command::new("zfs")
                .args(["list", "-H", "-t", "snapshot", snapshot])
                .output()
                .map_err(|e| format!("Failed to execute zfs command: {}", e))?;
            
            Ok(output.status.success())
        }
        "restic" => {
            // For restic, source_name is the repository path
            let output = Command::new("restic")
                .args(["-r", source_name, "snapshots", snapshot, "--json"])
                .output()
                .map_err(|e| format!("Failed to execute restic command: {}", e))?;
            
            
            if !output.status.success() {
                return Ok(false);
            }
            
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stdout_trimmed = stdout.trim();
            
            // Check if the JSON array is empty or the result is "[]"
            // An existing snapshot returns a non-empty array
            if stdout_trimmed.is_empty() || stdout_trimmed == "[]" {
                return Ok(false);
            }
            
            // Also check for the "Ignoring" warning message (though it's on stderr)
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("Ignoring") || stderr.contains("no matching ID found") {
                return Ok(false);
            }
            
            Ok(true)
        }
        _ => Err(format!("Unknown backup type: {}", backup_type))
    }
}


fn check_rsync_installed() -> Result<(), String> {
    match Command::new("rsync")
        .arg("--version")
        .output()
    {
        Ok(output) if output.status.success() => Ok(()),
        Ok(_) => Err("rsync command failed".to_string()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err("rsync is not installed. Please install rsync and try again.".to_string())
        }
        Err(e) => Err(format!("Failed to check for rsync: {}", e)),
    }
}


fn check_restic_installed() -> Result<(), String> {
    match Command::new("restic")
        .arg("version")
        .output()
    {
        Ok(output) if output.status.success() => Ok(()),
        Ok(_) => Err("restic command failed".to_string()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err("restic is not installed. Please install restic and try again.".to_string())
        }
        Err(e) => Err(format!("Failed to check for restic: {}", e)),
    }
}


fn load_config(path: &PathBuf) -> Result<Config, String> {
    let contents = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read file: {}", e))?;
    
    let config: Config = toml::from_str(&contents)
        .map_err(|e| format!("Failed to parse TOML: {}", e))?;
    
    if config.dataset.is_empty() && config.restic.is_empty() {
        return Err("No datasets or restic repositories defined in config file".to_string());
    }
    
    Ok(config)
}

fn is_dataset_mounted(dataset: &str) -> Result<bool, String> {
    // Run `zfs get -H mounted <dataset>`
    let output = Command::new("zfs")
        .args(["get", "-H", "mounted", dataset])
        .output()
        .map_err(|e| format!("Failed to execute zfs command: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("zfs command failed: {}", stderr.trim()));
    }
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    
    // Parse the output: format is "dataset\tmounted\tyes|no\tsource"
    let is_mounted = stdout
        .split('\t')
        .nth(2)
        .map(|s| s.trim() == "yes")
        .unwrap_or(false);
    
    Ok(is_mounted)
}


fn check_target_directory(target_dir: &PathBuf) -> Result<(), String> {
    if !target_dir.exists() {
        return Err(format!(
            "Target directory '{}' does not exist. Is the removable device mounted?",
            target_dir.display()
        ));
    }
    
    if !target_dir.is_dir() {
        return Err(format!(
            "'{}' exists but is not a directory",
            target_dir.display()
        ));
    }
    
    Ok(())
}


fn get_latest_snapshot(dataset: &str) -> Result<Option<String>, String> {
    // Run `zfs list -t snapshot -o name -s creation -H -r <dataset>`
    // -t snapshot: only snapshots
    // -o name: only output the name
    // -s creation: sort by creation time
    // -H: no headers (scriptable)
    let output = Command::new("zfs")
        .args(["list", "-t", "snapshot", "-o", "name", "-s", "creation", "-H", dataset])
        .output()
        .map_err(|e| format!("Failed to execute zfs command: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("zfs command failed: {}", stderr.trim()));
    }
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    
    // Get the last line (most recent due to sort order)
    let latest = stdout
        .lines()
        .filter(|line| !line.is_empty())
        .last()
        .map(|s| s.to_string());
    
    Ok(latest)
}


fn backup_dataset(dataset_config: &DatasetConfig, conn: &Connection) -> Result<(), String> {
    println!("=== Dataset: {} ===", dataset_config.name);
    
    // Check if target directory exists
    if let Err(e) = check_target_directory(&dataset_config.target_dir) { return Err(e); }
    
    // Check if dataset is mounted
    match is_dataset_mounted(&dataset_config.name) {
        Ok(true) => println!("Dataset '{}' is mounted", dataset_config.name),
        Ok(false) => { return Err(format!("Dataset '{}' is NOT mounted", dataset_config.name))}
        Err(e) => { return Err(e)}
    }
    
    // Check database for last successful backup
    let last_backup = match get_last_backed_up_snapshot(conn, "dataset", &dataset_config.name) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            eprintln!("Warning: Failed to query database: {}", e);
            None
        }
    };
    
    // Get the latest snapshot
    let latest_snapshot = match get_latest_snapshot(&dataset_config.name) {
        Ok(Some(snapshot)) => {
            println!("Latest snapshot: {}", snapshot);
            snapshot
        }
        Ok(None) => {return Err(format!("No snapshots found for dataset '{}'", dataset_config.name))}
        Err(e) => {return Err(e)}
    };
    
    println!("Target directory: {}", dataset_config.target_dir.display());

   // Determine if we need to backup
    match last_backup {
        None => {
            // No previous backup - do a full rsync
            println!("No previous backup found - performing full backup");
            
            // Get the mountpoint of the latest snapshot
            let snapshot_mountpoint = get_snapshot_mountpoint(&latest_snapshot)?;
            
            // Ensure snapshot mountpoint ends with / for rsync
            let source_path = format!("{}/", snapshot_mountpoint);
            
            // Run rsync
            run_rsync(&source_path, &dataset_config.target_dir)?;
            
            // Record successful backup
            record_successful_backup(
                conn,
                "dataset",
                &dataset_config.name,
                &latest_snapshot,
                &dataset_config.target_dir.to_string_lossy(),
            )?;
            
            println!("Backup recorded successfully");
        }
        Some(last_snap) => {
            if last_snap == latest_snapshot {
                println!("Already backed up - nothing to do");
            } else {
                println!("Incremental backup needed (last: {}, current: {})", last_snap, latest_snapshot);
                
                // Get the diff between snapshots
                let changes = get_snapshot_diff(&last_snap, &latest_snapshot)?;
                
                if changes.is_empty() {
                    println!("No changes detected between snapshots");
                } else {
                    println!("Found {} change(s):", changes.len());
                    for change in &changes {
                        println!("  {}", change);
                    }
                    
                    // Extract files that need to be synced
                    let dataset_mountpoint = get_dataset_mountpoint(&dataset_config.name)?;
                    let files_to_sync = extract_files_for_sync(&changes, &dataset_mountpoint);
                    
                    // Extract files that need to be deleted
                    let files_to_delete = extract_files_for_deletion(&changes, &dataset_mountpoint);
                    
                    // Delete removed files first
                    if !files_to_delete.is_empty() {
                        delete_files_from_target(&dataset_config.target_dir, &files_to_delete)?;
                    }
                    
                    // Then sync changed/new files
                    if !files_to_sync.is_empty() {
                        let snapshot_mountpoint = get_snapshot_mountpoint(&latest_snapshot)?;
                        let source_path = format!("{}/", snapshot_mountpoint);
                        
                        run_rsync_with_file_list(&source_path, &dataset_config.target_dir, &files_to_sync)?;
                    }                        

                    println!("Incremental backup recorded successfully");
                }
            }
        }
    }
     
    println!(); // Blank line between datasets
    Ok(())
}


fn record_successful_backup(
    conn: &Connection,
    backup_type: &str,
    source_name: &str,
    snapshot_name: &str,
    target_dir: &str,
) -> Result<(), String> {
    conn.execute(
        "INSERT INTO backup_history (backup_type, source_name, snapshot_name, target_dir)
         VALUES (?1, ?2, ?3, ?4)",
        [backup_type, source_name, snapshot_name, target_dir],
    )
    .map_err(|e| format!("Failed to record backup in database: {}", e))?;
    
    Ok(())
}


fn run_rsync(source_path: &str, target_dir: &PathBuf) -> Result<(), String> {
    println!("Starting rsync backup...");
    println!("Source: {}", source_path);
    println!("Target: {}", target_dir.display());
    
    let output = Command::new("rsync")
        .args([
            "-aAXHv",           // Archive mode with ACLs, extended attrs, hard links, verbose
            "--delete",         // Delete files in target that don't exist in source
            "--stats",          // Show transfer statistics
            source_path,
            &target_dir.to_string_lossy().to_string(),
        ])
        .output()
        .map_err(|e| format!("Failed to execute rsync: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("rsync failed: {}", stderr.trim()));
    }
    
    // Print rsync output
    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("{}", stdout);
    
    println!("Rsync completed successfully");
    Ok(())
}


fn get_snapshot_mountpoint(snapshot: &str) -> Result<String, String> {
    // ZFS snapshots are accessible under the hidden .zfs/snapshot directory
    // Parse snapshot name: pool/dataset@snapshot-name
    let parts: Vec<&str> = snapshot.split('@').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid snapshot name format: {}", snapshot));
    }
    
    let dataset = parts[0];
    let snapshot_name = parts[1];
    
    let mountpoint = match get_dataset_mountpoint(dataset) {
        Ok(mountpoint) => mountpoint,
        Err(e) => {return Err(e)}
    };
    
    // Construct the snapshot path
    let snapshot_path = format!("{}/.zfs/snapshot/{}", mountpoint, snapshot_name);
    
    Ok(snapshot_path)
}


fn strip_mountpoint_prefix(file_path: &str, mountpoint: &str) -> String {
    file_path.strip_prefix(mountpoint)
        .and_then(|s| s.strip_prefix('/'))
        .unwrap_or(file_path)
        .to_string()
}


fn get_snapshot_diff(old_snapshot: &str, new_snapshot: &str) -> Result<Vec<String>, String> {
    println!("Computing differences between snapshots...");
    
    let output = Command::new("zfs")
        .args(["diff", "-H", old_snapshot, new_snapshot])
        .output()
        .map_err(|e| format!("Failed to execute zfs diff: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("zfs diff failed: {}", stderr.trim()));
    }
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    
    // Parse the diff output
    // Format is: <change_type>\t<file_path>
    // Change types: M (modified), + (added), - (removed), R (renamed)
    let changed_files: Vec<String> = stdout
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
        .collect();
    
    Ok(changed_files)
}


fn parse_zfs_diff_line(line: &str) -> Option<(char, String)> {
    // Format: <change_type>\t<file_path>
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.len() >= 2 {
        let change_type = parts[0].chars().next()?;
        let file_path = parts[1].to_string();
        Some((change_type, file_path))
    } else {
        None
    }
}
fn extract_files_for_sync(changes: &[String], mountpoint: &str) -> Vec<String> {
    let mut files_to_sync = Vec::new();
    
    for change in changes {
        if let Some((change_type, file_path)) = parse_zfs_diff_line(change) {
            match change_type {
                '+' | 'M' => {
                    // Added or modified files need to be synced
                    let relative_path = strip_mountpoint_prefix(&file_path, mountpoint);
                    // Skip empty paths (the dataset root) and directory entries ending in /
                    if !relative_path.is_empty() && !relative_path.ends_with('/') {
                        files_to_sync.push(relative_path);
                    }                }
                'R' => {
                    // For renames, we'll sync the new name
                    if let Some(new_path) = file_path.split(" -> ").nth(1) {
                        let relative_path = strip_mountpoint_prefix(new_path, mountpoint);
                    // Skip empty paths (the dataset root) and directory entries ending in /
                    if !relative_path.is_empty() && !relative_path.ends_with('/') {
                        files_to_sync.push(relative_path);
                    }                    }
                }
                '-' => {
                    // Deletions will be handled by rsync --delete if we do a full sync
                }
                _ => {}
            }
        }
    }
    
    files_to_sync
}

fn run_rsync_with_file_list(
    source_path: &str,
    target_dir: &PathBuf,
    files: &[String],
) -> Result<(), String> {
    if files.is_empty() {
        println!("No files to sync");
        return Ok(());
    }
    
    println!("Syncing {} file(s) with rsync...", files.len());
    
    // Create a temporary file with the list of files
    let temp_file_path = "/tmp/rsync-files.txt";
    let mut temp_file = fs::File::create(temp_file_path)
        .map_err(|e| format!("Failed to create temp file: {}", e))?;
    
    // Write relative paths (without leading /)
    for file in files {
        let relative_path = file.strip_prefix('/').unwrap_or(file);
        writeln!(temp_file, "{}", relative_path)
            .map_err(|e| format!("Failed to write to temp file: {}", e))?;
    }
    
    // Flush to ensure all data is written
    temp_file.flush()
        .map_err(|e| format!("Failed to flush temp file: {}", e))?;
    
    drop(temp_file); // Close the file
    
    println!("Source: {}", source_path);
    println!("Target: {}", target_dir.display());
    
    let output = Command::new("rsync")
        .args([
            "-aAXHv",
            "--relative",           // Preserve directory structure
            "--files-from", temp_file_path,
            source_path,
            &target_dir.to_string_lossy().to_string(),
        ])
        .output()
        .map_err(|e| format!("Failed to execute rsync: {}", e))?;
    
    // Clean up temp file
    let _ = fs::remove_file(temp_file_path);
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("rsync failed: {}", stderr.trim()));
    }
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("{}", stdout);
    
    println!("Rsync completed successfully");
    Ok(())
}


fn get_dataset_mountpoint(dataset: &str) -> Result<String, String> {
    let output = Command::new("zfs")
        .args(["get", "-H", "-o", "value", "mountpoint", dataset])
        .output()
        .map_err(|e| format!("Failed to get dataset mountpoint: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("zfs command failed: {}", stderr.trim()));
    }
    
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}


fn extract_files_for_deletion(changes: &[String], mountpoint: &str) -> Vec<String> {
    let mut files_to_delete = Vec::new();
    
    for change in changes {
        if let Some((change_type, file_path)) = parse_zfs_diff_line(change) {
            if change_type == '-' {
                let relative_path = strip_mountpoint_prefix(&file_path, mountpoint);
                if !relative_path.is_empty() {
                    files_to_delete.push(relative_path);
                }
            }
        }
    }
    
    files_to_delete
}

fn delete_files_from_target(target_dir: &PathBuf, files: &[String]) -> Result<(), String> {
    if files.is_empty() {
        return Ok(());
    }
    
    println!("Deleting {} item(s) from target...", files.len());
    
    let mut deleted_count = 0;
    let mut error_count = 0;
    
    for file in files {
        let target_path = target_dir.join(file);
        
        // Check if path exists and what type it is
        let result = if target_path.is_dir() {
            println!("  Deleting directory: {}", file);
            fs::remove_dir_all(&target_path)
        } else if target_path.is_file() {
            println!("  Deleting file: {}", file);
            fs::remove_file(&target_path)
        } else {
            // Path doesn't exist
            println!("  Already gone: {}", file);
            deleted_count += 1;
            continue;
        };
        
        match result {
            Ok(()) => {
                deleted_count += 1;
            }
            Err(e) => {
                eprintln!("  Failed to delete {}: {}", file, e);
                error_count += 1;
            }
        }
    }
    
    println!("Deletion complete: {} deleted, {} errors", deleted_count, error_count);
    
    if error_count > 0 {
        Err(format!("{} item(s) failed to delete", error_count))
    } else {
        Ok(())
    }
}


fn get_latest_restic_snapshot(repository: &str) -> Result<Option<String>, String> {
    let output = Command::new("restic")
        .args(["-r", repository, "snapshots", "--json", "--last"])
        .output()
        .map_err(|e| format!("Failed to execute restic: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Empty repository returns error, but that's okay
        if stderr.contains("Is there a repository at the following location?") {
            return Ok(None);
        }
        return Err(format!("restic command failed: {}", stderr.trim()));
    }
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    if stdout.trim().is_empty() || stdout.trim() == "null" {
        return Ok(None);
    }
    
    // Parse JSON to get snapshot ID
    // Restic returns an array with one snapshot when using --last
    // Format: [{"time":"...","hostname":"...","username":"...","id":"abc123...",...}]
    // For simplicity, we'll extract the ID using basic string parsing
    if let Some(id_start) = stdout.find(r#""id":""#) {
        let id_section = &stdout[id_start + 6..];
        if let Some(id_end) = id_section.find('"') {
            let snapshot_id = &id_section[..id_end];
            return Ok(Some(snapshot_id.to_string()));
        }
    }
    
    Ok(None)
}

fn list_restic_files(repository: &str, snapshot_id: &str) -> Result<Vec<String>, String> {
    let output = Command::new("restic")
        .args(["-r", repository, "ls", snapshot_id, "--long"])
        .output()
        .map_err(|e| format!("Failed to execute restic ls: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("restic ls failed: {}", stderr.trim()));
    }
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    let files: Vec<String> = stdout
        .lines()
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            // Format: "drwxr-xr-x    0 2024-10-18 12:34:56 /path/to/file"
            // We want just the path (last field)
            line.split_whitespace().last().map(|s| s.to_string())
        })
        .collect();
    
    Ok(files)
}

fn restore_restic_files(
    repository: &str,
    snapshot_id: &str,
    target_dir: &PathBuf,
    files: &[String],
) -> Result<(), String> {
    if files.is_empty() {
        println!("No files to restore");
        return Ok(());
    }
    
    println!("Restoring {} file(s) from restic...", files.len());
    
    // Restic restore with --include for specific files
    let mut cmd = Command::new("restic");
    cmd.args(["-r", repository, "restore", snapshot_id, "--target", &target_dir.to_string_lossy()]);
    
    // Add include patterns for each file
    for file in files {
        cmd.arg("--include").arg(file);
    }
    
    let output = cmd.output()
        .map_err(|e| format!("Failed to execute restic restore: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("restic restore failed: {}", stderr.trim()));
    }
    
    println!("Restic restore completed successfully");
    Ok(())
}

fn backup_restic(restic_config: &ResticConfig, conn: &Connection) -> Result<(), String> {
    println!("=== Restic Repository: {} ===", restic_config.repository);
    
    // Check if target directory exists
    check_target_directory(&restic_config.target_dir)?;
    
    // Check database for last successful backup
    let last_backup = match get_last_backed_up_snapshot(conn, "restic", &restic_config.repository) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            eprintln!("Warning: Failed to query database: {}", e);
            None
        }
    };
    
    // Get the latest snapshot from restic repository
    let latest_snapshot = match get_latest_restic_snapshot(&restic_config.repository) {
        Ok(Some(snapshot)) => {
            println!("Latest snapshot: {}", snapshot);
            snapshot
        }
        Ok(None) => {
            return Err(format!("No snapshots found in restic repository '{}'", restic_config.repository));
        }
        Err(e) => { return Err(e) }
    };
    
    println!("Target directory: {}", restic_config.target_dir.display());
    
    // Determine if we need to backup
    match last_backup {
        None => {
            // No previous backup - restore all files
            println!("No previous backup found - performing full restore");
            
            let output = Command::new("restic")
                .args([
                    "-r", &restic_config.repository,
                    "restore", &latest_snapshot,
                    "--target", &restic_config.target_dir.to_string_lossy(),
                ])
                .output()
                .map_err(|e| format!("Failed to execute restic restore: {}", e))?;
            
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(format!("restic restore failed: {}", stderr.trim()));
            }
            
            println!("Restic restore completed successfully");
            
            // Record successful backup
            record_successful_backup(
                conn,
                "restic",
                &restic_config.repository,
                &latest_snapshot,
                &restic_config.target_dir.to_string_lossy(),
            )?;
            
            println!("Backup recorded successfully");
        }
        Some(last_snap) => {
            if last_snap == latest_snapshot {
                println!("Already backed up - nothing to do");
            } else {
                println!("Incremental backup needed (last: {}, current: {})", last_snap, latest_snapshot);
                println!("Note: Restic doesn't support diff like ZFS - performing full restore");
                println!("This will overwrite files in target directory");
                
                // For restic, we don't have a simple diff mechanism
                // We'll just do a full restore which will overwrite existing files
                let output = Command::new("restic")
                    .args([
                        "-r", &restic_config.repository,
                        "restore", &latest_snapshot,
                        "--target", &restic_config.target_dir.to_string_lossy(),
                    ])
                    .output()
                    .map_err(|e| format!("Failed to execute restic restore: {}", e))?;
                
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(format!("restic restore failed: {}", stderr.trim()));
                }
                
                println!("Restic restore completed successfully");
                
                // Record successful backup
                record_successful_backup(
                    conn,
                    "restic",
                    &restic_config.repository,
                    &latest_snapshot,
                    &restic_config.target_dir.to_string_lossy(),
                )?;
                
                println!("Backup recorded successfully");
            }
        }
    }
    
    println!(); // Blank line
    Ok(())
}