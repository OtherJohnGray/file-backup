use clap::Parser;
use rusqlite::{Connection, Result as SqliteResult};
use serde::Deserialize;
use std::fs;
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
    dataset: Vec<DatasetConfig>,
}


#[derive(Debug, Deserialize)]
struct DatasetConfig {
    name: String,
    target_dir: PathBuf,
}


fn main() {
    let args = Args::parse();

    // Check if rsync is installed
    if let Err(e) = check_rsync_installed() {
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

    println!("Processing {} dataset{}...\n", config.dataset.len(), match config.dataset.len() { 1 => {""} _ => {"s"} });     
        
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


fn get_last_backed_up_snapshot(conn: &Connection, backup_type: &str, source_name: &str) -> SqliteResult<Option<String>> {
    let mut stmt = conn.prepare(
        "SELECT snapshot_name, backup_timestamp 
         FROM backup_history 
         WHERE backup_type = ?1 AND source_name = ?2 
         ORDER BY backup_timestamp DESC 
         LIMIT 1"
    )?;
    
    let mut rows = stmt.query([backup_type, source_name])?;
    
    if let Some(row) = rows.next()? {
        let snapshot_name: String = row.get(0)?;
        let timestamp: String = row.get(1)?;
        println!("Last successful backup: {} (at {})", snapshot_name, timestamp);
        Ok(Some(snapshot_name))
    } else {
        println!("No previous backup found in database");
        Ok(None)
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


fn load_config(path: &PathBuf) -> Result<Config, String> {
    let contents = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read file: {}", e))?;
    
    let config: Config = toml::from_str(&contents)
        .map_err(|e| format!("Failed to parse TOML: {}", e))?;
    
    if config.dataset.is_empty() {
        return Err("No datasets defined in config file".to_string());
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
    match get_last_backed_up_snapshot(conn, "dataset", &dataset_config.name) {
        Ok(_) => {}, // Result is already printed in the function
        Err(e) => {
            eprintln!("Warning: Failed to query database: {}", e);
        }
    }
    
    // Get the latest snapshot
    match get_latest_snapshot(&dataset_config.name) {
        Ok(Some(snapshot)) => println!("Latest snapshot: {}", snapshot),
        Ok(None) => println!("No snapshots found for dataset '{}'", dataset_config.name),
        Err(e) => { return Err(e) }
    }
    
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
                println!("Incremental backup not yet implemented - skipping");
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
    
    // Get the mountpoint of the dataset
    let output = Command::new("zfs")
        .args(["get", "-H", "-o", "value", "mountpoint", dataset])
        .output()
        .map_err(|e| format!("Failed to get dataset mountpoint: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("zfs command failed: {}", stderr.trim()));
    }
    
    let mountpoint = String::from_utf8_lossy(&output.stdout).trim().to_string();
    
    // Construct the snapshot path
    let snapshot_path = format!("{}/.zfs/snapshot/{}", mountpoint, snapshot_name);
    
    Ok(snapshot_path)
}