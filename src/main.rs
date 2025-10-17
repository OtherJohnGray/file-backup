use clap::Parser;
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
        match backup_dataset(dataset_config) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error: {}", e);
                eprintln!("Skipping dataset '{}'\n", dataset_config.name);
            }
        }
    }
    
    println!("Done!");
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


fn backup_dataset(dataset_config: &DatasetConfig) -> Result<(), String> {
        println!("=== Dataset: {} ===", dataset_config.name);
        
        // Check if target directory exists
        if let Err(e) = check_target_directory(&dataset_config.target_dir) { return Err(e); }
        
        // Check if dataset is mounted
        match is_dataset_mounted(&dataset_config.name) {
            Ok(true) => println!("Dataset '{}' is mounted", dataset_config.name),
            Ok(false) => { return Err(format!("Error: Dataset '{}' is NOT mounted", dataset_config.name))}
            Err(e) => { return Err(e)}
        }
        
        // Get the latest snapshot
        match get_latest_snapshot(&dataset_config.name) {
            Ok(Some(snapshot)) => println!("Latest snapshot: {}", snapshot),
            Ok(None) => println!("No snapshots found for dataset '{}'", dataset_config.name),
            Err(e) => { return Err(e) }
        }
        
        println!("Target directory: {}", dataset_config.target_dir.display());
        println!(); // Blank line between datasets
        Ok(())

}