use clap::Parser;
use std::process::Command;

#[derive(Parser, Debug)]
#[command(name = "file-backup")]
#[command(about = "Backup ZFS filesystems, ZVOLs, and Restic repositories", long_about = None)]
struct Args {
    /// ZFS dataset name (e.g., tank/data)
    dataset: String,
}

fn main() {
    let args = Args::parse();
    
    match is_dataset_mounted(&args.dataset) {
        Ok(true) => println!("Dataset '{}' is mounted", args.dataset),
        Ok(false) => println!("Dataset '{}' is NOT mounted", args.dataset),
        Err(e) => eprintln!("Error checking dataset '{}': {}", args.dataset, e),
    }
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