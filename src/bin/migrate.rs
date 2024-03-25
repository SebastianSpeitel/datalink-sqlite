use datalink_sqlite::{error::Error, migration::Migrations, prelude::*};

#[derive(thiserror::Error)]
enum CliError {
    #[error("Already migrated")]
    AlreadyMigrated,
    #[error("{0}")]
    Usage(String),
    #[error("No database found at {0}")]
    NoDb(std::path::PathBuf),
    #[error("Schema version mismatch: current={0}, target={1}")]
    VersionMismatch(i32, i32),
    #[error(transparent)]
    Error(#[from] Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl std::fmt::Debug for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

fn main() -> Result<(), CliError> {
    let path = match std::env::args_os().filter(|p| p != "migrate").nth(1) {
        Some(path) => path,
        _ => {
            let arg0 = std::env::args_os().next().unwrap_or("migrate".into());
            return Err(CliError::Usage(format!(
                "Usage: {} <path-to-database>",
                arg0.to_string_lossy()
            )));
        }
    };

    let path = std::path::Path::new(&path);
    if !path.exists() {
        return Err(CliError::NoDb(path.to_path_buf()));
    }

    println!("Opening database at {}", path.to_string_lossy());
    let db = Database::open(&path)?;

    let current = db.schema_version()?;
    println!("Current schema version: {}", current);
    println!(
        "Target schema version: {}",
        datalink_sqlite::schema_version!()
    );
    if current == datalink_sqlite::schema_version!() {
        return Err(CliError::AlreadyMigrated);
    }
    println!("Migrating...");
    let migrations = Migrations::new(&db);
    for result in migrations {
        let version = result?;
        println!("Migrated to version {version}");
    }
    println!("Done");

    println!("Checking schema version...");
    let now = db.schema_version()?;
    println!("Schema version now: {}", now);
    if now != datalink_sqlite::schema_version!() {
        return Err(CliError::VersionMismatch(
            now,
            datalink_sqlite::schema_version!(),
        ));
    }

    println!("Migration successful");
    Ok(())
}
