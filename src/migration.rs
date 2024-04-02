use crate::database::Database;
use crate::error::Result;

type Version = i32;

pub struct Migrations<'db> {
    db: &'db Database,
    version: Version,
}

impl<'db> Migrations<'db> {
    #[inline]
    #[must_use]
    pub fn new(db: &'db Database) -> Self {
        let version = db.schema_version().unwrap_or(0);
        Self { db, version }
    }

    #[inline]
    pub fn run_one(&mut self) -> Option<Result<Version>> {
        debug_assert!(self.version >= 0);
        debug_assert!(self.version <= crate::schema_version!());
        debug_assert_eq!(self.version, self.db.schema_version().unwrap_or(0));

        if self.version == crate::schema_version!() {
            return None;
        }

        let conn = self.db.conn.lock().unwrap();

        macro_rules! migrate {
            ($version:literal) => {
                log::info!(concat!("Migrating to version ", $version, " ..."));
                if let Err(e) =
                    conn.execute_batch(include_str!(concat!("migrations/", $version, ".sql")))
                {
                    log::error!(
                        concat!("Failed to migrate to version ", $version, ": {}"),
                        e
                    );
                    return Some(Err(e.into()));
                }
                log::info!(concat!("Migrated to version ", $version));
                self.version = $version;
                return Some(Ok($version));
            };
        }

        match self.version {
            0 => {
                migrate!(1);
            }
            _ => {}
        }

        None
    }

    #[inline]
    pub fn run_all(self) -> Result<()> {
        for result in self {
            result?;
        }
        Ok(())
    }
}

impl Iterator for Migrations<'_> {
    type Item = Result<Version>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.run_one()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = crate::schema_version!() as usize - self.version as usize;
        (len, Some(len))
    }
}

impl std::iter::ExactSizeIterator for Migrations<'_> {}
impl std::iter::FusedIterator for Migrations<'_> {}

pub fn migrate(db: &Database) -> Migrations<'_> {
    Migrations::new(db)
}
