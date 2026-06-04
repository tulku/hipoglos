use anyhow::Context;
use rusqlite::{params, Connection};
use std::path::Path;

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path).context("Failed to open database")?;
        Ok(Self { conn })
    }

    pub fn initialize(&self) -> anyhow::Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS sync_state (
                    calendar_id TEXT PRIMARY KEY,
                    last_sync_time TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS event_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_calendar_id TEXT NOT NULL,
                    source_event_id TEXT NOT NULL,
                    target_calendar_id TEXT NOT NULL,
                    target_event_id TEXT NOT NULL,
                    UNIQUE(source_calendar_id, source_event_id, target_calendar_id)
                );
                CREATE INDEX IF NOT EXISTS idx_mappings_source
                    ON event_mappings(source_calendar_id, source_event_id);
                CREATE INDEX IF NOT EXISTS idx_mappings_target
                    ON event_mappings(target_calendar_id, target_event_id);
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS event_versions (
                    calendar_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    last_updated TEXT NOT NULL,
                    PRIMARY KEY (calendar_id, event_id)
                );",
            )
            .context("Failed to initialize database schema")?;
        Ok(())
    }

    pub fn get_last_sync(&self, calendar_id: &str) -> anyhow::Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT last_sync_time FROM sync_state WHERE calendar_id = ?1")?;
        match stmt.query_row(params![calendar_id], |row| row.get(0)) {
            Ok(time) => Ok(Some(time)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_last_sync(&self, calendar_id: &str, time: &str) -> anyhow::Result<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO sync_state (calendar_id, last_sync_time) VALUES (?1, ?2)",
                params![calendar_id, time],
            )
            .context("Failed to update sync state")?;
        Ok(())
    }

    pub fn save_mapping(
        &self,
        source_calendar: &str,
        source_event: &str,
        target_calendar: &str,
        target_event: &str,
    ) -> anyhow::Result<()> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO event_mappings
                 (source_calendar_id, source_event_id, target_calendar_id, target_event_id)
                 VALUES (?1, ?2, ?3, ?4)",
                params![source_calendar, source_event, target_calendar, target_event],
            )
            .context("Failed to save event mapping")?;
        Ok(())
    }

    pub fn get_mappings(
        &self,
        source_calendar: &str,
        source_event: &str,
    ) -> anyhow::Result<Vec<MirrorMapping>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_calendar_id, target_event_id FROM event_mappings
             WHERE source_calendar_id = ?1 AND source_event_id = ?2",
        )?;
        let rows = stmt.query_map(params![source_calendar, source_event], |row| {
            Ok(MirrorMapping {
                target_calendar_id: row.get(0)?,
                target_event_id: row.get(1)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row.context("Failed to read mapping row")?);
        }
        Ok(result)
    }

    pub fn delete_mappings(
        &self,
        source_calendar: &str,
        source_event: &str,
    ) -> anyhow::Result<()> {
        self.conn
            .execute(
                "DELETE FROM event_mappings
                 WHERE source_calendar_id = ?1 AND source_event_id = ?2",
                params![source_calendar, source_event],
            )
            .context("Failed to delete event mappings")?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM meta WHERE key = ?1")?;
        match stmt.query_row(params![key], |row| row.get(0)) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)",
                params![key, value],
            )
            .context("Failed to set meta")?;
        Ok(())
    }

    pub fn get_all_mappings(&self) -> anyhow::Result<Vec<FullMapping>> {
        let mut stmt = self.conn.prepare(
            "SELECT source_calendar_id, source_event_id, target_calendar_id, target_event_id
             FROM event_mappings ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(FullMapping {
                source_calendar_id: row.get(0)?,
                source_event_id: row.get(1)?,
                target_calendar_id: row.get(2)?,
                target_event_id: row.get(3)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row.context("Failed to read full mapping")?);
        }
        Ok(result)
    }

    pub fn get_event_version(
        &self,
        calendar_id: &str,
        event_id: &str,
    ) -> anyhow::Result<Option<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT last_updated FROM event_versions WHERE calendar_id = ?1 AND event_id = ?2",
        )?;
        match stmt.query_row(params![calendar_id, event_id], |row| row.get(0)) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_event_version(
        &self,
        calendar_id: &str,
        event_id: &str,
        last_updated: &str,
    ) -> anyhow::Result<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO event_versions (calendar_id, event_id, last_updated)
                 VALUES (?1, ?2, ?3)",
                params![calendar_id, event_id, last_updated],
            )
            .context("Failed to set event version")?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn delete_event_version(
        &self,
        calendar_id: &str,
        event_id: &str,
    ) -> anyhow::Result<()> {
        self.conn
            .execute(
                "DELETE FROM event_versions WHERE calendar_id = ?1 AND event_id = ?2",
                params![calendar_id, event_id],
            )
            .context("Failed to delete event version")?;
        Ok(())
    }

    pub fn clear_all_mirror_data(&self) -> anyhow::Result<()> {
        self.conn
            .execute_batch(
                "DELETE FROM event_mappings;
                 DELETE FROM event_versions;
                 DELETE FROM sync_state;",
            )
            .context("Failed to clear mirror data")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MirrorMapping {
    pub target_calendar_id: String,
    pub target_event_id: String,
}

#[derive(Debug, Clone)]
pub struct FullMapping {
    pub source_calendar_id: String,
    pub source_event_id: String,
    pub target_calendar_id: String,
    pub target_event_id: String,
}
