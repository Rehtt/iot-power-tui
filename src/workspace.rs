use anyhow::{Context, Result};
use rusqlite::OptionalExtension;
use std::path::{Path, PathBuf};

/// Deliberately has no automatic deletion: crashes/errors preserve staged data.
pub struct CaptureWorkspace {
    pub target: PathBuf,
    pub directory: PathBuf,
    pub database: PathBuf,
    committed: bool,
}
impl CaptureWorkspace {
    pub fn suggested_name(&self) -> Result<String> {
        let conn = rusqlite::Connection::open(&self.database)?;
        let row: Option<(String, String)> = conn.query_row(
            "SELECT started_at,coalesce(ended_at,started_at) FROM sessions ORDER BY id DESC LIMIT 1",
            [], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
        Ok(row
            .map(|(a, b)| format!("{a} - {b}"))
            .unwrap_or_else(|| "未命名会话".into()))
    }
    pub fn new(target: &str) -> Result<Self> {
        let target = std::path::absolute(target).context("resolve save destination")?;
        let parent = target.parent().unwrap_or(Path::new("."));
        let pending = parent.join(".iot-power-pending");
        std::fs::create_dir_all(&pending).context("create pending capture directory")?;
        let directory = tempfile::Builder::new()
            .prefix("capture-")
            .tempdir_in(pending)?
            .keep();
        let database = directory.join("capture.db");
        Ok(Self {
            target,
            directory,
            database,
            committed: false,
        })
    }
    pub fn save(&mut self, progress: impl Fn(u64, u64)) -> Result<Option<String>> {
        if !self.committed {
            anyhow::ensure!(self.database.exists(), "capture database is missing");
            crate::storage::import_capture(&self.database, &self.target, progress)?;
            // Set before cleanup. Retrying cleanup can never import the data twice.
            self.committed = true;
        }
        Ok(self.cleanup().err().map(|e| {
            format!(
                "数据已保存；缓存清理失败：{e:#}。位置：{}",
                self.directory.display()
            )
        }))
    }
    pub fn discard(&self) -> Result<()> {
        self.cleanup()
    }
    fn cleanup(&self) -> Result<()> {
        if self.directory.exists() {
            std::fs::remove_dir_all(&self.directory)
                .context("remove this run's capture directory")?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::{
            tests::{packet, status},
            Calibration, Decoder,
        },
        runtime::Shared,
        source::SessionInfo,
        storage::Store,
    };
    fn populate(path: &Path, n: u32) {
        let info = SessionInfo {
            device: "synthetic".into(),
            transport: "usb-cc",
            calibration: status(),
            received: chrono::Utc::now(),
            name: None,
        };
        let mut store = Store::open(path.to_str().unwrap(), &info).unwrap();
        let mut decoder = Decoder::new(Calibration::parse(&status()).unwrap(), info.device.clone());
        for id in 0..n {
            assert_eq!(
                store
                    .insert_batches(&[decoder.decode(packet(id), chrono::Utc::now()).unwrap()])
                    .unwrap(),
                800
            );
        }
        store
            .finish(&Shared {
                accepted: n as u64 * 800,
                saved: n as u64 * 800,
                ..Shared::default()
            })
            .unwrap();
    }
    #[test]
    fn merges_multiple_sessions_remaps_frames_preserves_history_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("final.db");
        populate(&target, 1);
        let mut workspace = CaptureWorkspace::new(target.to_str().unwrap()).unwrap();
        populate(&workspace.database, 6);
        populate(&workspace.database, 2);
        let c = rusqlite::Connection::open(&target).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM measurements", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            800
        );
        workspace.save(|_, _| {}).unwrap();
        workspace.save(|_, _| {}).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM sessions", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            c.query_row("SELECT count(*) FROM measurements", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            7200
        );
        assert_eq!(c.query_row("SELECT count(*) FROM measurements m JOIN frames f ON f.id=m.frame_id WHERE m.session_id<>f.session_id",[],|r|r.get::<_,u64>(0)).unwrap(),0);
        assert_eq!(
            c.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, u64>(
                0
            ))
            .unwrap(),
            0
        );
        assert!(!workspace.directory.exists());
    }
    #[test]
    fn failed_import_rolls_back_and_can_retry_without_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("final.db");
        populate(&target, 1);
        let c = rusqlite::Connection::open(&target).unwrap();
        c.execute_batch("CREATE TRIGGER fail_import BEFORE INSERT ON measurements WHEN (SELECT count(*) FROM measurements)>1000 BEGIN SELECT RAISE(FAIL,'synthetic import error'); END;").unwrap();
        let mut workspace = CaptureWorkspace::new(target.to_str().unwrap()).unwrap();
        populate(&workspace.database, 7);
        assert!(workspace.save(|_, _| {}).is_err());
        assert!(workspace.database.exists());
        assert!(!workspace.committed);
        assert_eq!(
            c.query_row("SELECT count(*) FROM measurements", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            800
        );
        assert_eq!(
            c.query_row("SELECT count(*) FROM sessions", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            1
        );
        c.execute_batch("DROP TRIGGER fail_import").unwrap();
        workspace.save(|_, _| {}).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM measurements", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            6400
        );
    }
    #[test]
    fn discard_removes_only_this_run_and_drop_retains_cache() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("final.db");
        populate(&target, 1);
        let a = CaptureWorkspace::new(target.to_str().unwrap()).unwrap();
        let b = CaptureWorkspace::new(target.to_str().unwrap()).unwrap();
        populate(&a.database, 1);
        populate(&b.database, 1);
        let retained = b.database.clone();
        drop(b);
        a.discard().unwrap();
        assert!(retained.exists());
        assert!(target.exists());
        assert!(!a.directory.exists());
    }
}
