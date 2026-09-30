//! Creating persistence files that only the server's user can read.
//!
//! Snapshots, the AOF and the cluster state hold every key and value. With
//! the default umask they came out world-readable (0644), and the data
//! directory 0755, which hands the whole dataset to any local user — a
//! boundary SECURITY.md puts in scope. Files are created 0600 and the
//! directory 0700; `O_NOFOLLOW` refuses a symlink planted where a file is
//! about to be opened.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

/// Mode for every persistence file.
pub const PRIVATE_FILE_MODE: u32 = 0o600;

/// Mode for a data directory this server creates.
pub const PRIVATE_DIR_MODE: u32 = 0o700;

/// `mkdir -p` with 0700 for the directories it creates. An existing directory
/// is left as the operator made it.
pub fn create_private_dir_all(dir: &Path) -> io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(PRIVATE_DIR_MODE)
        .create(dir)
}

/// Open for appending, creating 0600, refusing a symlink.
pub fn open_private_append(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

/// Create or truncate for writing, 0600, refusing a symlink.
pub fn create_private(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

/// `std::fs::write`, but 0600 and without following a symlink.
pub fn write_private(path: &Path, contents: impl AsRef<[u8]>) -> io::Result<()> {
    use std::io::Write;
    let mut f = create_private(path)?;
    f.write_all(contents.as_ref())?;
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn files_and_dirs_are_private() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("a/b");
        create_private_dir_all(&dir).unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);

        let f = dir.join("dump-0.rdb");
        write_private(&f, b"x").unwrap();
        let mode = std::fs::metadata(&f).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        let aof = dir.join("appendonly.aof");
        drop(open_private_append(&aof).unwrap());
        let mode = std::fs::metadata(&aof).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn a_symlink_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("elsewhere");
        std::fs::write(&target, b"keep").unwrap();
        let link = root.path().join("appendonly.aof");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(open_private_append(&link).is_err());
        assert!(create_private(&link).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
    }
}
