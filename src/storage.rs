//! Storage backends. Every byte the engine reads or writes goes through the
//! [`Storage`] trait, which is what lets the test suite fail any call or
//! simulate a power cut at any point (see `FaultStorage`).

use std::fs::File;
use std::io;
use std::sync::RwLock;

use crate::sys;

pub(crate) trait Storage: Send + Sync {
    /// Fill `buf` from `off`; reading past the end is an error.
    fn read_at(&self, buf: &mut [u8], off: u64) -> io::Result<()>;
    fn write_at(&self, buf: &[u8], off: u64) -> io::Result<()>;
    /// Durability barrier: every write issued before it is on stable storage
    /// when it returns.
    fn sync(&self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    /// Byte-range lock beyond the data. Returns `false` only when `wait` is
    /// false and the range is held elsewhere.
    fn lock(&self, off: u64, len: u64, exclusive: bool, wait: bool) -> io::Result<bool>;
    fn unlock(&self, off: u64, len: u64) -> io::Result<()>;
    /// `false` for backends that cannot be shared with other processes, so
    /// the engine can skip cross-process coordination entirely.
    fn shared_across_processes(&self) -> bool;
}

pub(crate) struct FileStorage {
    file: File,
}

impl FileStorage {
    pub(crate) fn new(file: File) -> Self {
        Self { file }
    }
}

impl Storage for FileStorage {
    fn read_at(&self, buf: &mut [u8], off: u64) -> io::Result<()> {
        sys::read_exact_at(&self.file, buf, off)
    }

    fn write_at(&self, buf: &[u8], off: u64) -> io::Result<()> {
        sys::write_all_at(&self.file, buf, off)
    }

    fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn lock(&self, off: u64, len: u64, exclusive: bool, wait: bool) -> io::Result<bool> {
        sys::lock(&self.file, off, len, exclusive, wait)
    }

    fn unlock(&self, off: u64, len: u64) -> io::Result<()> {
        sys::unlock(&self.file, off, len)
    }

    fn shared_across_processes(&self) -> bool {
        true
    }
}

/// A database that lives in memory (`Db::memory`). Useful for tests of code
/// that uses tradedb; nothing is persisted.
pub(crate) struct MemStorage {
    data: RwLock<Vec<u8>>,
}

impl MemStorage {
    pub(crate) fn new(data: Vec<u8>) -> Self {
        Self {
            data: RwLock::new(data),
        }
    }
}

pub(crate) fn mem_read(data: &[u8], buf: &mut [u8], off: u64) -> io::Result<()> {
    let start = usize::try_from(off).map_err(|_| io::Error::from(io::ErrorKind::UnexpectedEof))?;
    let end = start.checked_add(buf.len()).ok_or(io::ErrorKind::UnexpectedEof)?;
    if end > data.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    buf.copy_from_slice(&data[start..end]);
    Ok(())
}

pub(crate) fn mem_write(data: &mut Vec<u8>, buf: &[u8], off: u64) -> io::Result<()> {
    let start = usize::try_from(off).map_err(|_| io::Error::other("offset out of range"))?;
    let end = start + buf.len();
    if end > data.len() {
        data.resize(end, 0);
    }
    data[start..end].copy_from_slice(buf);
    Ok(())
}

impl Storage for MemStorage {
    fn read_at(&self, buf: &mut [u8], off: u64) -> io::Result<()> {
        mem_read(&self.data.read().unwrap(), buf, off)
    }

    fn write_at(&self, buf: &[u8], off: u64) -> io::Result<()> {
        mem_write(&mut self.data.write().unwrap(), buf, off)
    }

    fn sync(&self) -> io::Result<()> {
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.data.read().unwrap().len() as u64)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.data.write().unwrap().resize(len as usize, 0);
        Ok(())
    }

    fn lock(&self, _off: u64, _len: u64, _exclusive: bool, _wait: bool) -> io::Result<bool> {
        Ok(true)
    }

    fn unlock(&self, _off: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }

    fn shared_across_processes(&self) -> bool {
        false
    }
}

#[cfg(test)]
pub(crate) use fault::{FaultHandle, Op};

#[cfg(test)]
mod fault {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// One recorded storage operation.
    #[derive(Clone, Debug)]
    pub(crate) enum Op {
        Write(u64, Vec<u8>),
        SetLen(u64),
        Sync,
    }

    #[derive(Default)]
    pub(crate) struct FaultState {
        pub data: Vec<u8>,
        pub log: Vec<Op>,
        pub recording: bool,
        pub reads: u64,
        pub writes: u64,
        pub syncs: u64,
        pub fail_read: Option<u64>,
        pub fail_write: Option<u64>,
        pub fail_sync: Option<u64>,
        /// Once set, every write and sync fails (a dead disk).
        pub dead: bool,
    }

    /// In-memory storage that records every write and sync and can fail the
    /// Nth call of a kind. Cloning the handle shares the state.
    #[derive(Clone, Default)]
    pub(crate) struct FaultHandle(pub Arc<Mutex<FaultState>>);

    pub(crate) struct FaultStorage(pub FaultHandle);

    fn injected() -> io::Error {
        io::Error::other("injected fault")
    }

    impl FaultHandle {
        pub fn with_data(data: Vec<u8>) -> Self {
            let h = Self::default();
            h.0.lock().unwrap().data = data;
            h
        }
        pub fn storage(&self) -> Box<dyn Storage> {
            Box::new(FaultStorage(self.clone()))
        }
        pub fn data(&self) -> Vec<u8> {
            self.0.lock().unwrap().data.clone()
        }
        pub fn start_recording(&self) -> Vec<u8> {
            let mut s = self.0.lock().unwrap();
            s.log.clear();
            s.recording = true;
            s.data.clone()
        }
        pub fn take_log(&self) -> Vec<Op> {
            let mut s = self.0.lock().unwrap();
            s.recording = false;
            std::mem::take(&mut s.log)
        }
        pub fn arm(&self, f: impl FnOnce(&mut FaultState)) {
            let mut s = self.0.lock().unwrap();
            s.reads = 0;
            s.writes = 0;
            s.syncs = 0;
            s.fail_read = None;
            s.fail_write = None;
            s.fail_sync = None;
            s.dead = false;
            f(&mut s);
        }
        pub fn counts(&self) -> (u64, u64, u64) {
            let s = self.0.lock().unwrap();
            (s.reads, s.writes, s.syncs)
        }
    }

    impl Storage for FaultStorage {
        fn read_at(&self, buf: &mut [u8], off: u64) -> io::Result<()> {
            let mut s = self.0.0.lock().unwrap();
            s.reads += 1;
            if s.fail_read == Some(s.reads) {
                return Err(injected());
            }
            mem_read(&s.data, buf, off)
        }

        fn write_at(&self, buf: &[u8], off: u64) -> io::Result<()> {
            let mut s = self.0.0.lock().unwrap();
            s.writes += 1;
            if s.dead || s.fail_write == Some(s.writes) {
                s.dead = true;
                return Err(injected());
            }
            if s.recording {
                s.log.push(Op::Write(off, buf.to_vec()));
            }
            mem_write(&mut s.data, buf, off)
        }

        fn sync(&self) -> io::Result<()> {
            let mut s = self.0.0.lock().unwrap();
            s.syncs += 1;
            if s.dead || s.fail_sync == Some(s.syncs) {
                s.dead = true;
                return Err(injected());
            }
            if s.recording {
                s.log.push(Op::Sync);
            }
            Ok(())
        }

        fn len(&self) -> io::Result<u64> {
            Ok(self.0.0.lock().unwrap().data.len() as u64)
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            let mut s = self.0.0.lock().unwrap();
            if s.recording {
                s.log.push(Op::SetLen(len));
            }
            s.data.resize(len as usize, 0);
            Ok(())
        }

        fn lock(&self, _off: u64, _len: u64, _exclusive: bool, _wait: bool) -> io::Result<bool> {
            Ok(true)
        }

        fn unlock(&self, _off: u64, _len: u64) -> io::Result<()> {
            Ok(())
        }

        fn shared_across_processes(&self) -> bool {
            false
        }
    }
}
