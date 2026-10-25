//! Operating-system layer: positional I/O and byte-range locks.
//!
//! Locks are advisory byte-range locks far beyond any data offset, so they
//! never collide with page I/O (Windows enforces byte-range locks against
//! reads and writes). On Linux they are open-file-description locks, on
//! Windows they belong to the handle, and on other Unix systems they belong
//! to the process — which is why a database file may only be opened once
//! per process (see the open-file registry in `db.rs`).

use std::fs::File;
use std::io;

#[cfg(unix)]
mod imp {
    use super::*;
    use std::os::unix::fs::FileExt;
    use std::os::unix::io::AsRawFd;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    use std::sync::atomic::{AtomicBool, Ordering};

    pub fn read_exact_at(file: &File, buf: &mut [u8], off: u64) -> io::Result<()> {
        file.read_exact_at(buf, off)
    }

    pub fn write_all_at(file: &File, buf: &[u8], off: u64) -> io::Result<()> {
        file.write_all_at(buf, off)
    }

    /// Set once a kernel rejects open-file-description locks (very old
    /// Linux, WSL1); we then fall back to process-associated locks.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    static NO_OFD: AtomicBool = AtomicBool::new(false);

    fn fcntl_lock(file: &File, kind: libc::c_int, off: u64, len: u64, wait: bool) -> io::Result<bool> {
        // SAFETY: `flock` is a plain C struct for which all-zero is valid.
        let mut fl: libc::flock = unsafe { std::mem::zeroed() };
        fl.l_type = kind as _;
        fl.l_whence = libc::SEEK_SET as _;
        fl.l_start = off as libc::off_t;
        fl.l_len = len as libc::off_t;
        loop {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let cmd = if NO_OFD.load(Ordering::Relaxed) {
                if wait { libc::F_SETLKW } else { libc::F_SETLK }
            } else if wait {
                libc::F_OFD_SETLKW
            } else {
                libc::F_OFD_SETLK
            };
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            let cmd = if wait { libc::F_SETLKW } else { libc::F_SETLK };

            // SAFETY: valid descriptor and a pointer to an initialised flock.
            let r = unsafe { libc::fcntl(file.as_raw_fd(), cmd, &fl as *const libc::flock) };
            if r != -1 {
                return Ok(true);
            }
            let e = io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EAGAIN) | Some(libc::EACCES) if !wait => return Ok(false),
                #[cfg(any(target_os = "linux", target_os = "android"))]
                Some(libc::EINVAL) if !NO_OFD.load(Ordering::Relaxed) => {
                    NO_OFD.store(true, Ordering::Relaxed);
                    continue;
                }
                _ => return Err(e),
            }
        }
    }

    pub fn lock(file: &File, off: u64, len: u64, exclusive: bool, wait: bool) -> io::Result<bool> {
        let kind = if exclusive { libc::F_WRLCK } else { libc::F_RDLCK };
        fcntl_lock(file, kind as libc::c_int, off, len, wait)
    }

    pub fn unlock(file: &File, off: u64, len: u64) -> io::Result<()> {
        fcntl_lock(file, libc::F_UNLCK as libc::c_int, off, len, false).map(|_| ())
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::os::windows::fs::FileExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, ERROR_LOCK_VIOLATION};
    use windows_sys::Win32::Storage::FileSystem::{
        LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, UnlockFileEx,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    pub fn read_exact_at(file: &File, mut buf: &mut [u8], mut off: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match file.seek_read(buf, off) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    buf = &mut buf[n..];
                    off += n as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn write_all_at(file: &File, mut buf: &[u8], mut off: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match file.seek_write(buf, off) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    buf = &buf[n..];
                    off += n as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn overlapped(off: u64) -> OVERLAPPED {
        // SAFETY: OVERLAPPED is a plain C struct for which all-zero is valid.
        let mut ov: OVERLAPPED = unsafe { std::mem::zeroed() };
        ov.Anonymous.Anonymous.Offset = off as u32;
        ov.Anonymous.Anonymous.OffsetHigh = (off >> 32) as u32;
        ov
    }

    pub fn lock(file: &File, off: u64, len: u64, exclusive: bool, wait: bool) -> io::Result<bool> {
        let mut flags = 0;
        if exclusive {
            flags |= LOCKFILE_EXCLUSIVE_LOCK;
        }
        if !wait {
            flags |= LOCKFILE_FAIL_IMMEDIATELY;
        }
        let mut ov = overlapped(off);
        // SAFETY: valid handle; `ov` outlives the call, which completes
        // synchronously because the handle was not opened for overlapped I/O.
        let ok = unsafe {
            LockFileEx(
                file.as_raw_handle() as _,
                flags,
                0,
                len as u32,
                (len >> 32) as u32,
                &mut ov,
            )
        };
        if ok != 0 {
            return Ok(true);
        }
        let e = io::Error::last_os_error();
        let code = e.raw_os_error().map(|c| c as u32);
        if !wait && (code == Some(ERROR_LOCK_VIOLATION) || code == Some(ERROR_IO_PENDING)) {
            return Ok(false);
        }
        Err(e)
    }

    pub fn unlock(file: &File, off: u64, len: u64) -> io::Result<()> {
        let mut ov = overlapped(off);
        // SAFETY: valid handle and OVERLAPPED.
        let ok = unsafe { UnlockFileEx(file.as_raw_handle() as _, 0, len as u32, (len >> 32) as u32, &mut ov) };
        if ok != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

pub(crate) use imp::{lock, read_exact_at, unlock, write_all_at};
