//! The Blob Store: content-addressed attachment storage under the state
//! directory (DESIGN §8).
//!
//! Large payload content travels by reference: an `Attachment` names a
//! digest, the bytes live here, and the event log stays lean (ADR-0001 —
//! the log carries references, never bulk). The store is a directory of
//! files named by their own SHA-256 (`blobs/sha256/<aa>/<62 hex>`), which
//! buys the properties the payload path relies on for free: writes are
//! idempotent, two uploads of the same content cannot collide, and every
//! read can prove it returned what the address promised.
//!
//! # Integrity is verified at both edges, not assumed in the middle
//!
//! An upload declares its digest and size before its first byte; the store
//! hashes what actually arrives and refuses a mismatch, so an accepted
//! upload is one whose reference can be trusted. Every read re-hashes the
//! file on its way out and refuses to serve bytes that no longer match
//! their address — a corrupt store must fail the reader loudly, never hand
//! over plausible garbage under a truthful-looking name. Writes land in a
//! private `incoming/` file and are renamed into place only after
//! verification, so a half-written or mislabeled upload can never be
//! visible under a digest.
//!
//! # What is deliberately absent: deletion
//!
//! **Flagged DESIGN gap, not an oversight.** DESIGN §8 ties blob lifetime
//! to the session ("GC'd at session deletion"), but no session-deletion
//! operation exists (sessions archive, which retains the log), the store
//! records no ownership a collector could act on, and one blob may be
//! referenced from many sessions once dedup does its job. Until DESIGN
//! decides what may be deleted and when — per-session ownership records, a
//! reference count, or lifetime tied to log retention — this store exposes
//! no delete at all: an unreferenced blob wastes disk, but a deleted blob
//! that an event still references breaks the log's promise that a replayed
//! reference resolves. The same gap covers the *total*-size story; the cap
//! enforced here ([`BlobStore::max_blob_bytes`]) bounds each blob, not the
//! store.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use horde_proto::blob::{BlobDigest, Hashing, TRANSFER_CHUNK_BYTES};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The per-blob size cap when the operator does not choose one: 256 MiB.
///
/// Large enough for screenshots, build artifacts, and fat logs; small
/// enough that one runaway attachment cannot fill the state volume before
/// anyone notices. DESIGN §8 calls the cap server-configurable, which is
/// `--blob-max-bytes` on `--serve`.
pub const DEFAULT_MAX_BLOB_BYTES: u64 = 256 * 1024 * 1024;

/// Distinguishes concurrent in-flight writes of the same content.
static WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Why the store refused or failed.
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    /// The address is not `sha256:<64 lowercase hex>`. Refused before any
    /// filesystem path is formed from it.
    #[error("{given:?} is not a blob address (sha256:<64 lowercase hex>)")]
    MalformedDigest { given: String },

    /// No blob with this address is stored.
    #[error("no blob {digest} in the store")]
    NotFound { digest: String },

    /// The stored bytes no longer hash to their address. The read refuses:
    /// serving plausible garbage under a truthful-looking name is the one
    /// failure a content-addressed store exists to make impossible.
    #[error(
        "blob {expected} is corrupt: its {observed_size} stored bytes hash to {observed}; \
         refusing to serve content that does not match its address"
    )]
    Corrupt {
        expected: String,
        observed: String,
        observed_size: u64,
    },

    /// What the upload declared is not what arrived. The declaration rides
    /// events as the attachment's identity, so a lie here would propagate.
    #[error(
        "the upload declared {declared} but its bytes {arrived}; \
         refusing to store content under a reference that does not describe it"
    )]
    DeclarationMismatch { declared: String, arrived: String },

    /// The blob exceeds the per-blob cap.
    #[error(
        "the blob is over the {limit}-byte per-blob cap after {observed} bytes; \
         the cap is `--blob-max-bytes` on the server"
    )]
    TooLarge { limit: u64, observed: u64 },

    /// A read produced more than any accepted upload could contain: an
    /// empty run, more bytes than the per-blob cap, or more chunks than a
    /// cap-sized blob yields. Tampering on disk or a broken reader —
    /// either way the read stops, because a loop whose progress nothing
    /// bounds cannot be bounded by a clock (QUALITY_GATES: bound progress
    /// by count or structure, never by time).
    #[error(
        "the read left honest bounds after {chunks} chunk(s) and {bytes} bytes \
         against the {limit}-byte per-blob cap; refusing a runaway read"
    )]
    Runaway { chunks: u64, bytes: u64, limit: u64 },

    /// The filesystem failed underneath the store.
    #[error("the blob store could not {context}: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl BlobError {
    fn io(context: &'static str) -> impl FnOnce(std::io::Error) -> Self {
        move |source| Self::Io { context, source }
    }
}

/// A verified, stored blob — what an upload answer and an `Attachment`
/// carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBlob {
    pub digest: BlobDigest,
    pub size_bytes: u64,
}

/// The content-addressed store on disk.
#[derive(Debug, Clone)]
pub struct BlobStore {
    root: PathBuf,
    max_blob_bytes: u64,
}

impl BlobStore {
    /// Opens (creating if absent) the store rooted at `root` — under the
    /// server's state directory in production, a temp directory in tests.
    ///
    /// # Errors
    ///
    /// [`BlobError::Io`] when the directories cannot be created.
    pub fn open(root: &Path) -> Result<Self, BlobError> {
        std::fs::create_dir_all(root.join("sha256")).map_err(BlobError::io("create its root"))?;
        std::fs::create_dir_all(root.join("incoming"))
            .map_err(BlobError::io("create its incoming directory"))?;
        Ok(Self {
            root: root.to_owned(),
            max_blob_bytes: DEFAULT_MAX_BLOB_BYTES,
        })
    }

    /// Overrides the per-blob cap — `--blob-max-bytes`.
    #[must_use]
    pub fn with_max_blob_bytes(mut self, max_blob_bytes: u64) -> Self {
        self.max_blob_bytes = max_blob_bytes;
        self
    }

    /// The per-blob cap uploads are held to.
    #[must_use]
    pub fn max_blob_bytes(&self) -> u64 {
        self.max_blob_bytes
    }

    /// Where one digest's bytes live: `sha256/<aa>/<62 hex>`. Fanned out by
    /// the first byte so one directory never holds every blob.
    fn blob_path(&self, digest: &BlobDigest) -> PathBuf {
        let (fan, rest) = digest.hex().split_at(2);
        self.root.join("sha256").join(fan).join(rest)
    }

    /// Begins one upload that has declared its digest and size.
    ///
    /// The declaration is checked twice: the size against the cap here,
    /// before any byte lands, and both digest and size against what
    /// actually arrived at [`BlobWrite::finish`].
    ///
    /// # Errors
    ///
    /// [`BlobError::TooLarge`] when the declared size is over the cap, and
    /// [`BlobError::Io`] when the incoming file cannot be created.
    pub async fn begin(
        &self,
        declared: &BlobDigest,
        declared_size: u64,
    ) -> Result<BlobWrite, BlobError> {
        if declared_size > self.max_blob_bytes {
            return Err(BlobError::TooLarge {
                limit: self.max_blob_bytes,
                observed: declared_size,
            });
        }
        let temp_path = self.root.join("incoming").join(format!(
            "{}-{}-{}",
            declared.hex(),
            std::process::id(),
            WRITE_COUNTER.fetch_add(1, Ordering::Relaxed),
        ));
        let file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(BlobError::io("create an incoming file"))?;
        Ok(BlobWrite {
            file: Some(file),
            temp_path,
            final_path: self.blob_path(declared),
            declared: declared.clone(),
            declared_size,
            cap: self.max_blob_bytes,
            hashing: Hashing::new(),
            written: 0,
        })
    }

    /// Stores one whole in-memory blob — [`BlobStore::begin`] driven to
    /// completion, for callers that already hold the bytes.
    ///
    /// # Errors
    ///
    /// As [`BlobStore::begin`] and [`BlobWrite::finish`].
    pub async fn put(&self, bytes: &[u8]) -> Result<StoredBlob, BlobError> {
        let digest = BlobDigest::of(bytes);
        let mut write = self.begin(&digest, bytes.len() as u64).await?;
        write.chunk(bytes).await?;
        write.finish().await
    }

    /// Opens one blob for verified, chunked reading.
    ///
    /// # Errors
    ///
    /// [`BlobError::NotFound`] when no such blob is stored, and
    /// [`BlobError::Io`] when it exists but cannot be opened.
    pub async fn read(&self, digest: &BlobDigest) -> Result<BlobRead, BlobError> {
        let path = self.blob_path(digest);
        let file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(BlobError::NotFound {
                    digest: digest.to_string(),
                });
            }
            Err(source) => return Err(BlobError::io("open a stored blob")(source)),
        };
        Ok(BlobRead {
            file,
            expected: digest.clone(),
            hashing: Some(Hashing::new()),
        })
    }

    /// Reads one whole blob, verified — [`BlobStore::read`] drained, for
    /// callers that want the bytes in memory.
    ///
    /// The drain is bounded by structure, not by clock: no verified blob
    /// exceeds the cap that admitted it, so a read that yields more —
    /// bytes or chunks — has left the space of honest stores and is
    /// refused rather than followed.
    ///
    /// # Errors
    ///
    /// As [`BlobStore::read`] and [`BlobRead::chunk`] — notably
    /// [`BlobError::Corrupt`] when the stored bytes no longer match — plus
    /// [`BlobError::Runaway`] when the on-disk file outgrew the cap.
    pub async fn read_all(&self, digest: &BlobDigest) -> Result<Vec<u8>, BlobError> {
        let mut read = self.read(digest).await?;
        let mut bytes = Vec::new();
        let mut chunks = 0_u64;
        while let Some(chunk) = read.chunk().await? {
            if !read_stays_honest(chunk.len(), chunks, bytes.len() as u64, self.max_blob_bytes) {
                return Err(BlobError::Runaway {
                    chunks: chunks + 1,
                    bytes: bytes.len() as u64 + chunk.len() as u64,
                    limit: self.max_blob_bytes,
                });
            }
            chunks += 1;
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

/// Whether one more run of bytes keeps a blob read inside what an honest
/// store can produce: the reader never yields an empty run, a verified
/// blob never exceeds the cap that admitted it, and a cap-sized blob
/// yields at most `cap / TRANSFER_CHUNK_BYTES + 1` runs.
///
/// A named predicate rather than a condition in the loop, because the
/// states that would exercise it in place are exactly the ones an honest
/// store never produces (QUALITY_GATES: extract the compound condition,
/// test the truth table). Every consumer of [`BlobRead::chunk`] — the
/// in-memory drain above and the service's download stream — runs it, so
/// a reader that stops making progress fails in milliseconds instead of
/// spinning a loop no clock can bound.
#[must_use]
pub(crate) fn read_stays_honest(
    chunk_len: usize,
    chunks_before: u64,
    bytes_before: u64,
    cap: u64,
) -> bool {
    let max_chunks = cap / (TRANSFER_CHUNK_BYTES as u64) + 1;
    chunk_len > 0
        && bytes_before.saturating_add(chunk_len as u64) <= cap
        && chunks_before < max_chunks
}

/// One upload in flight: bytes in, hash alongside, verified rename at the
/// end. Dropped without [`BlobWrite::finish`], the incoming file is
/// removed — nothing unverified survives.
#[derive(Debug)]
pub struct BlobWrite {
    /// `Some` until `finish` closes it; `Drop` uses `None` as "finished".
    file: Option<tokio::fs::File>,
    temp_path: PathBuf,
    final_path: PathBuf,
    declared: BlobDigest,
    declared_size: u64,
    cap: u64,
    hashing: Hashing,
    written: u64,
}

impl BlobWrite {
    /// Appends one run of bytes, holding the running total to the cap.
    ///
    /// # Errors
    ///
    /// [`BlobError::TooLarge`] the moment the cap is crossed — an upload
    /// that under-declared its size does not get to stream past the limit —
    /// and [`BlobError::Io`] when the write fails.
    pub async fn chunk(&mut self, data: &[u8]) -> Result<(), BlobError> {
        self.written += data.len() as u64;
        if self.written > self.cap {
            return Err(BlobError::TooLarge {
                limit: self.cap,
                observed: self.written,
            });
        }
        self.hashing.update(data);
        if let Some(file) = self.file.as_mut() {
            file.write_all(data)
                .await
                .map_err(BlobError::io("write an incoming chunk"))?;
        }
        Ok(())
    }

    /// Verifies the declaration against what arrived and renames the blob
    /// into place. Idempotent with respect to existing content: the same
    /// digest landing twice leaves one file.
    ///
    /// # Errors
    ///
    /// [`BlobError::DeclarationMismatch`] when the bytes hash or count
    /// differently than declared, and [`BlobError::Io`] when the file
    /// cannot be settled into place.
    pub async fn finish(mut self) -> Result<StoredBlob, BlobError> {
        if let Some(mut file) = self.file.take() {
            file.flush()
                .await
                .map_err(BlobError::io("flush an incoming file"))?;
            file.sync_all()
                .await
                .map_err(BlobError::io("sync an incoming file"))?;
        }

        let (observed, size) = std::mem::take(&mut self.hashing).finish();
        if observed != self.declared || size != self.declared_size {
            let arrived = format!("hash to {observed} and count {size}");
            let declared = format!("{} at {} bytes", self.declared, self.declared_size);
            // Drop removes the temp file.
            return Err(BlobError::DeclarationMismatch { declared, arrived });
        }

        if let Some(parent) = self.final_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(BlobError::io("create a fan-out directory"))?;
        }
        // Rename is atomic within the state directory's filesystem, and
        // replacing an existing file of the same digest re-lands identical,
        // already-verified content.
        tokio::fs::rename(&self.temp_path, &self.final_path)
            .await
            .map_err(BlobError::io("settle a verified blob into place"))?;

        Ok(StoredBlob {
            digest: self.declared.clone(),
            size_bytes: size,
        })
    }
}

impl Drop for BlobWrite {
    fn drop(&mut self) {
        // `finish` renames the temp file away, making this a no-op; every
        // other exit leaves an unverified file that must not linger.
        let _ = std::fs::remove_file(&self.temp_path);
    }
}

/// One blob being read back, re-hashed as it streams.
#[derive(Debug)]
pub struct BlobRead {
    file: tokio::fs::File,
    expected: BlobDigest,
    /// `Some` while reading; taken at EOF for the final verification.
    hashing: Option<Hashing>,
}

impl BlobRead {
    /// The next run of bytes, or `None` at a verified end of blob.
    ///
    /// The final `None` is load-bearing: it is returned only after the
    /// whole file re-hashed to its address, so a reader that drains this
    /// stream has proof, not hope. A caller that stops early has verified
    /// nothing.
    ///
    /// # Errors
    ///
    /// [`BlobError::Corrupt`] at EOF when the stored bytes no longer match
    /// their address, and [`BlobError::Io`] when the read fails.
    pub async fn chunk(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        if self.hashing.is_none() {
            return Ok(None);
        }
        let mut buffer = vec![0u8; TRANSFER_CHUNK_BYTES];
        let filled = self
            .file
            .read(&mut buffer)
            .await
            .map_err(BlobError::io("read a stored blob"))?;

        if filled > 0 {
            buffer.truncate(filled);
            if let Some(hashing) = self.hashing.as_mut() {
                hashing.update(&buffer);
            }
            return Ok(Some(buffer));
        }

        let Some(hashing) = self.hashing.take() else {
            return Ok(None);
        };
        let (observed, observed_size) = hashing.finish();
        if observed == self.expected {
            Ok(None)
        } else {
            Err(BlobError::Corrupt {
                expected: self.expected.to_string(),
                observed: observed.to_string(),
                observed_size,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BlobError, BlobStore, DEFAULT_MAX_BLOB_BYTES};
    use horde_proto::blob::BlobDigest;

    fn store() -> (tempfile::TempDir, BlobStore) {
        let directory = tempfile::tempdir().expect("a temp dir");
        let store = BlobStore::open(&directory.path().join("blobs")).expect("the store opens");
        (directory, store)
    }

    #[tokio::test]
    async fn a_stored_blob_reads_back_verified() {
        let (_dir, store) = store();
        let stored = store.put(b"a screenshot's bytes").await.expect("stores");
        assert_eq!(stored.digest, BlobDigest::of(b"a screenshot's bytes"));
        assert_eq!(stored.size_bytes, 20);

        let bytes = store.read_all(&stored.digest).await.expect("reads back");
        assert_eq!(bytes, b"a screenshot's bytes");
    }

    /// Content addressing makes writes idempotent: the same bytes twice is
    /// one blob, and the second write must not disturb the first.
    #[tokio::test]
    async fn storing_the_same_content_twice_is_one_blob() {
        let (_dir, store) = store();
        let first = store.put(b"same bytes").await.expect("first put");
        let second = store.put(b"same bytes").await.expect("second put");
        assert_eq!(first, second);
        assert_eq!(
            store.read_all(&first.digest).await.expect("still readable"),
            b"same bytes"
        );
    }

    #[tokio::test]
    async fn a_missing_blob_is_not_found_by_name() {
        let (_dir, store) = store();
        let absent = BlobDigest::of(b"never stored");
        let error = store.read_all(&absent).await.expect_err("must refuse");
        assert!(
            matches!(&error, BlobError::NotFound { digest } if digest == &absent.to_string()),
            "got {error}"
        );
    }

    /// The first-class integrity refusal: bytes tampered with on disk must
    /// fail the *reader*, loudly, naming both hashes — never be served.
    #[tokio::test]
    async fn a_tampered_blob_refuses_to_be_read() {
        let (dir, store) = store();
        let stored = store.put(b"trusted content").await.expect("stores");

        // Corrupt the stored file behind the store's back.
        let (fan, rest) = stored.digest.hex().split_at(2);
        let path = dir.path().join("blobs/sha256").join(fan).join(rest);
        std::fs::write(&path, b"tampered content").expect("tamper");

        let error = store.read_all(&stored.digest).await.expect_err("refuses");
        let rendered = error.to_string();
        assert!(matches!(error, BlobError::Corrupt { .. }), "got {rendered}");
        assert!(
            rendered.contains(&stored.digest.to_string())
                && rendered.contains(&BlobDigest::of(b"tampered content").to_string()),
            "the refusal must name both hashes: {rendered}"
        );
    }

    /// An upload whose bytes do not match its declaration is refused and
    /// leaves nothing behind — not under the declared digest, and not in
    /// the incoming directory.
    #[tokio::test]
    async fn a_lying_upload_is_refused_and_leaves_no_trace() {
        let (dir, store) = store();
        let declared = BlobDigest::of(b"what was promised");

        let mut write = store.begin(&declared, 17).await.expect("begins");
        write.chunk(b"something else!!!").await.expect("streams");
        let error = write.finish().await.expect_err("must refuse");
        assert!(matches!(error, BlobError::DeclarationMismatch { .. }));

        let error = store.read_all(&declared).await.expect_err("nothing stored");
        assert!(matches!(error, BlobError::NotFound { .. }));
        let incoming: Vec<_> = std::fs::read_dir(dir.path().join("blobs/incoming"))
            .expect("incoming exists")
            .collect();
        assert!(incoming.is_empty(), "no unverified file may linger");
    }

    /// A declared size that disagrees with the arrived byte count is a lie
    /// even when the digest matches what arrived — the size rides events as
    /// attachment metadata and must be as trustworthy as the hash.
    #[tokio::test]
    async fn a_wrong_declared_size_is_refused() {
        let (_dir, store) = store();
        let digest = BlobDigest::of(b"honest bytes");
        let mut write = store.begin(&digest, 999).await.expect("begins");
        write.chunk(b"honest bytes").await.expect("streams");
        let error = write.finish().await.expect_err("must refuse");
        assert!(matches!(error, BlobError::DeclarationMismatch { .. }));
    }

    #[tokio::test]
    async fn the_per_blob_cap_refuses_at_declaration_and_again_mid_stream() {
        let (_dir, store) = store();
        let store = store.with_max_blob_bytes(8);
        assert_eq!(store.max_blob_bytes(), 8);

        // Declared over the cap: refused before a byte lands.
        let digest = BlobDigest::of(b"123456789");
        let error = store.begin(&digest, 9).await.err();
        assert!(matches!(error, Some(BlobError::TooLarge { limit: 8, .. })));

        // Declared under the cap but streaming over it: refused at the byte
        // that crosses, not trusted to stop on its own.
        let mut write = store.begin(&digest, 8).await.expect("begins");
        let error = write.chunk(b"123456789").await.expect_err("must refuse");
        assert!(matches!(error, BlobError::TooLarge { limit: 8, .. }));

        // The boundary itself: a blob exactly at the cap is accepted whole.
        // An off-by-one here silently shrinks the documented limit.
        let at_cap = BlobDigest::of(b"12345678");
        let mut write = store.begin(&at_cap, 8).await.expect("begins at the cap");
        write.chunk(b"12345678").await.expect("fills to the cap");
        let stored = write.finish().await.expect("at the cap is within it");
        assert_eq!(stored.size_bytes, 8);
    }

    /// The cap is enforced against the running total, not any single chunk:
    /// two honest-sized chunks whose sum crosses the limit are refused at
    /// the chunk that crosses.
    #[tokio::test]
    async fn the_cap_counts_accumulated_chunks_not_each_alone() {
        let (_dir, store) = store();
        let store = store.with_max_blob_bytes(10);
        let digest = BlobDigest::of(b"123456123456");

        let mut write = store.begin(&digest, 10).await.expect("begins");
        write.chunk(b"123456").await.expect("the first chunk fits");
        let error = write.chunk(b"123456").await.expect_err("the sum crosses");
        assert!(matches!(error, BlobError::TooLarge { limit: 10, .. }));
    }

    #[tokio::test]
    async fn the_default_cap_is_the_documented_quarter_gigabyte() {
        let (_dir, store) = store();
        assert_eq!(store.max_blob_bytes(), DEFAULT_MAX_BLOB_BYTES);
        assert_eq!(DEFAULT_MAX_BLOB_BYTES, 256 * 1024 * 1024);
    }

    /// The whole truth table of "one more run keeps the read honest",
    /// each factor flipped alone: an empty run, bytes past the cap
    /// (boundary held from both sides), and more chunks than a cap-sized
    /// blob can yield. The states an honest store never produces are
    /// exactly why this is a named predicate rather than a loop condition.
    #[test]
    fn a_read_stays_honest_only_while_every_bound_holds() {
        use super::read_stays_honest;
        // A small cap: one chunk suffices, so max chunks is 1.
        assert!(read_stays_honest(5, 0, 0, 8));
        assert!(
            read_stays_honest(4, 0, 4, 8),
            "exactly to the cap is honest"
        );
        assert!(
            !read_stays_honest(0, 0, 0, 8),
            "an empty run is no progress"
        );
        assert!(!read_stays_honest(9, 0, 0, 8), "past the cap in one run");
        assert!(!read_stays_honest(4, 0, 5, 8), "past the cap accumulated");
        assert!(
            !read_stays_honest(4, 1, 0, 8),
            "a second chunk when one suffices"
        );

        // A multi-chunk cap: 3 MiB yields at most 3 full chunks + 1 tail.
        let cap = 3 * 1024 * 1024;
        assert!(
            read_stays_honest(100, 3, 0, cap),
            "the fourth chunk is honest"
        );
        assert!(
            !read_stays_honest(100, 4, 0, cap),
            "a fifth chunk is more than a cap-sized blob yields"
        );
    }

    /// The honest path to `Runaway`: a stored file that outgrew the cap
    /// behind the store's back. The drain must refuse at the crossing,
    /// naming the cap, rather than follow the file wherever it goes.
    #[tokio::test]
    async fn a_file_grown_past_the_cap_refuses_as_a_runaway_read() {
        let (dir, store) = store();
        let store = store.with_max_blob_bytes(8);
        let stored = store.put(b"12345678").await.expect("at the cap stores");

        let (fan, rest) = stored.digest.hex().split_at(2);
        let path = dir.path().join("blobs/sha256").join(fan).join(rest);
        std::fs::write(&path, vec![7u8; 20]).expect("outgrow the cap");

        let error = store.read_all(&stored.digest).await.expect_err("refuses");
        let rendered = error.to_string();
        assert!(
            matches!(error, BlobError::Runaway { limit: 8, .. }),
            "got {rendered}"
        );
        assert!(
            rendered.contains("runaway"),
            "the refusal says why: {rendered}"
        );
    }

    /// The guard on "missing" is exact: an unreadable blob is an I/O
    /// failure naming the operation, never dressed up as NotFound — a
    /// caller told "no such blob" re-uploads, and a caller told the disk
    /// refused goes looking at the disk.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_unreadable_blob_is_an_io_failure_not_a_missing_one() {
        use std::os::unix::fs::PermissionsExt;

        let (dir, store) = store();
        let stored = store.put(b"present but locked").await.expect("stores");

        let (fan, rest) = stored.digest.hex().split_at(2);
        let path = dir.path().join("blobs/sha256").join(fan).join(rest);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000))
            .expect("lock the file");

        let error = store.read_all(&stored.digest).await.expect_err("refuses");
        assert!(
            matches!(&error, BlobError::Io { context, .. } if *context == "open a stored blob"),
            "an unreadable file is an I/O failure, got {error}"
        );
    }

    /// Chunked reads and whole reads agree — the streaming path is the
    /// production path, so it is held to the same verified end.
    #[tokio::test]
    async fn chunked_reading_drains_verified_and_then_stays_finished() {
        let (_dir, store) = store();
        let content = vec![42u8; 3 * 1024 * 1024 + 17]; // several chunks, odd tail
        let stored = store.put(&content).await.expect("stores");

        let mut read = store.read(&stored.digest).await.expect("opens");
        let mut drained = Vec::new();
        let mut chunks = 0u32;
        while let Some(chunk) = read.chunk().await.expect("chunks verify") {
            drained.extend_from_slice(&chunk);
            chunks += 1;
            assert!(chunks < 1024, "the read must reach EOF");
        }
        assert_eq!(drained, content);
        assert!(chunks >= 3, "several chunks were promised, got {chunks}");
        assert_eq!(
            read.chunk().await.expect("after the end"),
            None,
            "a drained read stays drained rather than re-verifying"
        );
    }
}
