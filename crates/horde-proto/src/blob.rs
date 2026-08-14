//! Content addressing for the Blob Store, and the transfer loops both ends
//! share.
//!
//! `Attachment.blob_digest` and `blob.proto` promise a specific address
//! format — `sha256:<64 lowercase hex>` — and promise that both ends of
//! every transfer verify it. This module is the single owner of that
//! contract: the digest type, the incremental hasher every verification
//! runs on, the inline threshold that decides whether content rides in the
//! event or in the store, and the upload/download chunking against
//! `BlobService`. The Server's store, the Worker's stash seam, and the CLI
//! all call in here, so none of them can drift on what an address is or
//! forget to check one.
//!
//! # The inline threshold, and why it is 64 KiB
//!
//! Events carry blob references and hashes, never bulk (ADR-0001) — but a
//! reference to a four-line log snippet would cost a round trip per read
//! for content smaller than the request describing it. So an attachment at
//! or under [`INLINE_THRESHOLD_BYTES`] rides inside the event, and anything
//! larger becomes a store upload; the seam decides, senders never do. The
//! number balances the two costs that pull on it. Upward: sparing the
//! store round trip for small content. Downward: every inline byte is paid
//! again on every replay to every attached client, and it sits in the
//! log's SQLite row forever — SQLite's own internal-versus-external blob
//! measurements put the efficiency crossover near 100 KiB at default page
//! sizes, and an event must stay far below the transport's 4 MiB message
//! ceiling even when batched. 64 KiB holds a generous text snippet, keeps
//! any single event a small fraction of a sync batch, and stays inside
//! SQLite's comfortable range.

use sha2::Digest as _;
use tokio_stream::StreamExt as _;

use crate::v1::blob_service_client::BlobServiceClient;
use crate::v1::{
    Attachment, BlobChunk, DownloadBlobRequest, Event, Payload, UploadBlobOpen, UploadBlobRequest,
    UploadBlobResponse, event::Kind, upload_blob_request,
};

/// The largest attachment that may travel inline in an event, in bytes.
/// Everything larger becomes a Blob Store reference. See the module docs
/// for how the number was chosen.
pub const INLINE_THRESHOLD_BYTES: u64 = 64 * 1024;

/// How many bytes ride one `BlobChunk`: 1 MiB, a quarter of tonic's default
/// 4 MiB message ceiling, so a chunk plus its framing can never brush the
/// limit either side would refuse at.
pub const TRANSFER_CHUNK_BYTES: usize = 1024 * 1024;

/// A well-formed blob address: `sha256:<64 lowercase hex>`.
///
/// Parsing is the only way in, so holding one *is* the proof the string is
/// safe to use — including as path segments under a store directory, which
/// is why the format refuses anything a filesystem could reinterpret.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobDigest {
    hex: String,
}

impl BlobDigest {
    /// Accepts exactly `sha256:<64 lowercase hex>`.
    ///
    /// Uppercase hex is refused rather than folded: two spellings of one
    /// address would be two store paths for one blob.
    #[must_use]
    pub fn parse(address: &str) -> Option<Self> {
        let hex = address.strip_prefix("sha256:")?;
        let well_formed = hex.len() == 64
            && hex
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase());
        well_formed.then(|| Self {
            hex: hex.to_owned(),
        })
    }

    /// The address of these bytes.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        let mut hashing = Hashing::new();
        hashing.update(bytes);
        hashing.finish().0
    }

    /// The bare hex, without the `sha256:` prefix.
    #[must_use]
    pub fn hex(&self) -> &str {
        &self.hex
    }
}

impl core::fmt::Display for BlobDigest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "sha256:{}", self.hex)
    }
}

/// An incremental SHA-256 over a byte stream, counting as it goes.
///
/// Every verification in the blob path — the store's writes and reads, the
/// receiving end of a download — runs on this one hasher, so "verified"
/// means the same thing everywhere.
#[derive(Debug, Default)]
pub struct Hashing {
    inner: sha2::Sha256,
    len: u64,
}

impl Hashing {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.inner.update(bytes);
        self.len += bytes.len() as u64;
    }

    /// The digest and byte count of everything seen.
    #[must_use]
    pub fn finish(self) -> (BlobDigest, u64) {
        let hex = self
            .inner
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        (BlobDigest { hex }, self.len)
    }
}

/// Whether content of this size may ride inline in an event.
#[must_use]
pub fn fits_inline(size_bytes: u64) -> bool {
    size_bytes <= INLINE_THRESHOLD_BYTES
}

/// The first attachment in this event carrying more inline bytes than the
/// threshold allows — the named predicate behind the log's refusal to store
/// bulk. `None` means the event is honest: every attachment either fits
/// inline or is a store reference.
#[must_use]
pub fn smuggled_bulk(event: &Event) -> Option<&Attachment> {
    payload_of(event.kind.as_ref()?)?
        .attachments
        .iter()
        .find(|attachment| !fits_inline(attachment.inline.len() as u64))
}

/// The payload an event kind carries, for the kinds that carry one.
fn payload_of(kind: &Kind) -> Option<&Payload> {
    match kind {
        Kind::AssignmentSent(sent) => sent.assignment.as_ref()?.payload.as_ref(),
        Kind::NoteSent(sent) => sent.note.as_ref()?.payload.as_ref(),
        Kind::ResultReported(reported) => reported.result.as_ref()?.payload.as_ref(),
        Kind::AgentSpawned(spawned) => spawned
            .spec
            .as_ref()?
            .initial_assignment
            .as_ref()?
            .payload
            .as_ref(),
        _ => None,
    }
}

/// Uploads one blob, chunked, declaring its digest and size up front.
///
/// Returns what the server verified — the digest and size an `Attachment`
/// should carry.
///
/// # Errors
///
/// The server's refusal: a digest mismatch, a size mismatch, a blob over
/// the store's cap, or the transport failing mid-stream.
pub async fn upload_blob(
    client: &mut BlobServiceClient<tonic::transport::Channel>,
    bytes: &[u8],
) -> Result<UploadBlobResponse, tonic::Status> {
    let digest = BlobDigest::of(bytes);
    let open = UploadBlobRequest {
        body: Some(upload_blob_request::Body::Open(UploadBlobOpen {
            blob_digest: digest.to_string(),
            size_bytes: bytes.len() as u64,
        })),
    };
    let chunks = bytes
        .chunks(TRANSFER_CHUNK_BYTES)
        .map(|chunk| UploadBlobRequest {
            body: Some(upload_blob_request::Body::Chunk(BlobChunk {
                data: chunk.to_vec(),
            })),
        });
    let frames: Vec<UploadBlobRequest> = std::iter::once(open).chain(chunks).collect();

    Ok(client
        .upload_blob(tokio_stream::iter(frames))
        .await?
        .into_inner())
}

/// Downloads one blob and verifies it against its address before handing it
/// back — the receiving half of "integrity-verified on read". A server
/// whose stream ends cleanly but whose bytes hash wrong is reported as
/// `DATA_LOSS`, never returned as data.
///
/// # Errors
///
/// `NOT_FOUND` when the store has no such blob, `DATA_LOSS` when what
/// arrived does not hash to `digest`, or the transport failing mid-stream.
pub async fn download_blob(
    client: &mut BlobServiceClient<tonic::transport::Channel>,
    digest: &BlobDigest,
) -> Result<Vec<u8>, tonic::Status> {
    let mut stream = client
        .download_blob(DownloadBlobRequest {
            blob_digest: digest.to_string(),
        })
        .await?
        .into_inner();

    let mut bytes = Vec::new();
    while let Some(message) = stream.next().await {
        if let Some(chunk) = message?.chunk {
            bytes.extend_from_slice(&chunk.data);
        }
    }

    let observed = BlobDigest::of(&bytes);
    if observed == *digest {
        Ok(bytes)
    } else {
        Err(tonic::Status::data_loss(format!(
            "the downloaded bytes hash to {observed}, not the requested {digest}; \
             refusing to hand back content that does not match its address"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{BlobDigest, Hashing, INLINE_THRESHOLD_BYTES, fits_inline, smuggled_bulk};
    use crate::v1::event::Kind;
    use crate::v1::{
        AgentResult, AgentSpawned, AgentSpec, Assignment, AssignmentSent, Attachment, Event, Note,
        NoteSent, Payload, ResultReported,
    };

    /// The reference vector: SHA-256 of the empty input, straight from the
    /// FIPS 180-4 examples. A hasher that disagrees with this is not
    /// hashing SHA-256, whatever else its tests say.
    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn the_digest_of_known_bytes_matches_the_published_vectors() {
        assert_eq!(
            BlobDigest::of(b"").to_string(),
            format!("sha256:{EMPTY_SHA256}")
        );
        // "abc", the other canonical FIPS 180-4 vector.
        assert_eq!(
            BlobDigest::of(b"abc").to_string(),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// Hashing in pieces must equal hashing at once — chunked transfers
    /// verify through [`Hashing`], and a split-sensitive hasher would fail
    /// honest uploads.
    #[test]
    fn incremental_hashing_is_split_insensitive_and_counts_bytes() {
        let mut hashing = Hashing::new();
        hashing.update(b"a");
        hashing.update(b"");
        hashing.update(b"bc");
        let (digest, len) = hashing.finish();
        assert_eq!(digest, BlobDigest::of(b"abc"));
        assert_eq!(len, 3);
    }

    #[test]
    fn parsing_accepts_exactly_the_canonical_form() {
        let canonical = format!("sha256:{EMPTY_SHA256}");
        let parsed = BlobDigest::parse(&canonical).expect("the canonical form parses");
        assert_eq!(parsed.to_string(), canonical);
        assert_eq!(parsed.hex(), EMPTY_SHA256);
    }

    /// Every rejection here is a distinct way an address could smuggle
    /// something into a store path or split one blob across two addresses.
    #[test]
    fn malformed_addresses_are_refused() {
        let uppercase = format!("sha256:{}", EMPTY_SHA256.to_uppercase());
        for wrong in [
            "",
            "e3b0c442",                          // no scheme
            &format!("md5:{EMPTY_SHA256}"),      // wrong scheme
            &format!("sha256:{EMPTY_SHA256}aa"), // too long
            "sha256:e3b0c442",                   // too short
            &uppercase,                          // a second spelling of the same blob
            "sha256:../../../../etc/passwd-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // traversal
            &format!("sha256:{}g", &EMPTY_SHA256[..63]), // non-hex
        ] {
            assert!(
                BlobDigest::parse(wrong).is_none(),
                "{wrong:?} must not parse as a blob address"
            );
        }
    }

    /// The boundary itself: at the threshold rides inline, one past does
    /// not. An off-by-one here silently moves where bulk enters the log.
    #[test]
    fn the_inline_threshold_is_inclusive() {
        assert!(fits_inline(0));
        assert!(fits_inline(INLINE_THRESHOLD_BYTES));
        assert!(!fits_inline(INLINE_THRESHOLD_BYTES + 1));
    }

    fn attachment(inline_len: usize) -> Attachment {
        Attachment {
            name: "big.log".to_owned(),
            blob_digest: BlobDigest::of(&vec![7u8; inline_len]).to_string(),
            size_bytes: inline_len as u64,
            media_type: String::new(),
            inline: vec![7u8; inline_len],
        }
    }

    fn event_with(kind: Kind) -> Event {
        Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(kind),
        }
    }

    fn payload_with(inline_len: usize) -> Payload {
        Payload {
            git_refs: Vec::new(),
            attachments: vec![attachment(inline_len)],
        }
    }

    /// Every payload-carrying kind is checked: an event kind the predicate
    /// forgot would be the one path bulk still enters the log through.
    #[test]
    fn smuggled_bulk_finds_oversized_inline_content_in_every_payload_carrying_kind() {
        let over = usize::try_from(INLINE_THRESHOLD_BYTES).expect("fits usize") + 1;
        let kinds: Vec<Kind> = vec![
            Kind::AssignmentSent(AssignmentSent {
                from: None,
                assignment: Some(Assignment {
                    assignment_id: None,
                    text: String::new(),
                    payload: Some(payload_with(over)),
                }),
                delivery: 0,
            }),
            Kind::NoteSent(NoteSent {
                from: None,
                note: Some(Note {
                    text: String::new(),
                    payload: Some(payload_with(over)),
                }),
                delivery: 0,
            }),
            Kind::ResultReported(ResultReported {
                result: Some(AgentResult {
                    assignment_id: None,
                    status: 0,
                    summary: String::new(),
                    payload: Some(payload_with(over)),
                }),
            }),
            Kind::AgentSpawned(AgentSpawned {
                parent_agent_id: None,
                spawned_by: None,
                spec: Some(AgentSpec {
                    initial_assignment: Some(Assignment {
                        assignment_id: None,
                        text: String::new(),
                        payload: Some(payload_with(over)),
                    }),
                    ..AgentSpec::default()
                }),
                tree_path: Vec::new(),
            }),
        ];
        for kind in kinds {
            let event = event_with(kind);
            let found = smuggled_bulk(&event).expect("the oversized attachment must be found");
            assert_eq!(found.name, "big.log");
        }
    }

    #[test]
    fn honest_events_are_not_flagged() {
        // At the threshold exactly: inline is legitimate.
        let at_threshold = usize::try_from(INLINE_THRESHOLD_BYTES).expect("fits usize");
        let event = event_with(Kind::ResultReported(ResultReported {
            result: Some(AgentResult {
                assignment_id: None,
                status: 0,
                summary: String::new(),
                payload: Some(payload_with(at_threshold)),
            }),
        }));
        assert!(smuggled_bulk(&event).is_none());

        // A pure reference: no inline bytes at all, any size.
        let reference = Attachment {
            inline: Vec::new(),
            size_bytes: 10 * 1024 * 1024,
            ..attachment(0)
        };
        let event = event_with(Kind::ResultReported(ResultReported {
            result: Some(AgentResult {
                assignment_id: None,
                status: 0,
                summary: String::new(),
                payload: Some(Payload {
                    git_refs: Vec::new(),
                    attachments: vec![reference],
                }),
            }),
        }));
        assert!(smuggled_bulk(&event).is_none());

        // A kind that carries no payload, and an event with no kind.
        let chunk = event_with(Kind::AgentOutputChunk(
            crate::v1::AgentOutputChunk::default(),
        ));
        assert!(smuggled_bulk(&chunk).is_none());
        let kindless = Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: None,
        };
        assert!(smuggled_bulk(&kindless).is_none());
    }
}
