//! Attachment plumbing: stashing content into a Payload and making a
//! Payload's attachments real files again (DESIGN §5, §8).
//!
//! The sending seam is [`AttachmentBroker::stash`], and it is where the
//! carrier is chosen — the caller hands over bytes and a name, never a
//! transport decision. Content at or under the inline threshold rides in
//! the event itself; anything larger is uploaded to the Server's Blob
//! Store and travels as a digest reference. Either way the attachment
//! carries its digest, so every receiver can verify what it reads.
//!
//! The receiving seam is [`AttachmentBroker::materialize`]: before a
//! delivered message's text reaches an Agent, its attachments become files
//! under the Agent's checkout root — the same moment and the same reason
//! the commit half of the payload is obtained (`ShareBroker::obtain`). A
//! message that names files the Agent cannot open would be transport
//! becoming the Agent's concern.
//!
//! Integrity is not the transport's job alone: an inline attachment is
//! verified against its own digest here, and a store download is verified
//! by the shared transfer loop — tampered content is refused on this side
//! regardless of what the Server said.

use std::path::{Path, PathBuf};

use horde_proto::blob::{BlobDigest, download_blob, fits_inline, upload_blob};
use horde_proto::v1::Attachment;
use horde_proto::v1::blob_service_client::BlobServiceClient;
use tonic::transport::Channel;

use crate::error::WorkerError;

/// Where one Agent's materialized attachments land, relative to its
/// checkout root: outside every repo directory, so attachment files never
/// dirty a working tree the "commit, then share" rule cares about.
pub const ATTACHMENTS_DIR: &str = ".horde/attachments";

/// Stashes outgoing content and materializes incoming attachments.
///
/// One per Runtime, like the [`ShareBroker`](crate::share::ShareBroker) it
/// works beside. The channel is the Worker's authenticated dial to the
/// Server; without one, inline attachments still work in both directions
/// and anything needing the store refuses honestly.
#[derive(Debug, Clone)]
pub struct AttachmentBroker {
    service: Option<Channel>,
}

impl AttachmentBroker {
    #[must_use]
    pub fn new(service: Option<Channel>) -> Self {
        Self { service }
    }

    fn client(&self, needed_for: &str) -> Result<BlobServiceClient<Channel>, WorkerError> {
        self.service
            .clone()
            .map(|channel| horde_proto::limited!(BlobServiceClient::new(channel)))
            .ok_or_else(|| {
                WorkerError::Attachment(format!(
                    "no blob service is configured on this worker, and {needed_for} \
                     needs the server's blob store"
                ))
            })
    }

    /// Turns content into the `Attachment` a Payload carries, choosing the
    /// carrier: inline at or under the threshold, a Blob Store upload
    /// above it. The caller never decides — that is the auto-detect seam.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Attachment`] when the content needs the store and no
    /// blob service is configured, and [`WorkerError::Rpc`] when the
    /// upload is refused.
    pub async fn stash(
        &self,
        name: &str,
        media_type: &str,
        content: Vec<u8>,
    ) -> Result<Attachment, WorkerError> {
        let digest = BlobDigest::of(&content);
        let size_bytes = content.len() as u64;
        let inline = if fits_inline(size_bytes) {
            content
        } else {
            let mut client = self.client(&format!("stashing {size_bytes} bytes of {name:?}"))?;
            upload_blob(&mut client, &content).await?;
            Vec::new()
        };
        Ok(Attachment {
            name: name.to_owned(),
            blob_digest: digest.to_string(),
            size_bytes,
            media_type: media_type.to_owned(),
            inline,
        })
    }

    /// The attachment's content, verified against its digest whichever
    /// carrier it rode.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Attachment`] when the attachment carries no
    /// well-formed digest or its inline content does not match it, and the
    /// transfer's own refusals for a store fetch.
    pub async fn fetch(&self, attachment: &Attachment) -> Result<Vec<u8>, WorkerError> {
        let digest = BlobDigest::parse(&attachment.blob_digest).ok_or_else(|| {
            WorkerError::Attachment(format!(
                "attachment {:?} carries no blob address ({:?} is not sha256:<64 hex>), \
                 so its content cannot be verified",
                attachment.name, attachment.blob_digest
            ))
        })?;

        if attachment.inline.is_empty() {
            let mut client = self.client(&format!("fetching {digest}"))?;
            // The shared download loop verifies against the address.
            return Ok(download_blob(&mut client, &digest).await?);
        }

        let observed = BlobDigest::of(&attachment.inline);
        if observed == digest {
            Ok(attachment.inline.clone())
        } else {
            Err(WorkerError::Attachment(format!(
                "attachment {:?} claims {digest} but its inline content hashes to \
                 {observed}; refusing to hand over content that does not match its address",
                attachment.name
            )))
        }
    }

    /// Makes every attachment a real file under
    /// `<checkout_root>/{ATTACHMENTS_DIR}`, named
    /// `<12 hex of digest>-<sanitized name>` — the digest prefix keeps two
    /// same-named attachments distinct, and re-delivery overwrites with
    /// identical verified content rather than erroring.
    ///
    /// Returns the written paths in attachment order.
    ///
    /// # Errors
    ///
    /// As [`AttachmentBroker::fetch`], plus the filesystem refusing.
    pub async fn materialize(
        &self,
        attachments: &[Attachment],
        checkout_root: &Path,
    ) -> Result<Vec<PathBuf>, WorkerError> {
        if attachments.is_empty() {
            return Ok(Vec::new());
        }
        let directory = checkout_root.join(ATTACHMENTS_DIR);
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| {
                WorkerError::Attachment(format!(
                    "could not create {}: {error}",
                    directory.display()
                ))
            })?;

        let mut written = Vec::with_capacity(attachments.len());
        for attachment in attachments {
            let content = self.fetch(attachment).await?;
            let path = directory.join(file_name_for(attachment));
            tokio::fs::write(&path, &content).await.map_err(|error| {
                WorkerError::Attachment(format!("could not write {}: {error}", path.display()))
            })?;
            written.push(path);
        }
        Ok(written)
    }
}

/// The on-disk name for one attachment: a digest prefix for uniqueness,
/// then the display name reduced to filesystem-safe characters.
fn file_name_for(attachment: &Attachment) -> String {
    let prefix: String = attachment
        .blob_digest
        .chars()
        .skip("sha256:".len())
        .take(12)
        .collect();
    format!("{prefix}-{}", safe_file_name(&attachment.name))
}

/// Reduces a display name to one safe path segment.
///
/// `Attachment.name` is display text from another machine — not a path,
/// and not trusted as one. Everything outside `[A-Za-z0-9._-]` becomes
/// `_`, a name that would start with `.` or `-` is prefixed so it can
/// neither hide in a listing nor read as a flag, and an empty name gets a
/// word. Separators fall to `_` with the rest, so `../../etc/passwd`
/// cannot leave the attachments directory.
#[must_use]
pub fn safe_file_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    match sanitized.chars().next() {
        None => "attachment".to_owned(),
        Some('.' | '-') => format!("_{sanitized}"),
        Some(_) => sanitized,
    }
}

#[cfg(test)]
mod tests {
    use super::{AttachmentBroker, file_name_for, safe_file_name};
    use horde_proto::blob::{BlobDigest, INLINE_THRESHOLD_BYTES};
    use horde_proto::v1::Attachment;

    #[tokio::test]
    async fn small_content_is_stashed_inline_and_needs_no_service() {
        let broker = AttachmentBroker::new(None);
        let attachment = broker
            .stash("note.txt", "text/plain", b"tiny".to_vec())
            .await
            .expect("inline stashing needs no server");
        assert_eq!(attachment.inline, b"tiny");
        assert_eq!(attachment.size_bytes, 4);
        assert_eq!(attachment.blob_digest, BlobDigest::of(b"tiny").to_string());
        assert_eq!(attachment.media_type, "text/plain");
    }

    /// The seam's boundary: at the threshold stays inline, one byte over
    /// needs the store — and without a service that is an honest refusal
    /// naming the size, never a silent inline that the log would bounce.
    #[tokio::test]
    async fn the_carrier_flips_exactly_at_the_threshold() {
        let broker = AttachmentBroker::new(None);
        let at = usize::try_from(INLINE_THRESHOLD_BYTES).expect("fits");

        let inline = broker
            .stash("at.bin", "", vec![1u8; at])
            .await
            .expect("at the threshold rides inline");
        assert_eq!(inline.inline.len(), at);

        let error = broker
            .stash("over.bin", "", vec![1u8; at + 1])
            .await
            .expect_err("over the threshold needs the store");
        let rendered = error.to_string();
        assert!(
            rendered.contains("no blob service") && rendered.contains("65537"),
            "the refusal names the gap and the size: {rendered}"
        );
    }

    #[tokio::test]
    async fn fetching_inline_content_verifies_it_against_its_address() {
        let broker = AttachmentBroker::new(None);
        let honest = Attachment {
            name: "log.txt".to_owned(),
            blob_digest: BlobDigest::of(b"as promised").to_string(),
            size_bytes: 11,
            media_type: String::new(),
            inline: b"as promised".to_vec(),
        };
        assert_eq!(
            broker.fetch(&honest).await.expect("verifies"),
            b"as promised"
        );

        let tampered = Attachment {
            inline: b"tampered!!!".to_vec(),
            ..honest.clone()
        };
        let error = broker.fetch(&tampered).await.expect_err("must refuse");
        assert!(
            error.to_string().contains("does not match its address"),
            "got {error}"
        );

        let addressless = Attachment {
            blob_digest: "not-an-address".to_owned(),
            ..honest
        };
        let error = broker.fetch(&addressless).await.expect_err("must refuse");
        assert!(
            error.to_string().contains("not-an-address"),
            "the refusal names what was given: {error}"
        );
    }

    /// Every substitution here is a distinct escape: separators, traversal,
    /// a dotfile, a flag-shaped name, an empty name.
    #[test]
    fn display_names_are_reduced_to_one_safe_path_segment() {
        assert_eq!(safe_file_name("failing-test.log"), "failing-test.log");
        assert_eq!(safe_file_name("../../etc/passwd"), "_.._.._etc_passwd");
        assert_eq!(safe_file_name("a b/c"), "a_b_c");
        assert_eq!(safe_file_name(".hidden"), "_.hidden");
        assert_eq!(safe_file_name("-rf"), "_-rf");
        assert_eq!(safe_file_name(""), "attachment");
    }

    #[test]
    fn on_disk_names_carry_a_digest_prefix_for_uniqueness() {
        let attachment = Attachment {
            name: "shot.png".to_owned(),
            blob_digest: BlobDigest::of(b"pixels").to_string(),
            size_bytes: 6,
            media_type: "image/png".to_owned(),
            inline: b"pixels".to_vec(),
        };
        let digest_hex = BlobDigest::of(b"pixels").hex().to_owned();
        assert_eq!(
            file_name_for(&attachment),
            format!("{}-shot.png", &digest_hex[..12])
        );
    }
}
