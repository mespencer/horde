//! The shared transfer loops, exercised in this package.
//!
//! `upload_blob` and `download_blob` are driven end-to-end by the server,
//! worker, and CLI suites — but cargo-mutants runs a mutated package's own
//! tests, so a loop whose only exercise lives in its consumers is a loop
//! whose mutants all survive. The remote end here is a scripted in-memory
//! `BlobService` — a map of blobs behind the real generated service trait,
//! served over a real socket — because what is under test is *this* crate's
//! chunking and verification, not a store; the double stores exactly what
//! it is told (under the declared address, unverified), which is also what
//! lets a test script a corrupt remote.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use horde_proto::blob::{BlobDigest, TRANSFER_CHUNK_BYTES, download_blob, upload_blob};
use horde_proto::v1::blob_service_client::BlobServiceClient;
use horde_proto::v1::blob_service_server::{BlobService, BlobServiceServer};
use horde_proto::v1::{
    BlobChunk, DownloadBlobRequest, DownloadBlobResponse, UploadBlobRequest, UploadBlobResponse,
    upload_blob_request,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Blobs by address. Shared with the test so it can inspect what arrived —
/// and seed a corrupt entry.
type Blobs = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// The scripted remote end. Stores what uploads declare, serves what the
/// map holds, in deliberately tiny frames so reassembly is exercised.
struct ScriptedStore {
    blobs: Blobs,
}

#[tonic::async_trait]
impl BlobService for ScriptedStore {
    async fn upload_blob(
        &self,
        request: Request<Streaming<UploadBlobRequest>>,
    ) -> Result<Response<UploadBlobResponse>, Status> {
        let mut stream = request.into_inner();
        let mut declared = String::new();
        let mut bytes = Vec::new();
        while let Some(frame) = stream.message().await? {
            match frame.body {
                Some(upload_blob_request::Body::Open(open)) => declared = open.blob_digest,
                Some(upload_blob_request::Body::Chunk(chunk)) => {
                    bytes.extend_from_slice(&chunk.data);
                }
                None => return Err(Status::invalid_argument("an empty frame")),
            }
        }
        let size_bytes = bytes.len() as u64;
        let mut blobs = self
            .blobs
            .lock()
            .map_err(|_| Status::internal("the map lock"))?;
        blobs.insert(declared.clone(), bytes);
        Ok(Response::new(UploadBlobResponse {
            blob_digest: declared,
            size_bytes,
        }))
    }

    type DownloadBlobStream = ReceiverStream<Result<DownloadBlobResponse, Status>>;

    async fn download_blob(
        &self,
        request: Request<DownloadBlobRequest>,
    ) -> Result<Response<Self::DownloadBlobStream>, Status> {
        let wanted = request.into_inner().blob_digest;
        let stored = {
            let blobs = self
                .blobs
                .lock()
                .map_err(|_| Status::internal("the map lock"))?;
            blobs
                .get(&wanted)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("no blob {wanted}")))?
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            // Three-byte frames: the loops must reassemble whatever framing
            // the remote chose, not just their own.
            for piece in stored.chunks(3) {
                let frame = DownloadBlobResponse {
                    chunk: Some(BlobChunk {
                        data: piece.to_vec(),
                    }),
                };
                if sender.send(Ok(frame)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

/// Serves the scripted store on a loopback socket; returns the shared map
/// and a connected client.
async fn scripted_remote() -> Setup<(Blobs, BlobServiceClient<tonic::transport::Channel>)> {
    let blobs: Blobs = Arc::new(Mutex::new(HashMap::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let store = ScriptedStore {
        blobs: Arc::clone(&blobs),
    };
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(BlobServiceServer::new(store))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    let client = BlobServiceClient::connect(format!("http://{address}")).await?;
    Ok((blobs, client))
}

/// The upload loop really moves the bytes and really declares the address:
/// what the remote received is byte-identical, filed under the digest of
/// the content, and the answer echoes that address and size. A body
/// replaced with `Ok(Default::default())` fails every one of these.
#[tokio::test]
async fn uploading_declares_the_address_and_delivers_every_byte() -> Setup<()> {
    let (blobs, mut client) = scripted_remote().await?;

    // Multi-chunk with an odd tail, so the chunking itself is on trial.
    let content = vec![0x5Au8; 2 * TRANSFER_CHUNK_BYTES + 7];
    let answer = upload_blob(&mut client, &content).await?;

    let address = BlobDigest::of(&content).to_string();
    assert_eq!(answer.blob_digest, address);
    assert_eq!(answer.size_bytes, content.len() as u64);
    let received = {
        let map = blobs.lock().map_err(|_| "the map lock")?;
        map.get(&address).cloned()
    };
    assert_eq!(
        received.as_deref(),
        Some(content.as_slice()),
        "the remote must hold exactly the uploaded bytes, under their address"
    );
    Ok(())
}

/// The download loop reassembles the remote's framing and hands back
/// verified bytes — and only verified bytes: a remote serving different
/// content under the requested address is refused as DATA_LOSS, whichever
/// way the comparison could have been flipped.
#[tokio::test]
async fn downloading_reassembles_and_verifies_against_the_address() -> Setup<()> {
    let (blobs, mut client) = scripted_remote().await?;

    let content = b"seventeen honest bytes!".to_vec();
    let address = BlobDigest::of(&content);
    {
        let mut map = blobs.lock().map_err(|_| "the map lock")?;
        map.insert(address.to_string(), content.clone());
    }
    let fetched = download_blob(&mut client, &address).await?;
    assert_eq!(
        fetched, content,
        "honest content must come back whole across the remote's own framing"
    );

    // The corrupt remote: same address, different bytes.
    {
        let mut map = blobs.lock().map_err(|_| "the map lock")?;
        map.insert(address.to_string(), b"tampered".to_vec());
    }
    let status = download_blob(&mut client, &address)
        .await
        .expect_err("content that does not hash to its address must be refused");
    assert_eq!(status.code(), tonic::Code::DataLoss);
    assert!(
        status.message().contains(&address.to_string()),
        "the refusal names the address that was asked for: {status}"
    );
    Ok(())
}

/// A refusing remote surfaces as the call's own error, not as data: the
/// loop must propagate a mid-call status rather than swallowing it into
/// an empty answer.
#[tokio::test]
async fn a_refusing_remote_is_an_error_not_an_empty_blob() -> Setup<()> {
    let (_blobs, mut client) = scripted_remote().await?;
    let absent = BlobDigest::of(b"never stored");
    let status = download_blob(&mut client, &absent)
        .await
        .expect_err("an absent blob must refuse");
    assert_eq!(status.code(), tonic::Code::NotFound);
    Ok(())
}
