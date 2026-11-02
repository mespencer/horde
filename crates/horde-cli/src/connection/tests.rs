//! The connection decision's refusals, against real directories.

use crate::ClientError;

/// With no endpoint anywhere, an enrolled dir is told where endpoints come
/// from; an empty one is told how to get a server at all.
#[test]
fn an_endpointless_refusal_depends_on_whether_the_dir_is_enrolled() {
    let dir = tempfile::tempdir().expect("a dir");
    let empty =
        super::resolve_connection(None, Some(dir.path()), None).expect_err("nothing anywhere");
    assert!(matches!(empty, ClientError::NoServer { .. }), "{empty:?}");
    for file in ["identity.pem", "identity.key", "ca.pem"] {
        std::fs::write(dir.path().join(file), "x").expect("writes");
    }
    let enrolled = super::resolve_connection(None, Some(dir.path()), None)
        .expect_err("enrolled, endpointless");
    assert!(
        matches!(enrolled, ClientError::NoEndpoint { .. }),
        "{enrolled:?}"
    );
}
