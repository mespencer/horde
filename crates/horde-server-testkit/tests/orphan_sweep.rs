//! The first process a test spawns sweeps the orphans an earlier, killed
//! test process left behind. Its own binary, so no other test has swept
//! before it looks.
#![cfg(unix)]

#[tokio::test]
async fn the_first_spawn_sweeps_the_orphans() {
    assert!(
        !horde_server_testkit::reap::swept(),
        "nothing has spawned yet"
    );
    let process = horde_server_testkit::process::SpawnedProcess::spawn(
        "true",
        std::path::Path::new("/bin/true"),
        &[],
        &[],
    )
    .expect("true spawns");
    assert!(
        horde_server_testkit::reap::swept(),
        "a spawn sweeps before it starts anything"
    );
    let (status, _, _) = process.wait().await.expect("true exits");
    assert!(status.success());
}
