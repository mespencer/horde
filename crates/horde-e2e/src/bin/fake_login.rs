//! The scripted provider login command (DESIGN §4, hermetically).
//!
//! Behaves like a provider CLI's `login`: prompts on its terminal, reads the
//! answer, stands up a local OAuth-callback listener, prints the URL the
//! browser must visit, requires exactly one hit on the listener, and exits
//! 0. Driven end-to-end by the e2e login scenarios through the real relay —
//! which is why it is a real binary and not a shell one-liner: the PTY, the
//! prompt, the listener, and the exit code are all load-bearing.
//!
//! std only, deliberately: the whole flow is sequential and blocking, and a
//! runtime would be weight without work.
//!
//! Flags: `--callback-port N` (the browser-facing port printed in the URL)
//! and `--listen-port M` (where to bind; defaults to N — split so a single
//! machine can host both the client's listener and this one).

use std::io::{BufRead, Read, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err((code, message)) => {
            eprintln!("fake-login: {message}");
            ExitCode::from(code)
        }
    }
}

/// The whole flow; every failure is `(exit code, why)`.
fn run() -> Result<(), (u8, String)> {
    let ports = ports_from(std::env::args().skip(1))?;

    print!("account? ");
    flush()?;
    let account = read_line()?;

    let Some((callback, listen)) = ports else {
        println!("login complete for {account}");
        return Ok(());
    };

    let listener = std::net::TcpListener::bind(("127.0.0.1", listen))
        .map_err(|error| (3, format!("could not bind 127.0.0.1:{listen}: {error}")))?;
    println!("visit http://localhost:{callback}/auth to finish");
    flush()?;

    let (mut connection, _peer) = listener
        .accept()
        .map_err(|error| (4, format!("accept failed: {error}")))?;
    // Read the request head; the content is irrelevant, the hit is the point.
    let mut head = [0_u8; 1024];
    let _ = connection.read(&mut head);
    let body = format!("signed-in as {account}");
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    connection
        .write_all(response.as_bytes())
        .and_then(|()| connection.flush())
        .map_err(|error| (4, format!("could not answer the callback: {error}")))?;
    drop(connection);

    println!("login complete for {account}");
    Ok(())
}

/// `(callback, listen)` from the flags; `None` when no callback was asked
/// for. Unknown flags refuse rather than being ignored.
fn ports_from(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Option<(u16, u16)>, (u8, String)> {
    let mut callback: Option<u16> = None;
    let mut listen: Option<u16> = None;
    let mut stream = arguments.into_iter();
    while let Some(flag) = stream.next() {
        let value = stream
            .next()
            .ok_or_else(|| (2, format!("{flag} needs a value")))?;
        let port: u16 = value
            .parse()
            .map_err(|_| (2, format!("{flag} is not a port: {value}")))?;
        match flag.as_str() {
            "--callback-port" => callback = Some(port),
            "--listen-port" => listen = Some(port),
            other => return Err((2, format!("unknown flag {other}"))),
        }
    }
    match (callback, listen) {
        (Some(callback), listen) => Ok(Some((callback, listen.unwrap_or(callback)))),
        (None, Some(_)) => Err((2, "--listen-port needs --callback-port".to_owned())),
        (None, None) => Ok(None),
    }
}

/// One line of the operator's typing, end-trimmed.
fn read_line() -> Result<String, (u8, String)> {
    let mut line = String::new();
    let read = std::io::stdin()
        .lock()
        .read_line(&mut line)
        .map_err(|error| (4, format!("could not read the account: {error}")))?;
    if read == 0 {
        return Err((2, "stdin closed before an account was typed".to_owned()));
    }
    Ok(line.trim_end().to_owned())
}

fn flush() -> Result<(), (u8, String)> {
    std::io::stdout()
        .flush()
        .map_err(|error| (4, format!("could not flush the prompt: {error}")))
}
