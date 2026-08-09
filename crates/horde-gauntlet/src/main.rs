//! The `gauntlet` binary. All behaviour lives in the library, where it is
//! testable; this is the shell that hands the process an exit code.

use std::process::ExitCode;

fn main() -> ExitCode {
    horde_gauntlet::cli::main()
}
