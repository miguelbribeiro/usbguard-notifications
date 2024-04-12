use std::process::ExitCode;
use libc::{geteuid, getuid, uid_t};
use usbguard_notifications::run;

#[tokio::main]
async fn main() -> ExitCode {
    if is_root() {
        eprintln!("this program should not be run as root");
        return ExitCode::from(1);
    }
    
    tracing_subscriber::fmt::init();

    run().await;
    unreachable!();
}

fn is_root() -> bool {
    const UID_ROOT: uid_t = 0;

    // according to the POSIX manual, both these functions shall always be successful
    let uid_process = unsafe { getuid() };
    let euid_process = unsafe { geteuid() };
    
    uid_process == UID_ROOT || euid_process == UID_ROOT
}
