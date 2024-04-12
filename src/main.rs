use anyhow::anyhow;
use libc::{geteuid, getuid, uid_t};
use usbguard_notifications::run;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if is_root() {
        return Err(anyhow!("this program should not be run as root"));
    }

    tracing_subscriber::fmt::init();

    run().await?;
    unreachable!()
}

fn is_root() -> bool {
    const UID_ROOT: uid_t = 0;

    // according to the POSIX manual, both these functions shall always be successful
    let uid_process = unsafe { getuid() };
    let euid_process = unsafe { geteuid() };

    uid_process == UID_ROOT || euid_process == UID_ROOT
}
