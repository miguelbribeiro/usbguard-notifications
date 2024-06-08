use usbguard_notifications::run;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    run().await?;
    unreachable!()
}
