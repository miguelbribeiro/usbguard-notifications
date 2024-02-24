use std::collections::HashMap;
use zbus::export::futures_util::StreamExt;
use zbus::Connection;
use zbus_macros::proxy;

#[proxy(
default_service = "org.usbguard1",
default_path = "/org/usbguard1/Devices",
interface = "org.usbguard.Devices1"
)]
trait UsbGuard1Devices {
    #[zbus(signal)]
    fn device_presence_changed(
        &self,
        id: u32,
        b: u32,
        c: u32,
        d: String,
        e: HashMap<&str, &str>,
    ) -> zbus::Result<()>;
}

async fn watch_device_changes(connection: Connection) -> zbus::Result<()> {
    let usbguard_proxy = UsbGuard1DevicesProxy::new(&connection).await?;
    let mut new_jobs_stream = usbguard_proxy.receive_device_presence_changed().await?;

    while let Some(msg) = new_jobs_stream.next().await {
        let args: DevicePresenceChangedArgs = msg.args().expect("Error parsing message");

        dbg!(args);
    }

    panic!("Stream ended unexpectedly");
}
