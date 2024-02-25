use crate::usbguard::{DeviceManager, DevicePresenceUpdate};
use tokio::sync::mpsc::Receiver;

mod notifications;
mod usbguard;
mod usbguard_dbus;

const CHANNEL_BUFFER_SIZE: usize = 64;

pub async fn run() {
    let manager = usbguard_dbus::DbusDeviceManager::new()
        .await
        .expect("should be able to connect to system bus");
    let mut receiver = subscribe_device_updates(manager).await;

    notifications::send_notification("gm").await.unwrap();

    loop {
        let update = receiver.recv().await.unwrap();
        println!("{}", update.rule());
        dbg!(update);
    }
}

async fn subscribe_device_updates(
    manager: impl DeviceManager + Send + 'static,
) -> Receiver<DevicePresenceUpdate> {
    let (sender, receiver) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);

    tokio::spawn(async move {
        manager.watch_device_changes(sender).await.unwrap();
    });

    receiver
}
