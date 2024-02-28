use std::sync::Arc;
use crate::usbguard::{DeviceManager, DevicePresenceUpdate};
use tokio::sync::mpsc::Receiver;

mod notifications;
mod usbguard;
mod usbguard_dbus;

const CHANNEL_BUFFER_SIZE: usize = 64;

pub async fn run() {
    let manager = Arc::new(usbguard_dbus::DbusDeviceManager::new()
        .await
        .expect("should be able to connect to system bus"));

    let notifications = notifications::Notifications::new().await.unwrap();

    let mut receiver = subscribe_device_updates(manager.clone()).await;
    loop {
        let update = receiver.recv().await.unwrap();
        let target = notifications
            .ask_target_for_device_update(&update)
            .await
            .unwrap(); // TODO don't unwrap

        manager.apply_device_target(update.device_id(), target).await.unwrap();
    }
}

async fn subscribe_device_updates<M: DeviceManager + Send + Sync + 'static>(
    manager: Arc<M>,
) -> Receiver<DevicePresenceUpdate> {
    let (sender, receiver) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);

    tokio::spawn(async move {
        manager.watch_device_changes(sender).await.unwrap();
    });

    receiver
}
