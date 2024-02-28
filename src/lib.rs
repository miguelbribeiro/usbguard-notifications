use crate::usbguard::{DeviceEvent, DeviceManager, DevicePresenceUpdate, DeviceTarget};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

mod notifications;
mod usbguard;
mod usbguard_dbus;

const CHANNEL_BUFFER_SIZE: usize = 64;

pub async fn run() {
    let device_manager = Arc::new(
        usbguard_dbus::DbusDeviceManager::new()
            .await
            .expect("should be able to connect to system bus"),
    );

    let notifications = notifications::Notifications::new().await.unwrap();

    let mut receiver = subscribe_device_updates(device_manager.clone()).await;
    loop {
        let update = receiver.recv().await.unwrap();

        // only query user if the device was just inserted and its target is "block", otherwise
        // ignore this device
        match (update.event(), update.target().unwrap_or(DeviceTarget::Block)) {
            (DeviceEvent::Insert, DeviceTarget::Block) => {},
            _ => { continue; }
        };

        let target = notifications
            .ask_target_for_device_update(&update)
            .await
            .unwrap(); // TODO don't unwrap

        device_manager
            .apply_device_target(update.device_id(), target)
            .await
            .unwrap();
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
