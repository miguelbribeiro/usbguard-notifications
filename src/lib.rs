#![allow(dead_code)]

use crate::notifications::Notifications;
use crate::notifications::TimeoutError;
use crate::usbguard::{DeviceEvent, DeviceManager, DevicePresenceUpdate, DeviceTarget};
use std::sync::Arc;
use tracing::{debug, error, instrument, warn};

mod notifications;
mod usbguard;
mod usbguard_dbus;

const CHANNEL_BUFFER_SIZE: usize = 64;

pub async fn run() {
    let notifications = Arc::new(Notifications::new().await.unwrap());
    let device_manager = Arc::new(
        usbguard_dbus::DbusDeviceManager::new()
            .await
            .expect("should be able to connect to system bus"),
    );

    {
        let device_manager = device_manager.clone();
        tokio::spawn(async move {
            device_manager.watch_device_changes().await.unwrap();
        });
    }

    let mut receiver = device_manager.subscribe_device_changes().unwrap();
    loop {
        let update = receiver.recv().await.unwrap();

        // only query user if the device was just inserted and its target is "block", otherwise
        // ignore this device
        match (
            update.event(),
            update.target().unwrap_or(DeviceTarget::Allow),
        ) {
            (DeviceEvent::Insert, DeviceTarget::Block) => {}
            _ => continue,
        };

        // clone Arcs
        let device_manager = device_manager.clone();
        let notifications = notifications.clone();

        tokio::spawn(async move {
            let _ = query_user(&update, &notifications, device_manager.as_ref()).await;
        });
    }
}

#[instrument(skip(manager, notifications))]
async fn query_user(
    update: &DevicePresenceUpdate,
    notifications: &Notifications,
    manager: &impl DeviceManager,
) -> anyhow::Result<()> {
    let allow = match notifications.ask_allow_device(update).await {
        Ok(target) => target,
        Err(error) => {
            match error.downcast_ref::<TimeoutError>() {
                Some(_) => {
                    debug!("Time limit for receiving an action from the user has been exceeded")
                }
                None => warn!(
                    "Error while sending notification or getting its action back: {}",
                    &error
                ),
            };

            return Err(error);
        }
    };

    debug!("Notification result: should allow: {}", allow);

    if allow {
        manager
            .apply_device_target(update.device_id(), DeviceTarget::Allow)
            .await
            .inspect_err(|error| error!("Couldn't apply new target to device: {}", error))?;
    }

    Ok(())
}
