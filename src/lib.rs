#![allow(dead_code)]

use crate::ask::*;
use crate::notifications::NotificationManager;
use crate::usbguard::{DeviceEvent, DeviceManager, DevicePresenceUpdate, DeviceTarget};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, instrument, warn};

mod ask;
mod notifications;
mod usbguard;

pub async fn run() {
    let notifications = Arc::new(notifications::dbus::NotificationsDbus::new().await.unwrap());
    let device_manager = Arc::new(
        usbguard::dbus::DbusDeviceManager::new()
            .await
            .expect("should be able to connect to system bus"),
    );

    {
        let device_manager = device_manager.clone();
        tokio::spawn(async move {
            device_manager.watch_device_changes().await.unwrap();
        });
    }

    let mut receiver = device_manager.subscribe_device_changes();
    loop {
        let update = receiver.recv().await.unwrap();

        // only query user if the device was just inserted and its target is "block", otherwise
        // ignore this device
        if update.event() == DeviceEvent::Insert
            && update.target().unwrap_or(DeviceTarget::Allow) == DeviceTarget::Block
        {
            let device_manager = device_manager.clone();
            let notifications = notifications.clone();

            tokio::spawn(async move {
                let _ = query_user(&update, notifications.as_ref(), device_manager.as_ref()).await;
            });
        }
    }
}

#[instrument(skip(notifications, devices))]
async fn query_user(
    update: &DevicePresenceUpdate,
    notifications: &impl NotificationManager,
    devices: &impl DeviceManager,
) -> anyhow::Result<()> {
    let allow = match ask_allow_device(notifications, devices, update).await {
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
        let result = devices
            .apply_device_target(update.device_id(), DeviceTarget::Allow)
            .await
            .inspect_err(|error| error!("Couldn't apply new target to device: {}", error));

        if let Err(error) = result {
            let body = format!(
                "Failed to apply target to device \"{}\", check the logs for more information",
                update.name()
            );

            let _ = notifications
                .notify(
                    "Failed to apply target",
                    &body,
                    &[],
                    &HashMap::default(),
                    None,
                )
                .await;

            return Err(error);
        }
    }

    Ok(())
}
