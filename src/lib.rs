#![allow(dead_code)]

use crate::ask::*;
use crate::notifications::NotificationManager;
use crate::usbguard::{Device, DeviceEvent, DeviceManager, DeviceTarget};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, instrument, warn};

mod ask;
mod notifications;
mod usbguard;

pub async fn run() {
    let notifications = notifications::dbus::NotificationsDbus::new().await.unwrap();
    let notifications = Arc::new(notifications);
    {
        let notifications = notifications.clone();
        tokio::spawn(async move {
            notifications.watch().await.unwrap();
        });
    }

    let devices = usbguard::dbus::DbusDeviceManager::new().await.unwrap();
    let devices = Arc::new(devices);
    {
        let devices = devices.clone();
        tokio::spawn(async move {
            devices.watch_device_changes().await.unwrap();
        });
    }

    let mut receiver = devices.subscribe_device_changes();
    loop {
        let update = receiver.recv().await.unwrap();

        // only query user if the device was just inserted and its target is "block", otherwise
        // ignore this device
        if update.event() == DeviceEvent::Insert
            && update.target().unwrap_or(DeviceTarget::Allow) == DeviceTarget::Block
        {
            let device_manager = devices.clone();
            let notifications = notifications.clone();

            tokio::spawn(async move {
                let _ =
                    handle_blocked_device(notifications.as_ref(), device_manager.as_ref(), &update)
                        .await;
            });
        }
    }
}

/// Handles a device by prompting the user and applying the new target if allowed.
#[instrument(skip(notification_manager, device_manager))]
async fn handle_blocked_device(
    notification_manager: &impl NotificationManager,
    device_manager: &impl DeviceManager,
    device: &Device,
) -> anyhow::Result<()> {
    let allow =
        match prompt_user_or_wait_removal(notification_manager, device_manager, device).await {
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
        let result = device_manager
            .apply_device_target(device.device_id(), DeviceTarget::Allow)
            .await
            .inspect_err(|error| error!("Couldn't apply new target to device: {}", error));

        if let Err(error) = result {
            let body = format!(
                "Failed to apply target to device \"{}\", check the logs for more information",
                device.name()
            );

            let _ = notification_manager
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
