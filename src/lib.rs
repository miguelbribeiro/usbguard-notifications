#![allow(dead_code)]

use crate::ask::*;
use crate::notifications::NotificationManager;
use crate::usbguard::{DeviceEvent, DeviceManager, DeviceTarget, DeviceUpdate, ListDevicesFilter};
use std::collections::HashMap;
use std::sync::Arc;
use anyhow::anyhow;
use tracing::{debug, error, instrument, warn};
use crate::usbguard::dbus::DbusDeviceManager;

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

    let devices = initialize_device_manager().await.unwrap();

    let mut receiver = devices.subscribe_device_changes();
    loop {
        let update = receiver.recv().await.unwrap();

        // only query user if the device was just inserted and its target is "block", otherwise
        // ignore this device
        if update.event() == DeviceEvent::Insert
            && update.device().target().unwrap_or(DeviceTarget::Allow) == DeviceTarget::Block
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

async fn initialize_device_manager() -> anyhow::Result<Arc<DbusDeviceManager>> {
    let device_manager = DbusDeviceManager::new().await.unwrap();
    let device_manager = Arc::new(device_manager);
    
    // only for checking if the dbus interface is available, the returned data isn't relevant
    if let Err(error) = device_manager.list_devices(ListDevicesFilter::None).await {
        return Err(anyhow!("failed to access USBGuard dbus service: {}", error));
    }
    
    // initialize signal listener
    {
        let devices = device_manager.clone();
        tokio::spawn(async move {
            devices.watch_device_changes().await.unwrap();
        });
    }
    
    Ok(device_manager)
}

/// Handles a device by prompting the user and applying the new target if allowed.
#[instrument(skip(notification_manager, device_manager))]
async fn handle_blocked_device(
    notification_manager: &impl NotificationManager,
    device_manager: &impl DeviceManager,
    update: &DeviceUpdate,
) -> anyhow::Result<()> {
    let allow =
        match prompt_user_or_wait_removal(notification_manager, device_manager, update).await {
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
            .apply_device_target(update.device().device_id(), DeviceTarget::Allow)
            .await
            .inspect_err(|error| error!("Couldn't apply new target to device: {}", error));

        if let Err(error) = result {
            let body = format!(
                "Failed to apply target to device \"{}\", check the logs for more information",
                update.name()
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
