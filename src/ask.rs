use anyhow::anyhow;
use core::panic;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::info;

use crate::notifications::{notify_action_device, NotificationManager, NotificationSignal};
use crate::usbguard::{DeviceEvent, DeviceManager, DevicePresenceUpdate};

#[derive(Debug)]
pub struct TimeoutError;

impl Display for TimeoutError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "timeout has been exceeded")
    }
}

impl std::error::Error for TimeoutError {}

#[tracing::instrument(skip(notifications, devices))]
pub async fn ask_allow_device(
    notifications: &impl NotificationManager,
    devices: &impl DeviceManager,
    update: &DevicePresenceUpdate,
) -> anyhow::Result<bool> {
    // subscriptions should be made before sending the notification to ensure no messages are missed
    let mut receiver_notifications = notifications.subscribe();
    let mut receiver_devices = devices.subscribe_device_changes();

    let notification_id: u32 = notify_action_device(notifications, update.name()).await?;

    // after a notification is sent, 1 of 3 things can happen:
    // 1. the user invokes an action of the notification
    // 2. the notification expires or the user closes it
    // 3. the USB device associated with the notification is removed

    tokio::select! {
        signal = next_signal_for_notification(notification_id, &mut receiver_notifications) => {
            match signal.as_ref() {
                NotificationSignal::ActionInvoked(signal) => Ok(signal.is_allow()),
                NotificationSignal::Closed(closed) => match closed.reason {
                    1 => Err(TimeoutError.into()),
                    _ => Err(anyhow!("notification closed")),
                },
                _ => panic!("this signal type shouldn't have reached this point"),
            }
        },
        () = wait_removal(&mut receiver_devices, update.device_id()) => {
            let _ = notifications.close(notification_id).await;
            info!("Device was removed, closing notification {}", notification_id);
            Err(anyhow!("device was removed while waiting for an action"))
        }
    }
}

/// Waits until the specified device is removed.
async fn wait_removal(receiver: &mut Receiver<Arc<DevicePresenceUpdate>>, device_id: u32) {
    loop {
        let update = receiver.recv().await.unwrap();

        if update.device_id() == device_id && update.event() == DeviceEvent::Remove {
            return;
        }
    }
}

/// Gets the next signal for the notification with the provided ID.
async fn next_signal_for_notification(
    notification_id: u32,
    recv: &mut Receiver<Arc<NotificationSignal>>,
) -> Arc<NotificationSignal> {
    loop {
        let signal = recv.recv().await.unwrap();

        let signal_notification_id = match signal.as_ref() {
            NotificationSignal::ActionInvoked(signal) => signal.notification_id,
            NotificationSignal::Closed(signal) => signal.notification_id,
            _ => continue,
        };

        if signal_notification_id == notification_id {
            return signal;
        }
    }
}
