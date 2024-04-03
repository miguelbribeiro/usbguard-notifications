use anyhow::anyhow;
use core::panic;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::info;

use crate::notifications::{notify_action_device, NotificationManager, NotificationSignal};
use crate::usbguard::{Device, DeviceEvent, DeviceManager};

#[derive(Debug)]
pub struct TimeoutError;

impl Display for TimeoutError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "timeout has been exceeded")
    }
}

impl std::error::Error for TimeoutError {}

/// Prompts the user to allow or ignore a blocked device.
/// Returns once the user has made a decision or the device in question has been removed.
#[tracing::instrument(skip(notification_manager, device_manager))]
pub async fn prompt_user_or_wait_removal(
    notification_manager: &impl NotificationManager,
    device_manager: &impl DeviceManager,
    device: &Device,
) -> anyhow::Result<bool> {
    // subscriptions should be made before sending the notification to ensure no messages are missed
    let mut receiver_notifications = notification_manager.subscribe();
    let mut receiver_devices = device_manager.subscribe_device_changes();

    let notification_id: u32 = notify_action_device(notification_manager, device.name()).await?;

    // after a notification is sent, 1 of 3 things can happen:
    // 1. the user invokes an action of the notification
    // 2. the notification expires/the user closes it without invoking an action
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
        () = wait_removal(&mut receiver_devices, device.device_id()) => {
            let _ = notification_manager.close(notification_id).await;
            info!("Device was removed, closing notification {}", notification_id);
            Err(anyhow!("device was removed while waiting for an action"))
        }
    }
}

/// Waits until the specified device is removed.
async fn wait_removal(receiver: &mut Receiver<Arc<Device>>, device_id: u32) {
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
