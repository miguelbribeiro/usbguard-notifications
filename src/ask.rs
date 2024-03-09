use anyhow::anyhow;
use std::fmt::{Display, Formatter};
use tokio::sync::broadcast::Receiver;

use crate::notifications::{notify_action_device, NotificationManager, NotificationSignal};
use crate::usbguard::DevicePresenceUpdate;

#[derive(Debug)]
pub struct TimeoutError;

impl Display for TimeoutError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "timeout has been exceeded")
    }
}

impl std::error::Error for TimeoutError {}

pub async fn ask_allow_device(
    notifications: &impl NotificationManager,
    update: &DevicePresenceUpdate,
) -> anyhow::Result<bool> {
    // subscription should be made before sending the notification to ensure no messages are missed
    let mut receiver: Receiver<NotificationSignal> = notifications.subscribe();

    let notification_id: u32 = notify_action_device(notifications, update.name()).await?;

    // after a notification is sent, 1 of 3 things can happen:
    // 1. the user invokes an action of the notification
    // 2. the notification expires or the user closes it
    // 3. the USB device associated with the notification is removed

    // TODO check if the USB device is removed
    tokio::select! {
        signal = get_next_signal(notification_id, &mut receiver) => {
            match signal {
                NotificationSignal::ActionInvoked(signal) => Ok(signal.is_allow()),
                NotificationSignal::Closed(closed) => match closed.reason {
                    1 => Err(TimeoutError.into()),
                    _ => Err(anyhow!("notification closed")),
                },
            }
        },
    }
}

/// Gets the next signal for the notification with the provided ID.
async fn get_next_signal(
    notification_id: u32,
    recv: &mut Receiver<NotificationSignal>,
) -> NotificationSignal {
    loop {
        let signal = recv.recv().await.unwrap();

        let signal_notification_id = match &signal {
            NotificationSignal::ActionInvoked(signal) => signal.notification_id,
            NotificationSignal::Closed(signal) => signal.notification_id,
        };

        if signal_notification_id == notification_id {
            return signal;
        }
    }
}
