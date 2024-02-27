use crate::usbguard::{DevicePresenceUpdate, DeviceTarget};
use anyhow::bail;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio::time::Instant;
use zbus::{zvariant::Value, Connection};

const NOTIFICATION_ACTION_CHANNEL_SIZE: usize = 64;
const NOTIFICATION_ACTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct NotificationAction {
    notification_id: u32,
    action: Arc<String>,
}

pub struct Notifications {
    connection: Connection,
    sender: Sender<NotificationAction>,
}

impl Notifications {
    pub async fn new() -> anyhow::Result<Self> {
        let connection = Connection::session().await?;
        let (sender, _) = broadcast::channel(NOTIFICATION_ACTION_CHANNEL_SIZE);
        
        // TODO unsure if the task should be spawned here
        let sender_clone = sender.clone();
        tokio::spawn(async move { 
           Self::watcher(sender_clone).await;
        });

        Ok(Notifications { connection, sender })
    }

    async fn watcher(sender: Sender<NotificationAction>) {
        todo!()
    }

    pub async fn ask_target_for_device_update(
        &self,
        update: &DevicePresenceUpdate,
    ) -> anyhow::Result<DeviceTarget> {
        // subscription should be made before sending the notification to ensure no messages are missed
        let mut receiver = self.sender.subscribe();

        let notification_id: u32 = self.send_notification(update.name()).await?;

        let mut time_remaining = NOTIFICATION_ACTION_TIMEOUT;
        loop {
            let start = Instant::now();

            // this returns from the function if timeout was exceeded
            let notification_action = tokio::time::timeout(time_remaining, receiver.recv())
                .await?
                .expect("this receiver shouldn't be falling behind");

            if notification_action.notification_id == notification_id {
                return Ok(DeviceTarget::Block);
            } else {
                let elapsed = start.elapsed();
                match time_remaining.checked_sub(elapsed) {
                    Some(value) => time_remaining = value,
                    None => bail!("Timeout exceeded"),
                }
            }
        }
    }

    async fn send_notification(&self, device_name: &str) -> anyhow::Result<u32> {
        self.connection
            .call_method(
                Some("org.freedesktop.Notifications"),
                "/org/freedesktop/Notifications",
                Some("org.freedesktop.Notifications"),
                "Notify",
                &(
                    "usbguard-notifications",
                    0u32,
                    "dialog-information",
                    "New device detected",
                    format!("Allow device \"{}\"?", device_name),
                    vec!["allow", "Allow", "block", "Block"],
                    HashMap::<&str, &Value>::new(),
                    Duration::from_secs(10).as_millis() as u32,
                ),
            )
            .await?
            .body()
            .deserialize()
            .map_err(|error| error.into())
    }
}
