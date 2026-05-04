#![allow(dead_code)]

use crate::notifications::NotificationManager;
use crate::usbguard::{DeviceEvent, DeviceId, DeviceTarget, DeviceUpdate};
use crate::{ask::*, usbguard::DeviceManager};
use tokio::select;
use tokio_stream::StreamExt;
use tracing::{error, info};

mod ask;
mod notifications;
mod usbguard;

pub async fn run() -> anyhow::Result<()> {
    let notifications = NotificationManager::new("usbguard-notifications").await?;
    let device_manager = DeviceManager::new().await?;

    if !notifications.has_capability_actions().await? {
        let _ = notifications
            .notify(
                "Failed to start",
                "This notification server does not support actions.",
            )
            .await?;

        return Err(anyhow::anyhow!("notification server does not support actions"));
    }

    let app = App::new(notifications, device_manager);
    app.run().await?;

    unreachable!();
}

struct App {
    notifications: NotificationManager,
    devices: DeviceManager,
}

impl App {
    fn new(notifications: NotificationManager, devices: DeviceManager) -> Self {
        Self {
            notifications,
            devices,
        }
    }

    async fn try_allow_device(&self, update: &DeviceUpdate) {
        let allow_result = self
            .devices
            .apply_device_policy(update.id, DeviceTarget::Allow)
            .await;

        if let Err(err) = allow_result {
            let device_name = update.name.as_deref().unwrap_or("(unknown)");
            error!("failed to apply target to device {}: {}", device_name, err);

            let error_notification_body = format!(
                "Failed to apply target to device \"{}\", check the logs for more information",
                device_name
            );
            let error_notification = self
                .notifications
                .notify("Failed to allow device", &error_notification_body)
                .await;
            if let Err(error) = error_notification {
                error!("failed to send error notification: {}", error);
            }
        }
    }

    async fn get_device_update_stream_wrapper(
        &self,
    ) -> anyhow::Result<impl futures::Stream<Item = DeviceUpdate>> {
        Ok(self
            .devices
            .get_device_update_stream()
            .await?
            .filter_map(|u| match u {
                Ok(u) => Some(u),
                Err(e) => {
                    error!("failed to parse device event: {}", e);
                    None
                }
            }))
    }

    async fn wait_device_removal(&self, device_id: DeviceId) -> anyhow::Result<()> {
        let mut device_stream = self.get_device_update_stream_wrapper().await?;

        while let Some(update) = device_stream.next().await {
            if update.id == device_id && update.event == DeviceEvent::Remove {
                break;
            }
        }

        Ok(())
    }

    async fn handle_blocked_device(&self, update: &DeviceUpdate) -> anyhow::Result<()> {
        let (decision_cancel_sender, decicion_cancel_receiver) = tokio::sync::oneshot::channel();
        let mut decision =
            std::pin::pin!(ask(&self.notifications, update, decicion_cancel_receiver));

        let decision_result = select! {
            result = &mut decision => {
                match result {
                    Ok(decision) => match decision {
                        notifications::DecisionResult::Decision(decision) => decision,
                        notifications::DecisionResult::Closed => AllowIgnoreQuestion::Ignore,
                    },
                    Err(_) => todo!(),
                }
            }
            result = self.wait_device_removal(update.id) => {
                decision_cancel_sender.send(()).expect("channel should be open");

                // this should finish almost immediately after cancelling it
                decision.await?;

                return result;
            }
        };

        match decision_result {
            AllowIgnoreQuestion::Allow => {
                self.try_allow_device(update).await;
            }
            AllowIgnoreQuestion::Ignore => {
                info!("ignoring device");
            }
        };

        Ok(())
    }

    async fn run(&self) -> anyhow::Result<()> {
        let mut device_stream = self.get_device_update_stream_wrapper().await?;

        while let Some(update) = device_stream.next().await {
            // only query user if the device was just inserted and its current target is "block"
            if update.event == DeviceEvent::Insert && update.target == DeviceTarget::Block {
                if let Err(e) = self.handle_blocked_device(&update).await {
                    error!("failed to handle blocked device: {}", e);
                };
            }
        }

        unreachable!();
    }
}
