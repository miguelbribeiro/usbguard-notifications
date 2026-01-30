#![allow(dead_code)]

use crate::ask::*;
use crate::notifications::NotificationManager;
use crate::usbguard::dbus::DbusDeviceManager;
use crate::usbguard::{DeviceEvent, DeviceManager, DeviceTarget, DeviceUpdate};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tracing::{error, info};

mod ask;
mod notifications;
mod usbguard;

pub async fn run() -> anyhow::Result<()> {
    let notifications = NotificationManager::new("").await?;
    let devices: DbusDeviceManager = todo!();

    let app = App::new(notifications, devices);
    app.run().await?;
}

// TODO merge into App
struct AppInner {
    notifications: NotificationManager,
    devices: DbusDeviceManager,
}

#[derive(Clone)]
struct App {
    inner: Arc<AppInner>,
}

impl App {
    fn new(notifications: NotificationManager, devices: DbusDeviceManager) -> Self {
        Self {
            inner: Arc::new(AppInner {
                notifications,
                devices,
            }),
        }
    }

    async fn try_allow_device(&self, update: &DeviceUpdate) {
        let allow_result = self
            .inner
            .devices
            .apply_device_target(update.device().device_id(), DeviceTarget::Allow)
            .await;

        if let Err(err) = allow_result {
            error!(
                "failed to apply target to device {}: {}",
                update.name(),
                err
            );

            let error_notification_body = format!(
                "Failed to apply target to device \"{}\", check the logs for more information",
                update.name()
            );
            let error_notification = self
                .inner
                .notifications
                .notify("Failed to allow device", &error_notification_body)
                .await;
            if let Err(error) = error_notification {
                error!("failed to send error notification: {}", error);
            }
        }
    }

    async fn handle_blocked_device(&self, update: &DeviceUpdate) {
        let (decision_cancel_sender, decicion_cancel_receiver) = tokio::sync::oneshot::channel();
        let mut decision = std::pin::pin!(ask(
            &self.inner.notifications,
            update,
            decicion_cancel_receiver
        ));

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
            // TODO
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                decision_cancel_sender.send(()).expect("channel should be open");

                // this should finish almost immediately after cancelling it
                if let Err(err) = decision.await {
                    error!("failed to close notification: {}", err);
                }

                return;
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
    }

    async fn run(&self) -> anyhow::Result<()> {
        let mut receiver = self.inner.devices.subscribe_device_changes();

        loop {
            let update = receiver.recv().await?;
            let is_block_target = update
                .device()
                .target()
                .map(|target| target == DeviceTarget::Block)
                .unwrap_or(false);

            // only query user if the device was just inserted and its current target is "block"
            if update.event() == DeviceEvent::Insert && is_block_target {
                let app = self.clone();

                tokio::spawn(async move {
                    let _ = app.handle_blocked_device(&update).await;
                });
            }
        }
    }
}
