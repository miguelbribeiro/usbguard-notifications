//! This module contains functionality for sending and handling notifications.
//!
//! The interface in this file models the one in
//! https://specifications.freedesktop.org/notification-spec/notification-spec-latest.html.

pub mod dbus;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use zvariant::Value;

const NOTIFICATION_APP_NAME: &str = "usbguard-notifications";

const ACTION_ALLOW: (&str, &str) = ("allow", "Allow");
const ACTION_IGNORE: (&str, &str) = ("ignore", "Ignore");

#[derive(Debug, Clone)]
pub enum NotificationSignal {
    ActionInvoked(NotificationActionInvoked),
    Closed(NotificationClosed),
    ActivationToken,
}

#[derive(Debug, Clone)]
pub struct NotificationActionInvoked {
    pub notification_id: u32,
    pub action: String,
}

impl NotificationActionInvoked {
    pub fn is_allow(&self) -> bool {
        self.action.as_str() == ACTION_ALLOW.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NotificationClosed {
    pub notification_id: u32,
    pub reason: u32,
}

pub trait NotificationManager {
    /// Sends a notification with the provided parameters.
    ///
    /// Returns the notification ID.
    fn notify(
        &self,
        summary: &str,
        body: &str,
        actions: &[&str],
        hints: &HashMap<&str, Value<'_>>,
        timeout: Option<Duration>,
    ) -> impl std::future::Future<Output = anyhow::Result<u32>> + Send;

    /// Closes the notification with the provided ID.
    ///
    /// Returns an error if a notification with that ID does not exist.
    fn close(
        &self,
        notification_id: u32,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;

    /// Creates a Receiver that will receive signals.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Arc<NotificationSignal>>;
}
/// Sends the notification with actions so the user can choose to allow or not
/// the new device.
///
/// Returns the notification ID.
pub async fn notify_action_device(
    notifications: &impl NotificationManager,
    device_name: &str,
) -> anyhow::Result<u32> {
    let mut hints = HashMap::new();
    hints.insert("urgency", Value::U8(2)); // set urgency to critical

    let actions = vec![
        ACTION_IGNORE.0,
        ACTION_IGNORE.1,
        ACTION_ALLOW.0,
        ACTION_ALLOW.1,
    ];

    notifications
        .notify(
            "New blocked USB device detected",
            format!("Allow device \"{}\"?", device_name).as_str(),
            actions.as_slice(),
            &hints,
            None,
        )
        .await
}
