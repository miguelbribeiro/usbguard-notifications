//! This module contains functionality for sending and handling notifications.
//!
//! The interface in this file models the one in
//! https://specifications.freedesktop.org/notification-spec/notification-spec-latest.html.

pub mod dbus;
mod ask;

pub use ask::*;

use std::collections::HashMap;
use std::time::Duration;
use zvariant::Value;

const NOTIFICATION_APP_NAME: &str = "usbguard-notifications";

#[derive(Debug, Clone)]
pub enum NotificationSignal {
    ActionInvoked(NotificationActionInvoked),
    Closed(NotificationClosed),
}

#[derive(Debug, Clone)]
pub struct NotificationActionInvoked {
    notification_id: u32,
    action: String,
}

#[derive(Debug, Clone, Copy)]
pub struct NotificationClosed {
    notification_id: u32,
    reason: u32,
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
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<NotificationSignal>;
}
