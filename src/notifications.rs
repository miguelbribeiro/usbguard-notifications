//! Low level functionality for sending and handling notifications through D-Bus.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use anyhow::anyhow;
use tracing::debug;
use zbus::export::futures_util::StreamExt;
use zbus::Connection;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotificationId(u32);

#[zbus::proxy(
    default_service = "org.freedesktop.Notifications",
    default_path = "/org/freedesktop/Notifications",
    interface = "org.freedesktop.Notifications"
)]
trait Notifications {
    fn notify(
        &self,
        app_name: &str,
        replaces_id: u32,
        app_icon: &str,
        summary: &str,
        body: &str,
        actions: &[&str],
        hints: HashMap<&str, &zvariant::Value<'_>>,
        expire_timeout: i32,
    ) -> zbus::Result<u32>;

    fn close_notification(&self, id: u32) -> zbus::Result<()>;

    fn get_capabilities(&self) -> zbus::Result<Vec<String>>;

    #[zbus(signal)]
    fn action_invoked(&self, notification_id: u32, action: String) -> zbus::Result<()>;

    #[zbus(signal)]
    fn notification_closed(&self, notification_id: u32, reason: u32) -> zbus::Result<()>;
}

/// Represents a set of actions in the notification prompt that are shown to
/// the user.
pub trait AsActions: Display {
    fn as_actions() -> impl Iterator<Item = Self>;
}

pub enum DecisionResult<R> {
    Decision(R),
    Closed,
}

/// The text displayed to the user and the value it yields for an action in a
/// notification.
struct NotificationAction<R> {
    display: String,
    value: R,
}

#[derive(Clone)]
pub struct NotificationManager {
    proxy: NotificationsProxy<'static>,
    app_name: Arc<String>, // TODO don't use arc
}

impl NotificationManager {
    pub async fn new(app_name: &str) -> zbus::Result<Self> {
        let connection = Connection::session().await?;
        let proxy = NotificationsProxy::new(&connection).await?;

        Ok(Self {
            proxy,
            app_name: Arc::new(app_name.to_string()),
        })
    }

    pub async fn has_capability_actions(&self) -> zbus::Result<bool> {
        let capabilities = self.proxy.get_capabilities().await?;
        Ok(capabilities.iter().any(|s| s == "actions"))
    }

    async fn notify_internal(
        &self,
        summary: &str,
        body: &str,
        actions: Option<&[&str]>,
    ) -> zbus::Result<u32> {
        self.proxy
            .notify(
                self.app_name.as_str(),
                0,
                "",
                summary,
                body,
                actions.unwrap_or(&[]),
                HashMap::default(),
                -1,
            )
            .await
    }

    pub async fn notify(&self, summary: &str, body: &str) -> anyhow::Result<NotificationId> {
        self.notify_internal(summary, body, None)
            .await
            .map(|v| NotificationId(v))
            .map_err(|err| err.into())
    }

    pub async fn close(&self, id: NotificationId) -> anyhow::Result<()> {
        self.proxy
            .close_notification(id.0)
            .await
            .map_err(|err| err.into())
    }

    /// Creates a notification containing a set of actions, and blocks until
    /// either the user selects an action, or the notifications closes or is
    /// closed.
    pub async fn decision<'a, R: AsActions>(
        &'a self,
        summary: &str,
        body: &str,
        mut cancel: tokio::sync::oneshot::Receiver<()>,
    ) -> anyhow::Result<DecisionResult<R>> {
        // generate a string id for each action
        let mut action_mapping: HashMap<String, NotificationAction<R>> = R::as_actions()
            .enumerate()
            .map(|(id, value)| {
                (
                    id.to_string(),
                    NotificationAction {
                        display: value.to_string(),
                        value,
                    },
                )
            })
            .collect();

        // TODO move this to the method
        let action_mapping_raw: Vec<&str> = action_mapping
            .iter()
            .map(|(id, action)| [id.as_str(), action.display.as_str()])
            .flatten()
            .collect();

        let (mut stream_action_invoked, mut stream_notification_closed) = tokio::try_join!(
            self.proxy.receive_action_invoked(),
            self.proxy.receive_notification_closed(),
        )?;

        let notification_id = self
            .notify_internal(summary, body, Some(action_mapping_raw.as_slice()))
            .await?;

        debug!("notification sent with id {}, waiting for action", notification_id);

        loop {
            tokio::select! {
                message = stream_action_invoked.next() => {
                    if let Some(message) = message {
                        let args = message.args()?;
                        if args.notification_id == notification_id {
                            debug!("action {} invoked for notification {}", args.action, notification_id);

                            return Ok(action_mapping
                                .remove(&args.action)
                                .ok_or_else(|| anyhow!("returned action is unknown"))
                                .map(|v| DecisionResult::Decision(v.value))?);
                        }
                    } else {
                        return Err(anyhow!("ActionInvoked stream closed unexpectely"));
                    }
                }
                message = stream_notification_closed.next() => {
                    if let Some(message) = message {
                        let args = message.args()?;
                        if args.notification_id == notification_id {
                            debug!("notification {} closed", notification_id);
                            return Ok(DecisionResult::Closed);
                        }
                    } else {
                        return Err(anyhow!("NotificationClosed stream closed unexpectely"));
                    }
                }
                _ = &mut cancel => {
                    debug!("cancel received, closing notification {}", notification_id);
                    self.close(NotificationId(notification_id)).await?;
                    return Ok(DecisionResult::Closed);
                }
            }
        }
    }
}
