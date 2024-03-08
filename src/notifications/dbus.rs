//! Low level functionality for sending and handling notifications through D-Bus.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::error;
use zbus::export::ordered_stream::OrderedStreamExt;
use zbus::{zvariant::Value, Connection, Message, Proxy};

use crate::notifications::*;

const DBUS_NOTIFICATIONS_DESTINATION: &str = "org.freedesktop.Notifications";
const DBUS_NOTIFICATIONS_OBJECT: &str = "/org/freedesktop/Notifications";
const DBUS_NOTIFICATIONS_INTERFACE: &str = "org.freedesktop.Notifications";
const DBUS_NOTIFICATIONS_INTERFACE_NOTIFY: &str = "Notify";
const DBUS_NOTIFICATIONS_INTERFACE_CLOSE: &str = "CloseNotification";
const DBUS_NOTIFICATIONS_INTERFACE_ACTION_INVOKED: &str = "ActionInvoked";
const DBUS_NOTIFICATIONS_INTERFACE_CLOSED: &str = "NotificationClosed";
const DBUS_NOTIFICATIONS_INTERFACE_ACTIVATION_TOKEN: &str = "ActivationToken";

const CHANNEL_SIGNAL_SIZE: usize = 64;

// TODO avoid duplicating all this code
impl TryFrom<&Message> for NotificationSignal {
    type Error = anyhow::Error;

    fn try_from(message: &Message) -> Result<Self, Self::Error> {
        let hdr = message.header();
        let message_type = message.message_type();
        let interface = hdr.interface();
        let member = hdr.member();
        let interface = interface.as_ref().map(|i| i.as_str());
        let member = member.as_ref().map(|m| m.as_str());

        if message_type != zbus::message::Type::Signal
            || interface.unwrap_or_default() != DBUS_NOTIFICATIONS_INTERFACE
        {
            return Err(anyhow!("wrong message type or interface"));
        }

        match member {
            Some(DBUS_NOTIFICATIONS_INTERFACE_ACTION_INVOKED) => message
                .body()
                .deserialize::<(u32, String)>()
                .map(|value| {
                    NotificationSignal::ActionInvoked(NotificationActionInvoked {
                        notification_id: value.0,
                        action: value.1,
                    })
                })
                .map_err(|err| err.into()),
            Some(DBUS_NOTIFICATIONS_INTERFACE_CLOSED) => message
                .body()
                .deserialize::<(u32, u32)>()
                .map(|value| {
                    NotificationSignal::Closed(NotificationClosed {
                        notification_id: value.0,
                        reason: value.0,
                    })
                })
                .map_err(|err| err.into()),
            Some(DBUS_NOTIFICATIONS_INTERFACE_ACTIVATION_TOKEN) => Err(anyhow!(
                "handling for signal ActivationToken is not implemented"
            )),
            _ => Err(anyhow!("unknown interface member")),
        }
    }
}

// TODO wrap NotificationSignal in an Arc

#[derive(Clone)]
pub struct NotificationsDbus {
    connection: Connection,
    sender: Sender<NotificationSignal>,
}

impl NotificationManager for NotificationsDbus {
    async fn notify(
        &self,
        summary: &str,
        body: &str,
        actions: &[&str],
        hints: &HashMap<&str, Value<'_>>,
        timeout: Option<Duration>,
    ) -> anyhow::Result<u32> {
        let duration = match timeout {
            Some(duration) => duration.as_secs() as i32,
            None => -1,
        };

        let body = &(
            NOTIFICATION_APP_NAME,
            0u32,
            "",
            summary,
            body,
            actions,
            hints,
            duration,
        );

        self.connection
            .call_method(
                Some(DBUS_NOTIFICATIONS_DESTINATION),
                DBUS_NOTIFICATIONS_OBJECT,
                Some(DBUS_NOTIFICATIONS_INTERFACE),
                DBUS_NOTIFICATIONS_INTERFACE_NOTIFY,
                &body,
            )
            .await?
            .body()
            .deserialize()
            .map_err(|error| error.into())
    }

    async fn close(&self, notification_id: u32) -> anyhow::Result<()> {
        let body = (notification_id,);

        self.connection
            .call_method(
                Some(DBUS_NOTIFICATIONS_DESTINATION),
                DBUS_NOTIFICATIONS_OBJECT,
                Some(DBUS_NOTIFICATIONS_INTERFACE),
                DBUS_NOTIFICATIONS_INTERFACE_CLOSE,
                &body,
            )
            .await?
            .body()
            .deserialize()
            .map_err(|error| error.into())
    }

    fn subscribe(&self) -> Receiver<NotificationSignal> {
        self.sender.subscribe()
    }
}

impl NotificationsDbus {
    pub async fn new() -> anyhow::Result<Self> {
        let connection = Connection::session().await?;
        let (sender, _) = broadcast::channel(CHANNEL_SIGNAL_SIZE);
        let notifications = NotificationsDbus { connection, sender };

        // TODO unsure if the task should be spawned here
        let notifications_clone = notifications.clone();
        tokio::spawn(async move {
            notifications_clone.watcher().await.unwrap();
        });

        Ok(notifications)
    }

    #[tracing::instrument(skip(self))]
    async fn watcher(&self) -> anyhow::Result<()> {
        let proxy: Proxy = zbus::proxy::Builder::new(&self.connection)
            .destination(DBUS_NOTIFICATIONS_DESTINATION)?
            .path(DBUS_NOTIFICATIONS_OBJECT)?
            .interface(DBUS_NOTIFICATIONS_INTERFACE)?
            .cache_properties(zbus::CacheProperties::No)
            .build()
            .await?;

        let mut stream = proxy.receive_all_signals().await?;

        while let Some(message) = stream.next().await {
            let signal: NotificationSignal = match (&message).try_into() {
                Ok(value) => value,
                Err(_) => {
                    error!("failed to parse notification for message {}", &message);
                    continue;
                }
            };

            let _ = self.sender.send(signal); // only fails if there are no receivers
        }

        Ok(())
    }
}
