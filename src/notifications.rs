use crate::usbguard::DevicePresenceUpdate;
use anyhow::anyhow;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::error;
use zbus::export::ordered_stream::OrderedStreamExt;
use zbus::{zvariant::Value, Connection, Message, Proxy};

const NOTIFICATION_APP_NAME: &str = "usbguard-notifications";
const NOTIFICATION_ACTION_CHANNEL_SIZE: usize = 64;
const NOTIFICATION_ACTION_TIMEOUT: Duration = Duration::from_secs(60 * 5);

const DBUS_NOTIFICATIONS_DESTINATION: &str = "org.freedesktop.Notifications";
const DBUS_NOTIFICATIONS_OBJECT: &str = "/org/freedesktop/Notifications";
const DBUS_NOTIFICATIONS_INTERFACE: &str = "org.freedesktop.Notifications";
const DBUS_NOTIFICATIONS_INTERFACE_NOTIFY: &str = "Notify";
const DBUS_NOTIFICATIONS_INTERFACE_CLOSE: &str = "CloseNotification";
const DBUS_NOTIFICATIONS_INTERFACE_ACTION_INVOKED: &str = "ActionInvoked";
const DBUS_NOTIFICATIONS_INTERFACE_CLOSED: &str = "NotificationClosed";

const ACTION_ALLOW: (&str, &str) = ("allow", "Allow");
const ACTION_IGNORE: (&str, &str) = ("ignore", "Ignore");

#[derive(Debug)]
pub struct TimeoutError;

impl Display for TimeoutError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "timeout has been exceeded")
    }
}

impl std::error::Error for TimeoutError {}

#[derive(Debug, Clone)]
enum NotificationSignal {
    ActionInvoked(NotificationActionInvoked),
    Closed(NotificationClosed),
}

#[derive(Debug, Clone)]
struct NotificationActionInvoked {
    notification_id: u32,
    action: Arc<String>,
}

#[derive(Debug, Clone, Copy)]
struct NotificationClosed {
    notification_id: u32,
    reason: u32,
}

// TODO avoid duplicating all this code
impl TryFrom<&Message> for NotificationSignal {
    type Error = (); // TODO use a proper error type

    fn try_from(message: &Message) -> Result<Self, Self::Error> {
        let hdr = message.header();
        let message_type = message.message_type();
        let interface = hdr.interface();
        let member = hdr.member();
        let interface = interface.as_ref().map(|i| i.as_str());
        let member = member.as_ref().map(|m| m.as_str());

        match (message_type, interface, member) {
            (
                zbus::message::Type::Signal,
                Some(DBUS_NOTIFICATIONS_INTERFACE),
                Some(DBUS_NOTIFICATIONS_INTERFACE_ACTION_INVOKED),
            ) => message
                .body()
                .deserialize::<(u32, String)>()
                .map(|value| {
                    NotificationSignal::ActionInvoked(NotificationActionInvoked {
                        notification_id: value.0,
                        action: Arc::new(value.1),
                    })
                })
                .map_err(|_| ()),
            (
                zbus::message::Type::Signal,
                Some(DBUS_NOTIFICATIONS_INTERFACE),
                Some(DBUS_NOTIFICATIONS_INTERFACE_CLOSED),
            ) => message
                .body()
                .deserialize::<(u32, u32)>()
                .map(|value| {
                    NotificationSignal::Closed(NotificationClosed {
                        notification_id: value.0,
                        reason: value.0,
                    })
                })
                .map_err(|_| ()),
            _ => Err(()),
        }
    }
}

#[derive(Clone)]
pub struct Notifications {
    connection: Connection,
    sender: Sender<NotificationSignal>,
}

impl Notifications {
    pub async fn new() -> anyhow::Result<Self> {
        let connection = Connection::session().await?;
        let (sender, _) = broadcast::channel(NOTIFICATION_ACTION_CHANNEL_SIZE);
        let notifications = Notifications { connection, sender };

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

    async fn get_next_signal(
        &self,
        notification_id: u32,
        mut recv: Receiver<NotificationSignal>,
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

    pub async fn ask_allow_device(&self, update: &DevicePresenceUpdate) -> anyhow::Result<bool> {
        // subscription should be made before sending the notification to ensure no messages are missed
        let receiver: Receiver<NotificationSignal> = self.sender.subscribe();

        let notification_id: u32 = self.notify_query_user(update.name()).await?;

        // after a notification is sent, 1 of 3 things can happen:
        // 1. the user invokes an action of the notification
        // 2. the notification expires or the user closes it
        // 3. the USB device associated with the notification is removed

        // TODO check if the USB device is removed
        tokio::select! {
            signal = self.get_next_signal(notification_id, receiver) => {
                match signal {
                    NotificationSignal::ActionInvoked(signal) => Ok(signal.action.as_str() == ACTION_ALLOW.0),
                    NotificationSignal::Closed(_) => Err(anyhow!("notification closed or expired")),
                }
            },
        }
    }

    // basic wrapper
    pub async fn notify(
        &self,
        summary: &str,
        body: &str,
        timeout: Duration,
    ) -> anyhow::Result<u32> {
        notify(
            &self.connection,
            summary,
            body,
            &[],
            &HashMap::default(),
            timeout,
        )
        .await
    }

    async fn notify_query_user(&self, device_name: &str) -> anyhow::Result<u32> {
        let mut hints = HashMap::new();
        hints.insert("urgency", Value::U8(2)); // set urgency to critical

        let actions = vec![
            ACTION_IGNORE.0,
            ACTION_IGNORE.1,
            ACTION_ALLOW.0,
            ACTION_ALLOW.1,
        ];

        notify(
            &self.connection,
            "New blocked USB device detected",
            format!("Allow device \"{}\"?", device_name).as_str(),
            actions.as_slice(),
            &hints,
            NOTIFICATION_ACTION_TIMEOUT,
        )
        .await
    }
}

/// Returns the notification ID.
async fn notify(
    connection: &Connection,
    summary: &str,
    body: &str,
    actions: &[&str],
    hints: &HashMap<&str, Value<'_>>,
    timeout: Duration,
) -> anyhow::Result<u32> {
    let body = &(
        NOTIFICATION_APP_NAME,
        0u32,
        "",
        summary,
        body,
        actions,
        hints,
        timeout.as_secs() as i32,
    );

    connection
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

async fn close_notification(connection: &Connection, notification_id: u32) -> anyhow::Result<()> {
    let body = (notification_id,);

    connection
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
