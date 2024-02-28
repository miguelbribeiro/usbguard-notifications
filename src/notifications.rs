use crate::usbguard::{DevicePresenceUpdate, DeviceTarget};
use anyhow::bail;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio::time::Instant;
use zbus::export::ordered_stream::OrderedStreamExt;
use zbus::{zvariant::Value, Connection, Message, Proxy};

const NOTIFICATION_ACTION_CHANNEL_SIZE: usize = 64;
const NOTIFICATION_ACTION_TIMEOUT: Duration = Duration::from_secs(10);

const DBUS_FREEDESKTOP_NOTIFICATIONS_DESTINATION: &str = "org.freedesktop.Notifications";
const DBUS_FREEDESKTOP_NOTIFICATIONS_OBJECT: &str = "/org/freedesktop/Notifications";
const DBUS_FREEDESKTOP_NOTIFICATIONS_INTERFACE: &str = "org.freedesktop.Notifications";
const DBUS_FREEDESKTOP_NOTIFICATIONS_INTERFACE_MEMBER: &str = "ActionInvoked";

#[derive(Debug, Clone)]
struct NotificationAction {
    notification_id: u32,
    action: Arc<String>,
}

// TODO avoid duplicating all this code
impl TryFrom<Message> for NotificationAction {
    type Error = ();

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        let hdr = message.header();
        let message_type = message.message_type();
        let interface = hdr.interface();
        let member = hdr.member();
        let interface = interface.as_ref().map(|i| i.as_str());
        let member = member.as_ref().map(|m| m.as_str());

        match (message_type, interface, member) {
            (
                zbus::message::Type::Signal,
                Some(DBUS_FREEDESKTOP_NOTIFICATIONS_INTERFACE),
                Some(DBUS_FREEDESKTOP_NOTIFICATIONS_INTERFACE_MEMBER),
            ) => message
                .body()
                .deserialize::<(u32, String)>()
                .map(|value| NotificationAction {
                    notification_id: value.0,
                    action: Arc::new(value.1),
                })
                .map_err(|_| ()),
            _ => Err(()),
        }
    }
}

#[derive(Clone)]
pub struct Notifications {
    connection: Connection,
    sender: Sender<NotificationAction>,
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

    async fn watcher(&self) -> anyhow::Result<()> {
        let proxy: Proxy = zbus::proxy::Builder::new(&self.connection)
            .destination(DBUS_FREEDESKTOP_NOTIFICATIONS_DESTINATION)?
            .path(DBUS_FREEDESKTOP_NOTIFICATIONS_OBJECT)?
            .interface(DBUS_FREEDESKTOP_NOTIFICATIONS_INTERFACE)?
            .cache_properties(zbus::CacheProperties::No)
            .build()
            .await?;

        let mut stream = proxy
            .receive_signal(DBUS_FREEDESKTOP_NOTIFICATIONS_INTERFACE_MEMBER)
            .await?;

        while let Some(message) = stream.next().await {
            let update: NotificationAction = match message.try_into() {
                Ok(value) => value,
                Err(_) => {
                    continue;
                } // TODO do something
            };

            self.sender.send(update)?;
        }

        Ok(())
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
                return DeviceTarget::parse(&notification_action.action);
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
                    "",
                    "New device detected",
                    format!("Allow device \"{}\"?", device_name),
                    vec!["block", "Block", "allow", "Allow"],
                    HashMap::<&str, &Value>::new(),
                    NOTIFICATION_ACTION_TIMEOUT.as_millis() as i32,
                ),
            )
            .await?
            .body()
            .deserialize()
            .map_err(|error| error.into())
    }
}
