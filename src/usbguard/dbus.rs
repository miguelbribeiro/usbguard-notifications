use crate::usbguard::{Device, DeviceManager, DeviceTarget};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use zbus::export::futures_util::StreamExt;
use zbus::proxy::SignalStream;
use zbus::{Connection, Message, Proxy};
use zvariant::Type;

const DBUS_DESTINATION: &str = "org.usbguard1";
const DBUS_OBJECT: &str = "/org/usbguard1/Devices";
const DBUS_INTERFACE: &str = "org.usbguard.Devices1";
const DBUS_INTERFACE_PRESENCE_CHANGED: &str = "DevicePresenceChanged";
const DBUS_INTERFACE_APPLY_POLICY: &str = "applyDevicePolicy";

const CHANNEL_LIMIT: usize = 64;

#[derive(Debug)]
struct UsbGuardDevicesProxy<'a>(Proxy<'a>);

impl<'a> From<Proxy<'a>> for UsbGuardDevicesProxy<'a> {
    fn from(proxy: Proxy<'a>) -> Self {
        UsbGuardDevicesProxy(::std::convert::Into::into(proxy))
    }
}

impl<'a> zbus::proxy::ProxyDefault for UsbGuardDevicesProxy<'a> {
    const INTERFACE: Option<&'static str> = Some(DBUS_INTERFACE);
    const DESTINATION: Option<&'static str> = Some(DBUS_DESTINATION);
    const PATH: Option<&'static str> = Some(DBUS_OBJECT);
}

impl<'a> UsbGuardDevicesProxy<'a> {
    async fn new(connection: &Connection) -> zbus::Result<Self> {
        zbus::proxy::Builder::new(connection)
            .cache_properties(zbus::CacheProperties::No)
            .build()
            .await
    }

    async fn receive_device_presence_changed(&self) -> zbus::Result<SignalStream> {
        self.0.receive_signal("DevicePresenceChanged").await
    }
}

enum MessageError {
    WrongMessage,
    Zbus(zbus::Error),
}

#[derive(Debug, Deserialize, Type)]
struct DevicePresenceUpdateInternal {
    id: u32,
    event: u32,
    // idk what this is tbh
    c: u32,
    rule: String,
    attributes: HashMap<String, String>,
}

impl TryFrom<Message> for DevicePresenceUpdateInternal {
    type Error = MessageError;

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
                Some(DBUS_INTERFACE),
                Some(DBUS_INTERFACE_PRESENCE_CHANGED),
            ) => message
                .body()
                .deserialize::<DevicePresenceUpdateInternal>()
                .map_err(|error| MessageError::Zbus(error)),
            _ => Err(MessageError::WrongMessage),
        }
    }
}

impl TryFrom<DevicePresenceUpdateInternal> for Device {
    type Error = &'static str;

    /// Only fails if an attribute was missing.
    fn try_from(mut value: DevicePresenceUpdateInternal) -> Result<Self, Self::Error> {
        let name = value.attributes.remove("name").ok_or("name")?;

        Ok(Device::new(value.id, value.event.into(), value.rule, name))
    }
}

pub struct DbusDeviceManager {
    connection: Connection,
    channel: Sender<Arc<Device>>,
}

impl DbusDeviceManager {
    pub async fn new() -> zbus::Result<Self> {
        let (sender, _) = tokio::sync::broadcast::channel(CHANNEL_LIMIT);

        Ok(Self {
            connection: Connection::system().await?,
            channel: sender,
        })
    }
}

impl DeviceManager for DbusDeviceManager {
    async fn watch_device_changes(&self) -> anyhow::Result<()> {
        let usbguard_proxy = UsbGuardDevicesProxy::new(&self.connection).await?;
        let mut update_stream = usbguard_proxy.receive_device_presence_changed().await?;

        while let Some(message) = update_stream.next().await {
            let update: DevicePresenceUpdateInternal = match message.try_into() {
                Ok(message) => message,
                Err(error) => match error {
                    MessageError::WrongMessage => continue,
                    MessageError::Zbus(error) => return Err(error.into()),
                },
            };

            let update: Device = match update.try_into() {
                Ok(update) => update,
                Err(error) => {
                    eprintln!("Failed to convert: {}", error);
                    continue;
                }
            };

            let _ = self.channel.send(Arc::new(update));
        }

        panic!("Stream ended unexpectedly");
    }

    fn subscribe_device_changes(&self) -> Receiver<Arc<Device>> {
        self.channel.subscribe()
    }

    async fn apply_device_target(
        &self,
        device_id: u32,
        target: DeviceTarget,
    ) -> anyhow::Result<()> {
        let target: u32 = match target {
            DeviceTarget::Allow => 0,
            DeviceTarget::Block => 1,
            DeviceTarget::Reject => 2,
        };

        // (id, target, permanent)
        let body = (device_id, target, false);

        self.connection
            .call_method(
                Some(DBUS_DESTINATION),
                DBUS_OBJECT,
                Some(DBUS_INTERFACE),
                DBUS_INTERFACE_APPLY_POLICY,
                &body,
            )
            .await
            .map(|_| ())
            .map_err(|err| err.into())
    }
}
