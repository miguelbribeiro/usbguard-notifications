use crate::usbguard::{DeviceManager, DevicePresenceUpdate, DeviceTarget};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use zbus::export::futures_util::StreamExt;
use zbus::proxy::SignalStream;
use zbus::{Connection, Message, Proxy};
use zvariant::Type;

const USBGUARD_DBUS_DESTINATION: &'static str = "org.usbguard1";
const USBGUARD_DBUS_OBJECT: &'static str = "/org/usbguard1/Devices";
const USBGUARD_DBUS_INTERFACE: &'static str = "org.usbguard.Devices1";
const USBGUARD_DBUS_INTERFACE_PRESENCE_CHANGED: &'static str = "DevicePresenceChanged";
const USBGUARD_DBUS_INTERFACE_APPLY_POLICY: &'static str = "applyDevicePolicy";

#[derive(Debug)]
struct UsbGuardDevicesProxy<'a>(Proxy<'a>);

impl<'a> From<Proxy<'a>> for UsbGuardDevicesProxy<'a> {
    fn from(proxy: Proxy<'a>) -> Self {
        UsbGuardDevicesProxy(::std::convert::Into::into(proxy))
    }
}

impl<'a> zbus::proxy::ProxyDefault for UsbGuardDevicesProxy<'a> {
    const INTERFACE: Option<&'static str> = Some(USBGUARD_DBUS_INTERFACE);
    const DESTINATION: Option<&'static str> = Some(USBGUARD_DBUS_DESTINATION);
    const PATH: Option<&'static str> = Some(USBGUARD_DBUS_OBJECT);
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
    c: u32,
    // idk what this is tbh
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
                Some(USBGUARD_DBUS_INTERFACE),
                Some(USBGUARD_DBUS_INTERFACE_PRESENCE_CHANGED),
            ) => message
                .body()
                .deserialize::<DevicePresenceUpdateInternal>()
                .map_err(|error| MessageError::Zbus(error)),
            _ => Err(MessageError::WrongMessage),
        }
    }
}

impl TryFrom<DevicePresenceUpdateInternal> for DevicePresenceUpdate {
    type Error = &'static str;

    /// Only fails if an attribute was missing.
    fn try_from(mut value: DevicePresenceUpdateInternal) -> Result<Self, Self::Error> {
        let name = value.attributes.remove("name").ok_or("name")?;

        Ok(DevicePresenceUpdate::new(
            value.id,
            value.event.into(),
            value.rule,
            name,
        ))
    }
}

pub struct DbusDeviceManager {
    connection: Connection,
}

impl DbusDeviceManager {
    pub async fn new() -> zbus::Result<Self> {
        Ok(Self {
            connection: Connection::system().await?,
        })
    }
}

impl DeviceManager for DbusDeviceManager {
    async fn watch_device_changes(
        &self,
        sender: Sender<DevicePresenceUpdate>,
    ) -> anyhow::Result<()> {
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

            let update: DevicePresenceUpdate = match update.try_into() {
                Ok(update) => update,
                Err(error) => {
                    eprintln!("Failed to convert: {}", error);
                    continue;
                }
            };

            sender.send(update).await.unwrap();
        }

        panic!("Stream ended unexpectedly");
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

        // TODO check return
        self.connection
            .call_method(
                Some(USBGUARD_DBUS_DESTINATION),
                USBGUARD_DBUS_OBJECT,
                Some(USBGUARD_DBUS_INTERFACE),
                USBGUARD_DBUS_INTERFACE_APPLY_POLICY,
                &body,
            )
            .await
            .map(|_| ())
            .map_err(|err| err.into())
    }
}
