use std::collections::HashMap;

use futures::StreamExt;
use zbus::Connection;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum DeviceEvent {
    Present,
    Insert,
    Update,
    Remove,
}

impl TryFrom<u32> for DeviceEvent {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Present),
            1 => Ok(Self::Insert),
            2 => Ok(Self::Update),
            3 => Ok(Self::Remove),
            _ => Err(()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum DeviceTarget {
    Allow,
    Block,
    Reject,
}

impl TryFrom<u32> for DeviceTarget {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(DeviceTarget::Allow),
            1 => Ok(DeviceTarget::Block),
            2 => Ok(DeviceTarget::Reject),
            _ => Err(()),
        }
    }
}

impl From<DeviceTarget> for u32 {
    fn from(value: DeviceTarget) -> Self {
        match value {
            DeviceTarget::Allow => 0,
            DeviceTarget::Block => 1,
            DeviceTarget::Reject => 2,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct DeviceId(u32);

#[derive(Debug)]
pub struct DeviceUpdate {
    pub id: DeviceId,
    pub event: DeviceEvent,
    pub target: DeviceTarget,
    pub name: Option<String>,
}

impl TryFrom<DevicePresenceChangedArgs<'_>> for DeviceUpdate {
    type Error = anyhow::Error;

    fn try_from(value: DevicePresenceChangedArgs) -> Result<Self, Self::Error> {
        let event = value
            .event
            .try_into()
            .map_err(|_| anyhow::anyhow!("failed to parse device event: {}", value.event))?;

        let target = value
            .target
            .try_into()
            .map_err(|_| anyhow::anyhow!("failed to parse device target: {}", value.target))?;

        let name = value.attributes.get("name").map(|v| v.to_string());

        Ok(DeviceUpdate {
            id: DeviceId(value.id),
            event: event,
            target: target,
            name: name,
        })
    }
}

#[zbus::proxy(
    default_service = "org.usbguard1",
    default_path = "/org/usbguard1/Devices",
    interface = "org.usbguard.Devices1",
    gen_blocking = false
)]
trait Devices {
    fn apply_device_policy(&self, id: u32, target: u32, permanent: bool) -> zbus::Result<u32>;

    /// DevicePresenceChanged signal
    #[zbus(signal)]
    fn device_presence_changed(
        &self,
        id: u32,
        event: u32,
        target: u32,
        device_rule: &str,
        attributes: HashMap<&str, &str>,
    ) -> zbus::Result<()>;
}

pub struct DeviceManager {
    proxy: DevicesProxy<'static>,
}

impl DeviceManager {
    pub async fn new() -> zbus::Result<Self> {
        let connection = Connection::system().await?;

        Ok(DeviceManager {
            proxy: DevicesProxy::new(&connection).await?,
        })
    }

    pub async fn get_device_update_stream(
        &self,
    ) -> zbus::Result<impl futures::Stream<Item = anyhow::Result<DeviceUpdate>>> {
        Ok(self
            .proxy
            .receive_device_presence_changed()
            .await?
            .map(|v| v.args()?.try_into()))
    }

    pub async fn apply_device_policy(
        &self,
        device_id: DeviceId,
        target: DeviceTarget,
    ) -> zbus::Result<()> {
        self.proxy
            .apply_device_policy(device_id.0, target.into(), false)
            .await
            .map(|_| ())
    }
}
