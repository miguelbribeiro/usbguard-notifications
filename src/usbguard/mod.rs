use anyhow::anyhow;
use std::sync::Arc;

pub mod dbus;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum DeviceEvent {
    Present,
    Insert,
    Update,
    Remove,
    Other,
}

impl From<u32> for DeviceEvent {
    fn from(value: u32) -> Self {
        match value {
            0 => Self::Present,
            1 => Self::Insert,
            2 => Self::Update,
            3 => Self::Remove,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum DeviceTarget {
    Allow,
    Block,
    Reject,
}

impl DeviceTarget {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "allow" => Ok(DeviceTarget::Allow),
            "block" => Ok(DeviceTarget::Block),
            "reject" => Ok(DeviceTarget::Reject),
            _ => Err(anyhow!("Failed to parse target from value \"{}\"", value)),
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

#[derive(Debug)]
pub struct Device {
    id: u32,
    rule: String,
}

impl Device {
    pub fn new(id: u32, rule: String) -> Self {
        Self { id, rule }
    }

    pub fn device_id(&self) -> u32 {
        self.id
    }

    pub fn rule(&self) -> &str {
        &self.rule
    }

    pub fn target(&self) -> anyhow::Result<DeviceTarget> {
        let target = self
            .rule
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow!("String is empty"))?;

        DeviceTarget::parse(target)
    }
}

/// Represents a USB device update handled by USBGuard.
#[derive(Debug)]
pub struct DeviceUpdate {
    device: Device,
    event: DeviceEvent,
    name: String,
}

impl DeviceUpdate {
    pub fn new(device: Device, event: DeviceEvent, name: String) -> Self {
        Self {
            device,
            event,
            name,
        }
    }

    pub fn device(&self) -> &Device {
        &self.device
    }

    pub fn event(&self) -> DeviceEvent {
        self.event
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub trait DeviceManager: Send {
    /// Listens and sends device presence updates to subscribers.
    /// The returned future only completes if there is an error.
    fn watch_device_changes(&self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;

    /// Returns a [Receiver](tokio::sync::broadcast::Receiver) that receives device presence updates.
    /// The listener must be started before, by running [watch_device_changes](Self::watch_device_changes).
    fn subscribe_device_changes(&self) -> tokio::sync::broadcast::Receiver<Arc<DeviceUpdate>>;

    /// Applies a target to the specified device.
    fn apply_device_target(
        &self,
        device_id: u32,
        target: DeviceTarget,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}
