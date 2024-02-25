#[derive(Debug, PartialEq, Copy, Clone)]
pub enum DeviceEvent {
    Present,
    Insert,
    Update,
    Remove,
    Other,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum DeviceTarget {
    Allow,
    Block,
    Reject,
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

#[derive(Debug)]
pub struct DevicePresenceUpdate {
    device_id: u32,
    event: DeviceEvent,
    rule: Box<str>,
    name: Box<str>,
}

impl DevicePresenceUpdate {
    pub fn new(device_id: u32, event: DeviceEvent, rule: String, name: String) -> Self {
        Self {
            device_id,
            event,
            rule: rule.into_boxed_str(),
            name: name.into_boxed_str(),
        }
    }

    pub fn device_id(&self) -> u32 {
        self.device_id
    }

    pub fn event(&self) -> DeviceEvent {
        self.event
    }

    pub fn rule(&self) -> &str {
        &self.rule
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn target(&self) -> DeviceTarget {
        if self.rule.starts_with("block") {
            DeviceTarget::Block
        } else if self.rule.starts_with("allow") {
            DeviceTarget::Allow
        } else if self.rule.starts_with("reject") {
            DeviceTarget::Reject
        } else {
            DeviceTarget::Other
        }
    }
}

pub trait DeviceManager: Send {
    fn watch_device_changes(
        &self,
        sender: tokio::sync::mpsc::Sender<DevicePresenceUpdate>,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}
