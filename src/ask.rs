use std::fmt::{Display, Formatter};

use crate::notifications::{AsActions, DecisionResult, NotificationManager};
use crate::usbguard::DeviceUpdate;

#[derive(Clone, Copy)]
pub enum AllowIgnoreQuestion {
    Allow,
    Ignore,
}

impl Display for AllowIgnoreQuestion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AllowIgnoreQuestion::Allow => write!(f, "Allow"),
            AllowIgnoreQuestion::Ignore => write!(f, "Ignore"),
        }
    }
}

impl AsActions for AllowIgnoreQuestion {
    fn as_actions() -> impl Iterator<Item = Self> {
        [Self::Allow, Self::Ignore].into_iter()
    }
}

/// Prompts the user to allow or ignore a blocked device.
#[tracing::instrument(skip(notification_manager))]
pub async fn ask<'a>(
    notification_manager: &'a NotificationManager,
    update: &DeviceUpdate,
    cancel: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<DecisionResult<AllowIgnoreQuestion>> {
    let name = update.name.as_deref();
    let prompt_body = format!("Allow device \"{}\"?", name.unwrap_or("(unknown)"));

    Ok(notification_manager
        .decision::<AllowIgnoreQuestion>("Blocked device detected", &prompt_body, cancel)
        .await?)
}
