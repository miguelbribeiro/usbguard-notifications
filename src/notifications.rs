use std::collections::HashMap;

use zbus::{zvariant::Value, Connection, Proxy};

#[derive(Debug, Clone, Copy)]
pub enum Action {
    Allow,
    Block,
    None,
}

async fn send_internal(connection: &Connection) -> anyhow::Result<u32> {
    let m = connection.call_method(
        Some("org.freedesktop.Notifications"),
        "/org/freedesktop/Notifications",
        Some("org.freedesktop.Notifications"),
        "Notify",
        &("my-app", 0u32, "dialog-information", "A summary", "Some body",
          vec![""; 0], HashMap::<&str, &Value>::new(), 5000),
    ).await?;
    
    m.body().deserialize().map_err(|error| error.into())
}

pub async fn send_notification(text: &str) -> anyhow::Result<Action> {
    let connection = Connection::session().await?;
    let proxy: Proxy = zbus::proxy::Builder::new(&connection).build().await?;
    
    
    Ok(Action::None)
}
