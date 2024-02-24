use zbus::Connection;

mod notifications;
mod usbguard;
mod usbguard_dbus;

const CHANNEL_BUFFER_SIZE: usize = 64;

pub async fn run() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
    
    tokio::spawn(async {
        usbguard_dbus::watch_device_changes(Connection::system().await.unwrap(), sender)
            .await
            .unwrap();
    });

    loop {
        let k = receiver.recv().await.unwrap();
        dbg!(k);
    }
}
