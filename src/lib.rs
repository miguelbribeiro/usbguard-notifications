mod notifications;
mod usbguard_dbus;

pub async fn run() {
    notifications::send_notification("gm").await.unwrap()    ;
}
