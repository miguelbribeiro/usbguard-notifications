# usbguard-notifications
Allow devices blocked by USBGuard through notifications.

Requires a notifications daemon that implements the [Desktop Notifications Specification from freedesktop.org](https://specifications.freedesktop.org/notification/latest/index.html) and supports actions.
Only tested on GNOME, but it should work on KDE and other modern desktop environments/notifications daemons too.

## Usage
1. Enable and start `usbguard-dbus.service`:
```
# systemctl enable --now usbguard-dbus.service
```
2. If necessary, create a [polkit rule](#polkit-rules).
3. Start the program:
```
$ cargo run --release
```

## Polkit Rules
This program is designed to be ran in a graphical session, under an unprivileged user.
However, in most setups, an unprivilegd user will not be able to manage USBGuard.
To allow access to the USBGuard daemon over DBus, make sure your user is part of the `wheel` group and add the following `polkit` rule to `/etc/polkit-1/rules.d/70-allow-usbguard.rules`:
```
polkit.addRule(function(action, subject) {
    if ((action.id == "org.usbguard.Policy1.listRules" ||
         action.id == "org.usbguard.Policy1.appendRule" ||
         action.id == "org.usbguard.Policy1.removeRule" ||
         action.id == "org.usbguard.Devices1.applyDevicePolicy" ||
         action.id == "org.usbguard.Devices1.listDevices" ||
         action.id == "org.usbguard1.getParameter" ||
         action.id == "org.usbguard1.setParameter") &&
        subject.active == true && subject.local == true &&
        subject.isInGroup("wheel")) {
            return polkit.Result.YES;
    }
});
```

If you prefer using a group other than `wheel`, change the group name in the previous rule.
