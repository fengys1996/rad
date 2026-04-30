`systemd` setup for Linux.

Unit file:

- `rad.service` (user service)

Quick setup:

1. Copy the unit file into `~/.config/systemd/user/`.
2. Reload the user daemon and start the service.

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/rad.service ~/.config/systemd/user/rad.service
systemctl --user daemon-reload
systemctl --user enable --now rad
systemctl --user status rad
```
