`launchd` setup for macOS.

Unit file:

- `com.user.rad.plist` (LaunchAgent)

Quick setup:

1. Replace these placeholders in `com.user.rad.plist` with absolute paths:
   - `{path-to-rad}`: directory containing the `rad` binary.
   - `{username}`: your macOS username, used in `/Users/{username}/.cargo/bin`.
2. Copy the plist into `~/Library/LaunchAgents/`.
3. Load and start the agent:

```bash
mkdir -p ~/Library/LaunchAgents
cp deploy/launchd/com.user.rad.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.user.rad.plist
launchctl enable gui/$(id -u)/com.user.rad
launchctl kickstart -k gui/$(id -u)/com.user.rad
launchctl print gui/$(id -u)/com.user.rad
```
