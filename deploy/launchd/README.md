`launchd` setup for macOS.

Unit file:

- `com.user.rad.plist` (LaunchAgent)

Quick setup:

1. Replace the placeholder in `com.user.rad.plist`:
   - `{path-to-rad}`: directory containing the `rad` binary.
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
