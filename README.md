# rad

`rad` is an abbreviation for **rust-analyzer daemon**.
It decouples the `rust-analyzer` lifecycle from editor like neovim, so backend
instances can outlive editor sessions.
It also allows multiple editors opening the same workspace to reuse a single
`rust-analyzer` instance.

## Architecture

```text
+----------------------+   stdio   +------------+                      +-------------+      stdio      +-----------------------+
| Neovim1              | <-------> | rad client | -------------------> |             | <-------------> | rust-analyzer A       |
| ~/source/greptimedb  |           +------------+                      |             |                 | ~/source/greptimedb   |
+----------------------+                                               |             |                 +-----------------------+
                                                                       |             |
+----------------------+   stdio   +------------+                      | rad server  |
| Neovim2              | <-------> | rad client | -------------------> | (mux/router)|      stdio      +-----------------------+
| ~/source/greptimedb  |           +------------+                      |             | <-------------> | rust-analyzer B       |
+----------------------+                                               |             |                 | ~/source/rad          |
                                                                       |             |                 +-----------------------+
+----------------------+   stdio   +------------+                      |             |
| VSCode               | <-------> | rad client | -------------------> |             |
| ~/source/rad         |           +------------+                      +-------------+
+----------------------+
```

## Features

- Reuse existing rust-analyzer instance for the same workspace.
- Keep rust-analyzer alive when clients disconnect; idle reaper shuts it down after 5 minutes.
- Start rust-analyzer in the workspace directory to respect each project's Rust toolchain.

## How it work?

todo

## How to Use

### Run rad Server

Default address: `127.0.0.1:27631`.

#### 1. Direct Run

```bash
rad server
```

#### 2. systemd

Unit file:

- `deploy/systemd/rad.service` (user service)

Quick setup:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/rad.service ~/.config/systemd/user/rad.service
systemctl --user daemon-reload
systemctl --user enable --now rad
systemctl --user status rad
```

#### 3. launchd (macOS)

For macOS setup, see [deploy/launchd/README.md](deploy/launchd/README.md).

### Configure Editor

#### Neovim(rustaceanvim)

An example configuration of rustaceanvim is shown below.

```lua
vim.g.rustaceanvim = {
    server = {
        cmd = function()
            return {
                vim.fn.exepath("rad"),
                "client",
            }
        end
    },
    -- other configurations
}
```

#### VSCode

VSCode's Rust Analyzer extension calls `--version` (`-V`) on the configured
server binary during startup. Since `rad client` is a proxy command, use a
wrapper script to forward version queries to the real `rust-analyzer`.

1. Create a wrapper script, for example `~/.local/bin/rad-ra`:

```bash
#!/bin/bash

if [[ "$1" == "--version" || "$1" == "-V" ]]; then
    exec rust-analyzer --version
fi

exec rad client "$@"
```

2. Make it executable:

```bash
chmod +x ~/.local/bin/rad-ra
```

3. Configure VSCode (`settings.json`):

```json
{
  "rust-analyzer.server.path": "{path}/rad-ra"
}
```
