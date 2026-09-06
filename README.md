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
- Keep rust-analyzer alive when clients disconnect; idle reaper shuts it down after a configurable timeout.
- Pin a running instance by PID to exempt it from idle shutdown (see `rad pin`).
- Start rust-analyzer in the workspace directory to respect each project's Rust toolchain.

## Configuration

The default config path is `~/.config/rad/rad.toml`. Use `-c` / `--config-file`
to override it.

An example config file is provided at [`rad.toml`](rad.toml) in the repository
root.

## How to Use

### Run rad Server

**Direct Run**

```bash
rad server
```

**systemd**

For Linux setup, see [deploy/systemd/README.md](deploy/systemd/README.md).

**launchd (macOS)**

For macOS setup, see [deploy/launchd/README.md](deploy/launchd/README.md).

### Configure Editor

**Neovim(rustaceanvim)**

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

**VSCode**

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

### rad status

Show all running LSP instances:

```bash
rad status
```

Example output:

```
workspace: file:///home/user/greptimedb
  pid:      12345
  clients:  2
  idle:     1m 15s
  pinned:   yes
  healthy:  yes

workspace: file:///home/user/rad
  pid:      67890
  clients:  0
  idle:     5m 30s
  pinned:   no
  healthy:  yes
```

### rad clean

`rad clean` shuts down idle instances, i.e. instances with no attached
clients that are not pinned (see `rad pin`). Pinned instances are kept even
when idle:

```bash
rad clean
```

`rad clean -f` skips all checks and removes every instance, regardless of
pin state or attached clients. Instances currently serving clients are shut
down as well, so use it with care:

```bash
rad clean -f
```

Example output:

```
file:///home/user/rad (pid: 67890)
```

### rad pin

Prevent an LSP instance from being removed when it is idle:

```bash
rad pin 12345
```

Remove the pin:

```bash
rad pin -r 12345
```

Use `rad status` to find the instance PID and check its pin state. Pinned
instances are skipped by `rad clean`, but can still be removed with
`rad clean -f`.
