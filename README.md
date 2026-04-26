# rad

`rad` is an LSP multiplexer for `rust-analyzer`.
It lets multiple Neovim clients share one backend instance per workspace.

## Architecture

```text
+---------+      stdio       +------------+      TCP       +------------+      stdio      +-----------------+
|  Neovim |  <-------------> | rad client | <----------->  | rad server | <-------------> | rust-analyzer   |
|  (LSP)  |                  |   (proxy)  |                | (mux/router)|                 |   (backend RA)  |
+---------+                  +------------+                +------------+                 +-----------------+
```

## Features

- Multiplex multiple LSP clients onto one `rust-analyzer` process (per workspace key).
- Remap client-local request IDs to global IDs to avoid collisions.
- Route responses back to the original client and restore original request IDs.
- Rewrite `$/cancelRequest` IDs from client-local ID to global ID.
- Forward RA messages without request IDs to the most recently active client.
- Reuse existing instance for the same workspace.
- Short-circuit `initialize` for reused instances using cached initialize result.
- Keep RA alive when clients disconnect; idle reaper shuts it down after 5 minutes.

## Run Modes

Default address: `127.0.0.1:27631`

Run server mode (default):

```bash
rad
# or
rad server
```

Run client proxy mode (`stdio <-> tcp`):

```bash
rad client
# or custom address
rad client 127.0.0.1:27631
```

## Neovim Usage

Use `rad` as stdio server:

```lua
vim.lsp.start({
  name = "rad",
  cmd = { "rad", "client", "127.0.0.1:27631" },
})
```

## Multiplexing Behavior

### Workspace instance key

On the first client message, `rad` parses `initialize` and picks workspace key in this order:

1. `params.workspaceFolders[0].uri`
2. `params.rootUri`
3. `params.rootPath`
4. fallback: `default-workspace`

### Request/response routing

- Client request with `id` gets remapped to a global numeric ID before forwarding to RA.
- RA response `id` is mapped back to the original client and original local `id`.
- `$/cancelRequest` is rewritten using the same mapping table.

### Initialize shortcut for reused instances

When a second client attaches to an existing healthy instance:

- `initialize` is not forwarded to RA.
- `rad` replies from cached `initialize` result from the first successful initialize.
- The following client `initialized` notification is swallowed for that client.

If cache is unavailable, `initialize` falls back to normal forwarding.

### Shutdown/exit handling

Client `shutdown` / `exit` are handled locally and are not forwarded to RA.
This prevents a single client from terminating shared backend instance.

### Idle lifecycle

- Instance is kept alive even with zero connected clients.
- Background reaper checks every 30 seconds.
- If an instance has no clients and no activity for 5 minutes, it is removed and RA process is shut down.

## Logs

Use `RUST_LOG` to control verbosity:

```bash
RUST_LOG=info rad server
RUST_LOG=debug rad server
```

## Current Limitations

- Document sync (`didOpen`/`didChange`/`didClose`) is not deduplicated across multiple editors on the same file.
- This may still trigger RA-side duplicate-open warnings in concurrent editing scenarios.
