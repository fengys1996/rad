# rad

`rad` is an abbreviation for **rust-analyzer daemon**.
It decouples the `rust-analyzer` lifecycle from Neovim, so backend instances can
outlive editor sessions.
It also allows multiple Neovim clients opening the same workspace to reuse a
single `rust-analyzer` instance.

## Architecture

```text
+---------+   stdio   +------------+                          +-------------+      stdio      +-----------------+
| Neovim1 | <-------> | rad client | -----------------------> |             | <-------------> | rust-analyzer A |
+---------+           +------------+                          |             |                 +-----------------+
                                                              |             |
+---------+   stdio   +------------+                          | rad server  |
| Neovim2 | <-------> | rad client | -----------------------> | (mux/router)|
+---------+           +------------+                          |             |      stdio      +-----------------+
                                                              |             | <-------------> | rust-analyzer B |
+---------+   stdio   +------------+                          |             |                 +-----------------+
| Neovim3 | <-------> | rad client | -----------------------> |             |
+---------+           +------------+                          +-------------+
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

## How To Run rad Server

Default address: `127.0.0.1:27631`

Run server mode (default):

```bash
rad server
```

## Neovim Usage

Use `rad` as stdio server:

```lua
vim.lsp.start({
  name = "rad",
  cmd = { "rad", "client", "127.0.0.1:27631" },
})
```

## Current Limitations

- Document sync (`didOpen`/`didChange`/`didClose`) is not deduplicated across 
    multiple editors on the same file.
- This may still trigger RA-side duplicate-open warnings in concurrent editing
    scenarios.
