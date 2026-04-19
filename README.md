# rad

## How to quickly connect to rad in tests?

fun the following command in Neovim's command mode
```
:lua vim.lsp.start({name="rad", cmd=vim.lsp.rpc.connect("127.0.0.1", 27631)})
```

## Design Challenge

### Challenge 1

When multiplexing multiple LSP clients over a single backend (e.g., rust-analyzer),
request IDs are only unique per client and can easily collide. This makes response
routing ambiguous, since the backend cannot distinguish which client a response
belongs to. To resolve this, the multiplexer must remap client-local IDs into
globally unique IDs, maintain mappings for routing responses and handling
cancellations, and ensure proper cleanup to avoid stale state.
