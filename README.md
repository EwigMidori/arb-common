# arb-common

Shared EVM and observability infrastructure for arb-research and dex-dag-arb.

| Crate | Purpose |
| --- | --- |
| `evm-rpc` | EVM RPC transport and subscriptions. |
| `evm-tx` | Transaction encoding, signing, and fee arithmetic. |
| `revm-fork` | Fork state and local EVM execution. |
| `probe-obs` | Metrics serving and notifications. |

## Build and test

Use the stable Rust toolchain with Clippy installed:

```sh
cargo check --locked
cargo clippy --all-targets -- -D warnings
cargo test --locked
python scripts/check_workspace_deps.py
```

## Versioning

The workspace shares a single version. Annotated Git tags use `vMAJOR.MINOR.PATCH`; consumers can select a tag or commit through Cargo Git dependencies. During the `0.x` series, minor releases may include incompatible API changes. These crates are maintained as Git dependencies and are not published to crates.io.

Licensed under the [MIT License](LICENSE).
