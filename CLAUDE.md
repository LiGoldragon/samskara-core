# samskara-core

Shared agent infrastructure for the aski world. Owns the World VCS protocol:
commit, snapshot, restore over rkyv-serialized typed data stored in criome-store.

## v1 branch

The CozoDB-based VCS (commit/snapshot/delta, MCP scaffolding, jj mirror)
lives on the `v1` branch and is pinned by all current consumers.

## New direction

- rkyv World blobs over criome-store (append-only content-addressed)
- blake3 commit identity
- HEAD is a single 32-byte mutable file
- Zero-copy reads via rkyv::access
- Written in aski
