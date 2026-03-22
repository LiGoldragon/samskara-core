# samskara-core

Shared agent infrastructure crate — VCS (world commits), boot (genesis/restore),
and MCP server scaffolding. Extracted from samskara so all agents share the same
commit/snapshot/delta machinery.

## VCS

Jujutsu (`jj`) is mandatory. Git is the backend only. Always pass `-m` to
`jj` commands.
