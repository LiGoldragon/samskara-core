# samskara-core

Shared agent infrastructure crate — VCS (world commits), boot (genesis/restore),
and MCP server scaffolding. Extracted from samskara so all agents share the same
commit/snapshot/delta machinery.

## Current implementation

CozoDB-backed: world_commit, world_snapshot, world_delta, world_manifest
relations. Snapshots are JSON→zstd→base64 strings. Deltas are per-relation
row diffs. Every 10 commits a full snapshot is taken. This is the old stack.

## Target: rkyv World + criome-store

Replace CozoDB VCS with typed rkyv serialization over criome-store's
append-only content-addressed file.

### Two object types in the store

Both content-addressed by blake3, stored via criome-store:

- **World blob** — `rkyv::to_bytes(&world)`, the entire typed World struct.
  100KB–1MB. Immutable once written.
- **Commit node** — small rkyv struct pointing to a World blob + parent commit.

```
Commit {
  WorldHash  [u8; 32]    // blake3 of the World blob
  ParentHash [u8; 32]    // blake3 of parent Commit (zero = root)
  AgentId    String
  Message    String
  Timestamp  u64
}
```

A Commit's own identity = blake3 of its rkyv archive bytes. Not derived
from WorldHash alone — two commits can snapshot the same World.

### HEAD

```
~/.samskara/HEAD    32 bytes — blake3 hash of current commit
```

The only mutable file. Atomically replaced via write-tmp + rename.

### Write path (commit_world)

```
 1. Derive all relations (derive rules on World)
 2. world_bytes = rkyv::to_bytes(&world)
 3. world_hash  = blake3(&world_bytes)
 4. store.put(WORLD, &world_bytes)       // skip if hash exists
 5. commit = Commit { WorldHash, ParentHash(old_head), ... }
 6. commit_bytes = rkyv::to_bytes(&commit)
 7. commit_hash  = blake3(&commit_bytes)
 8. store.put(COMMIT, &commit_bytes)
 9. fdatasync(store.bin)                 // objects on disk
10. HEAD::write(commit_hash)             // atomic rename
11. fdatasync(directory)                 // rename durable
```

fdatasync in step 9 ensures objects are on disk before HEAD advances.
Crash before step 10: HEAD still points to old commit, appended bytes
are orphaned (harmless). Crash during step 10: atomic rename means
HEAD is either old or new, never partial.

### Read path (boot / restore_world)

```
1. Read HEAD → commit_hash
2. store.get(commit_hash) → commit rkyv bytes
3. access::<ArchivedCommit>(&bytes) → zero-copy read
4. store.get(commit.world_hash) → world rkyv bytes
5. rkyv::from_bytes::<World>(&bytes) → owned mutable World
6. Agent is running. Queries read from the in-memory World.
```

Step 5 is full deserialize into an owned struct — the live World must
be mutable for assertions. Zero-copy `&ArchivedWorld` is used for
read-only historical access (time-travel queries).

### Historical access (zero-copy)

```
1. store.get(any_commit_hash) → ArchivedCommit
2. store.get(archived_commit.world_hash) → &[u8]
3. rkyv::access::<ArchivedWorld>(&bytes) → zero-copy read-only view
```

No deserialization. The mmap'd bytes ARE the archived World. rkyv
field access works directly on the mapped memory.

### What goes away

- world_commit, world_snapshot, world_delta, world_manifest relations
- world_snapshot_index, world_schema relations
- JSON→zstd→base64 snapshot pipeline
- Delta computation (per-relation row diffs)
- Snapshot interval logic (every commit is a full World — dedup is free)
- serde_json, zstd, base64 dependencies for VCS

### What stays

- blake3 (content addressing)
- rkyv (zero-copy binary)
- Phase/Dignity lifecycle on the World struct
- Boot/restore protocol (same semantics, different backend)

## VCS

Jujutsu (`jj`) is mandatory. Git is the backend only. Always pass `-m` to
`jj` commands.
