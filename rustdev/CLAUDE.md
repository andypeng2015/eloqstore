# EloqStore Rust Implementation

## Quick Reference
**Goal**: Port EloqStore C++ to Rust, maintaining exact functionality
**Rule**: Follow C++ implementation closely - no new features (except I/O abstraction)
**C++ Code**: Located in `../` (read-only reference)

## 🎯 Current Priority Tasks
1. ✅ ~~**Clean up codebase**~~ - Completed: Removed old files, consolidated types
2. **Implement Store core** - Port `eloq_store.cpp`
3. **Fix task TODOs** - Complete read/write page lookup logic
4. **Port shard system** - Implement coroutine scheduling from `shard.cpp`

## 📊 Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| Types & Errors | ✅ Done | Consolidated and organized |
| Page System | ✅ Done | Working |
| I/O Backend | ✅ Done | Abstraction layer created |
| Index System | ✅ Done | IndexPageManager complete |
| Config | ✅ Done | KvOptions implemented |
| Tasks | 🚧 50% | Has TODOs in read/write |
| Store Core | ❌ 5% | Only stub exists |
| Shard System | ❌ 20% | Basic structure only |

## 🔧 Immediate Actions Needed

### Fix These Files
- `src/task/read.rs` - Line 56-58, 153-159 (TODO: page lookup)
- `src/task/write.rs` - Line 136-137, 303-305 (TODO: page allocation)
- `src/store/mod.rs` - Implement actual store logic from `eloq_store.cpp`

## 📚 C++ Reference Map

| Rust Component | C++ Reference | Key Functions |
|---------------|--------------|---------------|
| `store/eloq_store.rs` | `eloq_store.cpp` | HandleRequest, Start, Stop |
| `task/read_v2.rs` | `read_task.cpp` | Execute, ReadPage |
| `task/write_v2.rs` | `batch_write_task.cpp` | Execute, AllocatePage |
| `shard/shard.rs` | `shard.cpp` | Run, ProcessTask |
| `page/page_mapper.rs` | `page_mapper.cpp` | MapPage, ToFilePage |

## 🏗️ Architecture Notes

### I/O Abstraction (Our Only Innovation)
Created to solve tokio-uring thread safety:
- Trait: `IoBackend`
- Implementations: sync, tokio, thread-pool, io_uring
- Location: `src/io/backend/`

### Page ID Encoding
```rust
FilePageId = (file_id << 32) | page_offset
```

### Key Patterns from C++
- Shared ownership → `Arc<T>`
- Mutex → `RwLock<T>` or `Mutex<T>`
- Coroutines → `async/await` tasks
- Swizzling → Raw pointers in `MemIndexPage`

## ⚡ Quick Commands
```bash
# Build
cargo build

# Test
cargo test

# Check compilation
cargo check

# Run with tokio backend
cargo run -- --io-backend tokio
``` 