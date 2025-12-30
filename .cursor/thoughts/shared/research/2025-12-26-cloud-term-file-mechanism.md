---
date: 2025-12-26T14:11:36+08:00
researcher: Auto
git_commit: 08b010a7ad1148592659bc3f0e0dd5b3b00b1c04
branch: eloqstore-multi-write-term-gc
repository: eloqdata/eloqstore
topic: "Cloud Mode Term File Mechanism for GC Safety"
tags: [research, codebase, cloud-mode, term-file, gc, GetManifest, CAS]
status: complete
last_updated: 2025-12-26
last_updated_by: Auto
last_updated_note: "Updated CAS retry strategy from infinite to limited (max 10 attempts), renamed CasUpdateTermFile to UpsertTermFile, updated function return types to include response_code, and removed recursive call from CasCreateTermFile"
---

# Research: Cloud Mode Term File Mechanism for GC Safety

**Date**: 2025-12-26T14:11:36+08:00
**Researcher**: Auto
**Git Commit**: 08b010a7ad1148592659bc3f0e0dd5b3b00b1c04
**Branch**: eloqstore-multi-write-term-gc
**Repository**: eloqdata/eloqstore

## Research Question

How to implement a term file mechanism in cloud mode to prevent GC threads from deleting files that might be used by other eloqstore instances? The requirements are:
1. In cloud mode, introduce a term file to store the current maximum term
2. In GetManifest (cloud mode): check if term file exists, compare with process_term, use CAS to update, then download manifest
3. In GC (cloud mode): check cloud term file content, only execute GC if it equals current process_term

## Summary

The codebase currently has:
- **ProcessTerm management**: `CloudStoreMgr` maintains a `process_term_` field set via `SetProcessTerm()` and accessed via `ProcessTerm()`
- **GetManifest in cloud mode**: `CloudStoreMgr::GetManifest()` lists cloud files, finds the best manifest term, checks if it's greater than `process_term`, and downloads it
- **GC in cloud mode**: `FileGarbageCollector::ExecuteCloudGC()` checks manifest terms against `process_term` before deletion
- **Cloud file operations**: Upload/download via `ObjectStore` and `AsyncHttpManager` using signed URLs
- **No existing term file**: Currently no centralized term file exists in cloud storage
- **No CAS mechanism**: Current upload operations don't use conditional headers (ETag/If-Match) for CAS

The implementation would need to:
1. Create a term file (e.g., `term` or `.term`) in cloud storage per table
2. Add CAS support to upload operations (using HTTP conditional headers)
3. Modify `GetManifest()` to read/update term file before manifest selection
4. Modify `ExecuteCloudGC()` to check term file before GC execution

## Detailed Findings

### ProcessTerm Management

**Location**: `async_io_manager.h:541-548`, `async_io_manager.cpp:2446`

`CloudStoreMgr` maintains a `process_term_` field:
- Set during initialization via `Shard::Init()` calling `SetProcessTerm(term)` (`shard.cpp:42`)
- Accessed via `ProcessTerm()` which returns `process_term_`
- Used for term-aware file naming and validation

```cpp
void SetProcessTerm(uint64_t term) {
    process_term_ = term;
}
uint64_t ProcessTerm() const override {
    return process_term_;
}
```

**File**: `async_io_manager.h:541-548`, `shard.cpp:32-47`

### GetManifest Implementation in Cloud Mode

**Location**: `async_io_manager.cpp:2437-2594`

`CloudStoreMgr::GetManifest()` currently:
1. Lists all files in cloud for the table (`ObjectStore::ListTask`)
2. Parses manifest filenames to extract terms
3. Finds the best (maximum) manifest term
4. Checks if `selected_term > process_term` and returns `ExpiredTerm` if true
5. Downloads the selected manifest file
6. If `selected_term != process_term`, promotes it by renaming locally and uploading as `manifest_<process_term>`

**Key code** (`async_io_manager.cpp:2446-2527`):
```cpp
uint64_t process_term = ProcessTerm();
uint64_t selected_term = 0;
// ... list files ...
if (selected_term > process_term) {
    return {nullptr, KvError::ExpiredTerm};
}
```

**File**: `async_io_manager.cpp:2437-2594`

### GC Implementation in Cloud Mode

**Location**: `file_gc.cpp:577-636`

`FileGarbageCollector::ExecuteCloudGC()` currently:
1. Lists all cloud files
2. Classifies files (archive, data, manifest)
3. Checks if any manifest term > `process_term` and returns `ExpiredTerm` if found
4. Gets archived max file ID
5. Deletes unreferenced files

**Key code** (`file_gc.cpp:601-609`):
```cpp
auto process_term = cloud_mgr->ProcessTerm();
for (auto term : manifest_terms) {
    if (term > process_term) {
        return KvError::ExpiredTerm;
    }
}
```

**File**: `file_gc.cpp:577-636`

### Cloud File Operations

**Location**: `object_store.cpp`, `async_io_manager.cpp`

**Upload**: `CloudStoreMgr::UploadFiles()` reads local files and uploads via `ObjectStore::UploadTask` which uses signed PUT URLs (`object_store.cpp:1034-1069`)

**Download**: `CloudStoreMgr::DownloadFile()` downloads via `ObjectStore::DownloadTask` which uses signed GET URLs (`object_store.cpp:1017-1032`)

**Delete**: Uses `ObjectStore::DeleteTask` with signed DELETE URLs (`object_store.cpp:1071-1091`)

**Current upload setup** (`object_store.cpp:1034-1069`):
- Creates signed URL via `backend_->CreateSignedUrl(CloudHttpMethod::kPut, key)`
- Sets `CURLOPT_CUSTOMREQUEST` to "PUT"
- Sets `CURLOPT_POSTFIELDS` with data
- No conditional headers (ETag, If-Match, If-None-Match) are currently used

**Files**: `object_store.cpp:1017-1091`, `async_io_manager.cpp:2944-2964`, `async_io_manager.cpp:3332-3821`

### Cloud Backend Architecture

**Location**: `object_store.cpp:475-764`

The system supports multiple cloud backends:
- `AwsCloudBackend`: AWS S3-compatible storage
- `GcsCloudBackend`: Google Cloud Storage (extends `AwsCloudBackend`)

Both use signed URLs for operations. The backend is created via `CreateBackend()` based on `options_->cloud_provider`.

**File**: `object_store.cpp:475-764`

### File Naming Conventions

**Location**: `common.h:218-283`

Files use term-aware naming:
- Manifest: `manifest_<term>` (e.g., `manifest_5`)
- Data: `data_<file_id>_<term>` (e.g., `data_123_5`)
- Archive: `manifest_<term>_<timestamp>` (e.g., `manifest_5_1234567890`)

**File**: `common.h:218-283`

### Error Handling

**Location**: `error.h:14-34`

`ExpiredTerm` error exists and is used when:
- Manifest term > process_term in `GetManifest()`
- Manifest term > process_term in `ExecuteCloudGC()`

**File**: `error.h:14-34`, `error.h:70-71`

### Cloud Path Structure

**Location**: `object_store.cpp:412-450`, `object_store.cpp:956-978`

Cloud paths are structured as: `{bucket}/{prefix}/{table_id}/{filename}`

The `ComposeKey()` method builds keys by combining:
- `cloud_path_.prefix` (from `cloud_store_path` option)
- Table identifier (via `tbl_id->ToString()`)
- Filename

**File**: `object_store.cpp:412-450`, `object_store.cpp:956-978`

## Code References

- `async_io_manager.h:541-548` - `SetProcessTerm()` and `ProcessTerm()` methods
- `async_io_manager.cpp:2437-2594` - `CloudStoreMgr::GetManifest()` implementation
- `file_gc.cpp:577-636` - `FileGarbageCollector::ExecuteCloudGC()` implementation
- `file_gc.cpp:601-609` - Term validation in GC
- `object_store.cpp:1017-1091` - Cloud file operation setup (download, upload, delete)
- `object_store.cpp:1034-1069` - Upload request setup (no CAS headers currently)
- `object_store.cpp:475-764` - Cloud backend classes
- `object_store.cpp:956-978` - `ComposeKey()` for building cloud paths
- `shard.cpp:32-47` - ProcessTerm initialization in `Shard::Init()`
- `error.h:14-34` - Error definitions including `ExpiredTerm`
- `common.h:218-283` - File naming functions

## Architecture Documentation

### Current Cloud Mode Flow

1. **Initialization**: `Shard::Init()` sets `process_term_` on `CloudStoreMgr`
2. **GetManifest**: Lists files, finds best term, validates against `process_term`, downloads
3. **GC**: Lists files, validates terms, deletes unreferenced files
4. **File Operations**: All use signed URLs via `AsyncHttpManager` and `CloudBackend`

### Term File Requirements

To implement the term file mechanism:

1. **Term File Location**: Store as `{table_path}/CURRENT_TERM` in cloud
2. **Term File Format**: Simple text file containing the term number (e.g., "5")
3. **CAS Update**: Use HTTP conditional headers:
   - Read term file to get current value
   - Use `If-Match: <ETag>` or `If-None-Match: *` for CAS
   - On conflict (412 Precondition Failed or 409 Conflict), retry or return error
4. **GetManifest Changes**:
   - Check if term file exists (download it)
   - Parse term value (term1)
   - If term1 > process_term, return `ExpiredTerm`
   - If term1 == process_term, skip update and proceed with manifest selection/download
   - If term1 < process_term, CAS update term file with process_term, then proceed with manifest selection/download
5. **GC Changes**:
   - Download term file
   - Parse term value
   - Only proceed if term == process_term
   - Otherwise, skip GC or return error

### CAS Implementation Approach

For CAS updates, the system would need:
1. **Read with ETag**: Download term file and capture ETag from response headers
2. **Conditional Write**: Upload with `If-Match: <ETag>` header
3. **Conflict Handling**: On 412 Precondition Failed or 409 Conflict, retry or fail

Current `SetupUploadRequest()` would need modification to:
- Accept optional conditional headers
- Set `CURLOPT_HTTPHEADER` with `If-Match` or `If-None-Match`
- Handle 412 and 409 responses as conflicts

## Open Questions and Answers

### 1. Term File Naming: Should it be `.term`, `term`, or `term.txt`?

**Answer**: Use **`CURRENT_TERM`** (uppercase, descriptive name).

**Decision**: The term file will be named `CURRENT_TERM` to clearly indicate its purpose as the current maximum term value.

**Rationale**:
- **Descriptive naming**: `CURRENT_TERM` clearly indicates the file's purpose
- **Uppercase convention**: Follows common practice for important metadata files
- **No ambiguity**: Avoids confusion with other potential term-related files
- **Cloud storage compatibility**: Both S3 and GCS support uppercase filenames without issues

**Implementation**: Store as `{table_path}/CURRENT_TERM` in cloud storage.

### 2. CAS Retry Strategy: How many retries for CAS conflicts?

**Answer**: **Limited retry with conditional validation (max 10 attempts)** - if CAS fails, re-read term file and validate before retrying up to 10 times until success, condition becomes invalid, or max attempts exceeded.

**Decision**: When CAS update fails (412 Precondition Failed or 409 Conflict), the system should:
1. Re-read the term file to get the latest term value
2. Check if the latest term value < `process_term` (update condition)
3. If condition still valid: log the error and retry CAS update with new ETag (up to 10 attempts)
4. If condition invalid (latest term >= `process_term`): return `ExpiredTerm` error immediately
5. If max attempts exceeded: return `CloudErr` error

**Rationale**:
- **CAS conflict indicates file was modified**: A 412 error means another process updated the term file
- **Re-validation is necessary**: The term file content may have changed, so we must re-check the update condition
- **Limited retries prevent infinite loops**: Maximum 10 attempts prevents the function from hanging indefinitely under high contention
- **Error logging provides visibility**: Log warnings before each retry to help diagnose contention issues
- **Efficient error handling**: If the condition is no longer valid, there's no point retrying - return error immediately
- **Function renamed to UpsertTermFile**: Better reflects the function's behavior (both create and update)

**Implementation Flow**:
```cpp
// CAS upsert with limited conditional retry (max 10 attempts)
constexpr uint64_t kMaxAttempts = 10;
uint64_t attempt = 0;
while (attempt < kMaxAttempts) {
    // 1. Read term file (get current_term and ETag)
    auto [current_term, etag, read_err] = ReadTermFile(tbl_id);
    
    if (read_err == KvError::NotFound) {
        // Try CAS create
        auto [create_err, response_code] = CasCreateTermFile(tbl_id, process_term);
        if (create_err == KvError::NoError) {
            return KvError::NoError;
        }
        // Handle CAS conflict from create
        if (create_err == KvError::CloudErr && IsCasRetryable(response_code)) {
            ++attempt;
            continue;  // Retry by reading in next iteration
        }
        // ... handle other errors
    }
    
    // 2. Validate update condition
    if (current_term > process_term) {
        return KvError::ExpiredTerm;
    }
    if (current_term == process_term) {
        return KvError::NoError;
    }
    
    // 3. Attempt CAS update with If-Match: etag
    auto [err, response_code] = CasUpdateTermFileWithEtag(tbl_id, process_term, etag);
    
    if (err == KvError::NoError) {
        return KvError::NoError;
    }
    
    // 4. Check if CAS conflict (412 or 409 or 404)
    if (err == KvError::NotFound ||
        (err == KvError::CloudErr && IsCasRetryable(response_code))) {
        ++attempt;
        LOG(WARNING) << "UpsertTermFile: CAS conflict (HTTP " << response_code
                     << ") for table " << tbl_id << " (attempt " << attempt
                     << "/" << kMaxAttempts << "), retrying";
        continue;
    }
    
    // Non-CAS error - return immediately
    return err;
}

// Exceeded max attempts
return KvError::CloudErr;
```

**Key Points**:
- **Limited retries**: Maximum 10 attempts to prevent infinite loops
- **Re-read on each retry**: Always get the latest term file content before retrying
- **Validate condition**: Check `current_term < process_term` before each CAS attempt
- **Error logging**: Log warning messages with attempt count (e.g., "attempt 5/10") before each retry
- **Immediate failure**: If condition invalid, return error without retrying
- **Function name**: `UpsertTermFile` better reflects create-or-update behavior
- **Return values**: `CasCreateTermFile` and `CasUpdateTermFileWithEtag` return `{error, response_code}` pairs

### 3. Term File Initialization: Who creates the initial term file? First instance?

**Answer**: **First instance that successfully performs CAS create** (using `If-None-Match: *`).

**Rationale**:
- **CAS create pattern**: Use `If-None-Match: *` header to atomically create the file only if it doesn't exist
- **Race condition handling**: Multiple instances can try to create simultaneously; only one succeeds
- **Initial value**: Set to `process_term` of the creating instance
- **Fallback**: If CAS create fails (file exists), fall back to read-and-update flow

**Implementation Flow**:
1. Try CAS create with `If-None-Match: *` and initial term value
2. If 412 Precondition Failed (file exists), read current term file
3. Proceed with normal CAS update flow

### 4. Backward Compatibility: How to handle tables without term files (legacy)?

**Answer**: **Graceful degradation**: If term file doesn't exist (404), treat as legacy table and skip term file checks.

**Rationale**:
- **Existing pattern**: Codebase shows legacy format rejection but with existence checks (e.g., `common.h:129-130` rejects legacy format but checks first)
- **Migration path**: Allow existing tables to work without term files initially
- **Gradual adoption**: Term file is created on first `GetManifest()` call for legacy tables
- **GC behavior**: For legacy tables without term file, GC should proceed with existing term validation (check manifest terms)

**Implementation**:
- `GetManifest()`: If term file 404, create it with current `process_term`
- `ExecuteCloudGC()`: If term file 404, proceed with existing manifest term validation (backward compatible)
- Log warning when operating in legacy mode

### 5. Concurrent Updates: What happens if multiple instances try to update term file simultaneously?

**Answer**: **CAS handles this**: Only one instance succeeds per attempt; others retry.

**Rationale**:
- **CAS semantics**: `If-Match: <ETag>` ensures atomic update - only succeeds if ETag matches
- **Expected behavior**: Multiple instances with same `process_term` can all succeed (updating to same value)
- **Conflict scenario**: If instances have different `process_term` values:
  - Instance A (term=5) reads term file (term=5, ETag=E1)
  - Instance B (term=6) reads term file (term=5, ETag=E1)
  - Instance A updates: `If-Match: E1` → succeeds, term=5
  - Instance B updates: `If-Match: E1` → fails (412), retries, reads new term=5, updates to term=6
- **Term file always reflects maximum term**: The CAS retry mechanism ensures the term file eventually reflects the highest `process_term` among active instances

**Implementation**: Standard CAS with retry handles this automatically.

### 6. ETag Support: Do all cloud backends (S3, GCS) support ETags consistently?

**Answer**: **Yes, both S3 and GCS support ETags**, but implementation details differ slightly.

**Rationale**:
- **AWS S3**: Returns `ETag` header in GET responses; supports `If-Match` and `If-None-Match` for PUT operations
- **Google Cloud Storage**: Returns `ETag` header (same as S3-compatible); supports conditional headers
- **Current code**: Uses AWS SDK for S3 and S3-compatible API for GCS (`object_store.cpp:709-751`)
- **Implementation note**: Need to capture ETag from GET response headers and use in PUT `If-Match` header

**Implementation**:
- Extract `ETag` from `CURLINFO_RESPONSE_CODE` response headers after download
- Set `If-Match: <ETag>` header in upload request
- Handle 412 Precondition Failed as CAS conflict

### 7. Error Handling: Should CAS failures be retryable or immediately fail?

**Answer**: **CAS failures should be retryable indefinitely** (with conditional validation), but distinguish from other errors.

**Rationale**:
- **CAS conflicts are transient**: Another instance updating is temporary, retry makes sense
- **Limited retries (max 10) prevent infinite loops**: Maximum 10 attempts prevents the function from hanging indefinitely while allowing sufficient retries under contention
- **Existing pattern**: Codebase distinguishes retryable vs non-retryable errors (`object_store.cpp:1184-1222`)
- **HTTP 412 is retryable**: Precondition Failed (412) indicates conflict, should retry
- **Error logging provides visibility**: Log warnings before each retry to help diagnose contention issues
- **Other errors**: Network errors, 404 (file not found for create), etc. have different handling

**Implementation**:
- **Retryable**: 412 Precondition Failed or 409 Conflict (CAS conflict) - retry up to 10 times with error logging
- **Not retryable**: 404 on update (file deleted) - return error
- **Special case**: 404 on create attempt - proceed to read-and-update (file created by another instance)
- **Limited retries (max 10)**: Retry up to 10 times as long as the update condition (`current_term < process_term`) remains valid

**Note on HTTP 409 Conflict**:
- **Current codebase behavior**: HTTP 409 is classified as `KvError::CloudErr` and is not automatically retryable (`object_store.cpp:1411`, `object_store.cpp:1388-1402`)
- **CAS context**: For CAS operations, 409 should be treated similarly to 412:
  - Both indicate a conflict condition
  - 412: Precondition Failed (If-Match/If-None-Match condition not met)
  - 409: Conflict (resource state conflict, may be used by some backends instead of 412)
- **Handling**: When CAS operation returns 409, follow the same retry strategy as 412:
  1. Re-read term file to get latest content
  2. Validate update condition (`current_term < process_term`)
  3. If condition valid: retry with new ETag
  4. If condition invalid: return `ExpiredTerm` immediately

**Error Classification**:
```cpp
bool IsCasRetryable(int64_t response_code) const {
    return response_code == 412 || response_code == 409;  // Precondition Failed or Conflict
}
```

## Related Components

- **ObjectStore**: Handles cloud storage operations (`object_store.h`, `object_store.cpp`)
- **AsyncHttpManager**: Manages HTTP requests to cloud (`object_store.h:209-280`)
- **CloudStoreMgr**: Cloud mode IO manager (`async_io_manager.h:479-679`)
- **FileGarbageCollector**: GC implementation (`file_gc.h`, `file_gc.cpp`)
- **CloudBackend**: Abstract interface for cloud providers (`object_store.h:190-207`)

## Follow-up Research 2025-12-26

### Design Decisions

Based on implementation requirements, the following design decisions were made:

#### 1. Term File Naming: `CURRENT_TERM`

**Decision**: The term file will be named `CURRENT_TERM` (uppercase).

**Location**: `{table_path}/CURRENT_TERM` in cloud storage.

**Rationale**:
- Clear, descriptive name that indicates purpose
- Uppercase convention for important metadata files
- Avoids ambiguity with other potential term-related files

#### 2. CAS Retry Strategy: Limited Conditional Retry with Re-validation (Max 10 Attempts)

**Decision**: CAS retry strategy must re-validate the update condition after each conflict and retry up to 10 times until success, condition becomes invalid, or max attempts exceeded.

**Behavior**:
1. When CAS update fails (412 Precondition Failed or 409 Conflict), re-read the term file to get latest content
2. Validate update condition: `current_term < process_term`
3. If condition valid: log error warning and retry CAS update with new ETag (up to 10 attempts)
4. If condition invalid: return `ExpiredTerm` error immediately (no retry)
5. If max attempts exceeded: return `CloudErr` error

**Rationale**:
- CAS conflict (412 or 409) indicates another process modified the term file or resource state conflict
- The term file content may have changed, requiring re-validation
- Limited retries (max 10) prevent infinite loops while allowing sufficient attempts under contention
- Error logging provides visibility into contention and helps diagnose issues
- If condition is no longer valid (term >= process_term), retrying is pointless
- Only retry when the update condition remains valid
- Function renamed to `UpsertTermFile` to better reflect create-or-update behavior

**Implementation Notes**:
- Use limited retry loop (`while (attempt < kMaxAttempts)`) with max 10 attempts
- Each retry attempt must re-read the term file to get latest term value and ETag
- Validate `current_term < process_term` before each CAS attempt
- Log warning messages before each retry with attempt number (e.g., "attempt 5/10") and current state
- Return `CloudErr` when max attempts exceeded
- Return `ExpiredTerm` immediately if condition becomes invalid
- Retry only when condition is still valid after re-reading
- `CasCreateTermFile` and `CasUpdateTermFileWithEtag` return `{error, response_code}` pairs

**Example Flow**:
```
Attempt 1:
  - Read CURRENT_TERM: term=5, ETag=E1
  - Validate: 5 < 7 (process_term) ✓
  - CAS update with If-Match: E1 → 412 (conflict)
  - Log: WARNING "UpsertTermFile: CAS conflict (HTTP 412) for table X (attempt 1/10), current_term=5, process_term=7, retrying with backoff"

Attempt 2:
  - Re-read CURRENT_TERM: term=6, ETag=E2 (updated by another process)
  - Validate: 6 < 7 (process_term) ✓
  - CAS update with If-Match: E2 → Success

Alternative scenario:
Attempt 1:
  - Read CURRENT_TERM: term=5, ETag=E1
  - Validate: 5 < 7 (process_term) ✓
  - CAS update with If-Match: E1 → 412 (conflict)
  - Log: WARNING "UpsertTermFile: CAS conflict (HTTP 412) for table X (attempt 1/10), current_term=5, process_term=7, retrying with backoff"

Attempt 2:
  - Re-read CURRENT_TERM: term=8, ETag=E2 (updated by another process)
  - Validate: 8 < 7 (process_term) ✗
  - Return ExpiredTerm immediately (no retry)
```

#### 3. HTTP 409 Conflict Error Handling

**Decision**: HTTP 409 Conflict errors should be treated the same as 412 Precondition Failed for CAS operations.

**Current Codebase Behavior**:
- HTTP 409 is classified as `KvError::CloudErr` (`object_store.cpp:1411`)
- 409 is not in the retryable HTTP codes list (`object_store.cpp:1388-1402`)
- 409 typically indicates resource state conflict

**CAS Context**:
- **412 Precondition Failed**: Condition header (If-Match/If-None-Match) not satisfied
- **409 Conflict**: Resource state conflict, may be used by some cloud backends instead of 412
- Both indicate a conflict that requires re-reading and re-validation

**Handling Strategy**:
When CAS operation returns 409, follow the same retry strategy as 412:
1. Re-read term file to get latest term value and ETag
2. Validate update condition: `current_term < process_term`
3. If condition valid: retry CAS update with new ETag
4. If condition invalid: return `ExpiredTerm` immediately

**Implementation**:
```cpp
// CAS error handling
if (response_code == 412 || response_code == 409) {
    // CAS conflict - re-read and validate
    auto [current_term, etag] = ReadTermFile(tbl_id);
    if (current_term >= process_term) {
        return KvError::ExpiredTerm;  // Condition no longer valid
    }
    // Retry with new ETag
}
```

**Rationale**:
- Some cloud backends may return 409 instead of 412 for conditional header conflicts
- Both errors indicate the resource was modified by another process
- Same retry logic applies: re-read, validate, retry if condition still valid
