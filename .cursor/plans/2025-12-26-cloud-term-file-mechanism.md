# Cloud Mode Term File Mechanism Implementation Plan

## Overview

Implement a term file mechanism in cloud mode to prevent GC threads from deleting files that might be used by other eloqstore instances. The term file (`CURRENT_TERM`) stores the current maximum term in cloud storage, and CAS (Compare-And-Swap) operations ensure atomic updates across multiple instances.

## Current State Analysis

**What exists:**
- `CloudStoreMgr` maintains `process_term_` field (`async_io_manager.h:541-548`)
- `GetManifest()` validates manifest terms against `process_term` (`async_io_manager.cpp:2437-2594`)
- `ExecuteCloudGC()` checks manifest terms before deletion (`file_gc.cpp:577-636`)
- Cloud file operations via `ObjectStore` and `AsyncHttpManager` using signed URLs
- HTTP error classification exists (`object_store.cpp:1404-1426`)

**What's missing:**
- No centralized term file in cloud storage
- No ETag extraction from HTTP response headers
- No CAS support (conditional headers: `If-Match`, `If-None-Match`)
- No term file read/write operations
- Term file validation not integrated into `GetManifest()` and `ExecuteCloudGC()`

**Key Constraints:**
- Must support both AWS S3 and GCS backends
- Must handle backward compatibility (legacy tables without term files)
- Must use existing coroutine-based async I/O model
- CAS conflicts (412/409) require conditional retry with re-validation

## Desired End State

After implementation:
1. **Term file exists**: `CURRENT_TERM` file stored at `{table_path}/CURRENT_TERM` in cloud
2. **GetManifest flow**: Checks term file, validates against `process_term`, CAS updates if needed, then proceeds
3. **GC flow**: Checks term file equals `process_term` before executing GC
4. **CAS support**: Upload operations support conditional headers with ETag-based conflict detection
5. **Backward compatibility**: Legacy tables without term files continue to work

**Verification:**
- Unit tests for term file read/write operations
- Integration tests for CAS conflict handling
- Manual testing with multiple instances updating term file concurrently
- GC only executes when term file matches `process_term`

## What We're NOT Doing

- Not modifying local mode behavior (term file only for cloud mode)
- Not changing existing manifest term validation logic (only adding term file check)
- Not implementing distributed locking (using CAS instead)
- Not modifying file naming conventions (using existing term-aware naming)
- Not changing `process_term` initialization or management

## Implementation Approach

**Strategy**: Incremental implementation with clear phase boundaries
1. **Phase 1**: Add infrastructure (ETag extraction, CAS headers)
2. **Phase 2**: Implement term file operations (read/write with CAS)
3. **Phase 3**: Integrate into `GetManifest()`
4. **Phase 4**: Integrate into `ExecuteCloudGC()`

Each phase is independently testable and can be verified before proceeding.

---

## Phase 1: Add ETag Extraction and CAS Header Support

### Overview
Add infrastructure to extract ETag from HTTP response headers and support conditional headers (`If-Match`, `If-None-Match`) in upload requests.

### Changes Required:

#### 1. Add ETag Storage to Task Classes
**File**: `object_store.h`
**Changes**: Add `etag_` field to `ObjectStore::Task` base class to store ETag from responses

```cpp
class Task {
    // ... existing fields ...
    std::string etag_{};  // ETag from response headers for CAS operations
};
```

#### 2. Add Header Callback for ETag Extraction
**File**: `object_store.cpp`
**Changes**: 
- Add static header callback function to extract ETag from response headers
- Set `CURLOPT_HEADERFUNCTION` and `CURLOPT_HEADERDATA` in `SubmitRequest()`

```cpp
// In AsyncHttpManager class
static size_t HeaderCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
    // Extract ETag header: "ETag: "value"\r\n"
    // Store in task->etag_
}

// In SubmitRequest(), after curl_easy_init():
curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, HeaderCallback);
curl_easy_setopt(easy, CURLOPT_HEADERDATA, task);
```

#### 3. Add Conditional Header Support to UploadTask
**File**: `object_store.h`
**Changes**: Add optional conditional header fields to `UploadTask`

```cpp
class UploadTask : public Task {
    // ... existing fields ...
    std::string if_match_{};      // For If-Match header
    std::string if_none_match_{};  // For If-None-Match header (use "*" for create)
};
```

#### 4. Modify SetupUploadRequest to Support Conditional Headers
**File**: `object_store.cpp`
**Changes**: Add conditional headers to upload request if provided

```cpp
bool AsyncHttpManager::SetupUploadRequest(ObjectStore::UploadTask *task, CURL *easy) {
    // ... existing code ...
    
    // Add conditional headers if provided
    if (!task->if_match_.empty()) {
        std::string header = "If-Match: " + task->if_match_;
        task->headers_ = curl_slist_append(task->headers_, header.c_str());
    } else if (!task->if_none_match_.empty()) {
        std::string header = "If-None-Match: " + task->if_none_match_;
        task->headers_ = curl_slist_append(task->headers_, header.c_str());
    }
    
    // ... rest of existing code ...
}
```

#### 5. Update Error Classification for CAS Conflicts
**File**: `object_store.cpp`
**Changes**: Add helper function to identify CAS retryable errors

```cpp
bool AsyncHttpManager::IsCasRetryable(int64_t response_code) const {
    return response_code == 412 || response_code == 409;  // Precondition Failed or Conflict
}
```

### Success Criteria:

#### Automated Verification:
- [x] Code compiles without errors: `make` or `cmake --build .`
- [x] No linting errors: `clang-format --check` or equivalent
- [ ] Unit tests for header callback ETag extraction pass
- [ ] Unit tests for conditional header setup pass
- [ ] Integration test: Upload with `If-Match` header returns 412 when ETag doesn't match

#### Manual Verification:
- [ ] ETag is correctly extracted from GET response headers
- [ ] `If-Match` header is correctly set in PUT requests
- [ ] `If-None-Match: *` header works for conditional create
- [ ] 412 and 409 responses are correctly identified as CAS conflicts

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation from the human that the manual testing was successful before proceeding to the next phase.

---

## Phase 2: Implement Term File Read/Write Operations

### Overview
Implement functions to read and write the `CURRENT_TERM` file in cloud storage with CAS support.

### Changes Required:

#### 1. Add Term File Constant
**File**: `common.h` or `types.h`
**Changes**: Add constant for term file name

```cpp
static constexpr char CurrentTermFileName[] = "CURRENT_TERM";
```

#### 2. Add Term File Read Function
**File**: `async_io_manager.h`
**Changes**: Add method declaration to `CloudStoreMgr`

```cpp
class CloudStoreMgr : public IouringMgr {
    // ... existing methods ...
    
    // Read term file from cloud, returns {term_value, etag, error}
    // If file doesn't exist (404), returns {0, "", NotFound}
    std::tuple<uint64_t, std::string, KvError> ReadTermFile(
        const TableIdent &tbl_id);
};
```

#### 3. Implement ReadTermFile
**File**: `async_io_manager.cpp`
**Changes**: Implement term file download and parsing

```cpp
std::tuple<uint64_t, std::string, KvError> CloudStoreMgr::ReadTermFile(
    const TableIdent &tbl_id) {
    KvTask *current_task = ThdTask();
    
    // Download CURRENT_TERM file
    ObjectStore::DownloadTask download_task(&tbl_id, CurrentTermFileName);
    download_task.SetKvTask(current_task);
    obj_store_.GetHttpManager()->SubmitRequest(&download_task);
    current_task->WaitIo();
    
    if (download_task.error_ == KvError::NotFound) {
        return {0, "", KvError::NotFound};  // Legacy table
    }
    if (download_task.error_ != KvError::NoError) {
        return {0, "", download_task.error_};
    }
    
    // Parse term value from response_data_
    uint64_t term = 0;
    if (!ParseUint64(download_task.response_data_, term)) {
        return {0, "", KvError::Corrupted};
    }
    
    // Extract ETag from download_task.etag_
    return {term, download_task.etag_, KvError::NoError};
}
```

#### 4. Add Term File Upsert Function
**File**: `async_io_manager.h`
**Changes**: Add method declaration

```cpp
// Upsert term file with limited retry logic (max 10 attempts)
// Returns NoError on success, ExpiredTerm if condition invalid, other errors on failure
KvError UpsertTermFile(const TableIdent &tbl_id,
                       uint64_t process_term);
```

#### 5. Implement UpsertTermFile
**File**: `async_io_manager.cpp`
**Changes**: Implement CAS upsert with conditional retry (max 10 attempts)

```cpp
KvError CloudStoreMgr::UpsertTermFile(const TableIdent &tbl_id,
                                      uint64_t process_term) {
    constexpr uint64_t kMaxAttempts = 10;
    uint64_t attempt = 0;
    while (attempt < kMaxAttempts) {
        // 1. Read term file (get current_term and ETag)
        auto [current_term, etag, read_err] = ReadTermFile(tbl_id);
        
        if (read_err == KvError::NotFound) {
            // Legacy table - create term file with current process_term
            auto [create_err, response_code] = CasCreateTermFile(tbl_id, process_term);
            if (create_err == KvError::NoError) {
                return KvError::NoError;  // Successfully created
            }
            
            // Check if CAS conflict (412 or 409) - file already exists
            if (create_err == KvError::CloudErr &&
                obj_store_.GetHttpManager()->IsCasRetryable(response_code)) {
                ++attempt;
                LOG(WARNING) << "CloudStoreMgr::UpsertTermFile: CAS conflict "
                             << "(HTTP " << response_code << ") when creating "
                             << "term file for table " << tbl_id << " (attempt "
                             << attempt << "/" << kMaxAttempts
                             << "), retrying with backoff";
                continue;  // Retry by reading term file in next iteration
            }
            
            // Non-CAS error - try read again to see if file was created
            std::tie(current_term, etag, read_err) = ReadTermFile(tbl_id);
            if (read_err != KvError::NoError) {
                return create_err;
            }
            // Successfully read after retry, continue with validation
        }
        if (read_err != KvError::NoError) {
            return read_err;
        }
        
        // 2. Validate update condition
        if (current_term > process_term) {
            return KvError::ExpiredTerm;  // Term file is ahead, instance is outdated
        }
        if (current_term == process_term) {
            return KvError::NoError;  // Already up-to-date, no update needed
        }
        
        // 3. Attempt CAS update with If-Match: etag
        auto [err, response_code] = CasUpdateTermFileWithEtag(tbl_id, process_term, etag);
        
        if (err == KvError::NoError) {
            return KvError::NoError;  // Success
        }
        
        // 4. Check if CAS conflict (412 or 409 or 404)
        if (err == KvError::NotFound ||
            (err == KvError::CloudErr &&
             obj_store_.GetHttpManager()->IsCasRetryable(response_code))) {
            ++attempt;
            LOG(WARNING) << "CloudStoreMgr::UpsertTermFile: CAS conflict "
                         << "(HTTP " << response_code << ") for table "
                         << tbl_id << " (attempt " << attempt << "/"
                         << kMaxAttempts << "), current_term=" << current_term
                         << ", process_term=" << process_term
                         << ", retrying with backoff";
            continue;
        }
        
        // Non-CAS error - return immediately
        LOG(ERROR) << "CloudStoreMgr::UpsertTermFile: non-retryable error "
                   << ErrorString(err) << " for table " << tbl_id;
        return err;
    }
    
    // Exceeded max attempts
    LOG(ERROR) << "CloudStoreMgr::UpsertTermFile: exceeded max attempts ("
               << kMaxAttempts << ") for table " << tbl_id;
    return KvError::CloudErr;
}
```

#### 6. Add Helper Functions for CAS Create and Update
**File**: `async_io_manager.cpp`
**Changes**: Implement CAS create and update with ETag, both return {error, response_code}

```cpp
// CAS create term file (only if doesn't exist)
// Returns {error, response_code}
std::pair<KvError, int64_t> CloudStoreMgr::CasCreateTermFile(
    const TableIdent &tbl_id, uint64_t process_term) {
    KvTask *current_task = ThdTask();
    std::string term_str = std::to_string(process_term);
    
    ObjectStore::UploadTask upload_task(&tbl_id, CurrentTermFileName);
    upload_task.data_buffer_ = term_str;
    upload_task.if_none_match_ = "*";  // Only create if doesn't exist
    upload_task.SetKvTask(current_task);
    
    obj_store_.GetHttpManager()->SubmitRequest(&upload_task);
    current_task->WaitIo();
    
    return {upload_task.error_, upload_task.response_code_};
}

// CAS update term file with specific ETag
// Returns {error, response_code}
std::pair<KvError, int64_t> CloudStoreMgr::CasUpdateTermFileWithEtag(
    const TableIdent &tbl_id, uint64_t process_term, const std::string &etag) {
    KvTask *current_task = ThdTask();
    std::string term_str = std::to_string(process_term);
    
    ObjectStore::UploadTask upload_task(&tbl_id, CurrentTermFileName);
    upload_task.data_buffer_ = term_str;
    upload_task.if_match_ = etag;  // Only update if ETag matches
    upload_task.SetKvTask(current_task);
    
    obj_store_.GetHttpManager()->SubmitRequest(&upload_task);
    current_task->WaitIo();
    
    return {upload_task.error_, upload_task.response_code_};
}
```

### Success Criteria:

#### Automated Verification:
- [x] Code compiles without errors
- [x] Response code stored in Task for CAS conflict detection
- [x] UpsertTermFile uses IsCasRetryable to properly detect 412/409 conflicts
- [x] UpsertTermFile uses limited retry (max 10 attempts) instead of infinite retry
- [x] CasCreateTermFile returns {error, response_code} pair
- [x] CasUpdateTermFileWithEtag returns {error, response_code} pair
- [ ] Unit tests for `ReadTermFile()` pass (with mocked HTTP responses)
- [ ] Unit tests for `UpsertTermFile()` limited retry logic pass
- [ ] Unit tests verify error logging before retries
- [ ] Unit tests for `CasCreateTermFile()` pass
- [ ] Integration test: Read term file from cloud storage
- [ ] Integration test: CAS update succeeds when ETag matches
- [ ] Integration test: CAS update retries indefinitely on 412/409 conflicts until success

#### Manual Verification:
- [ ] Term file can be read from cloud storage
- [ ] Term file can be created in cloud storage
- [ ] CAS update works correctly with valid ETag
- [ ] CAS update retries up to 10 times on 412/409 conflicts
- [ ] Error logs are printed before each retry attempt with correct information
- [ ] CAS update returns `ExpiredTerm` when condition becomes invalid
- [ ] Legacy tables (no term file) handled gracefully

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation from the human that the manual testing was successful before proceeding to the next phase.

---

## Phase 3: Integrate Term File Check in GetManifest

### Overview
Modify `CloudStoreMgr::GetManifest()` to check and update the term file before manifest selection.

### Changes Required:

#### 1. Modify GetManifest to Check Term File
**File**: `async_io_manager.cpp`
**Changes**: Add term file check at the beginning of `GetManifest()`

```cpp
std::pair<ManifestFilePtr, KvError> CloudStoreMgr::GetManifest(
    const TableIdent &tbl_id) {
    KvTask *current_task = ThdTask();
    uint64_t process_term = ProcessTerm();
    
    // Check and update term file
    KvError term_err = UpsertTermFile(tbl_id, process_term);
    if (term_err != KvError::NoError) {
        return {nullptr, term_err};
    }
    
    // Continue with existing manifest selection logic
    // ... existing code from line 2449 onwards ...
}
```

### Success Criteria:

#### Automated Verification:
- [x] Code compiles without errors
- [ ] Existing GetManifest tests still pass
- [ ] New test: GetManifest with term file (term < process_term) updates term file
- [ ] New test: GetManifest with term file (term == process_term) skips update
- [ ] New test: GetManifest with term file (term > process_term) returns ExpiredTerm
- [ ] New test: GetManifest with legacy table (no term file) creates term file
- [ ] Integration test: Multiple instances calling GetManifest concurrently

#### Manual Verification:
- [ ] GetManifest works correctly with existing term file
- [ ] GetManifest creates term file for legacy tables
- [ ] GetManifest updates term file when term < process_term
- [ ] GetManifest skips update when term == process_term (verify no unnecessary CAS)
- [ ] GetManifest returns ExpiredTerm when term > process_term
- [ ] Concurrent GetManifest calls from multiple instances work correctly
- [ ] No regressions in manifest selection logic

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation from the human that the manual testing was successful before proceeding to the next phase.

---

## Phase 4: Integrate Term File Check in GC

### Overview
Modify `FileGarbageCollector::ExecuteCloudGC()` to check the term file before executing GC operations.

### Changes Required:

#### 1. Modify ExecuteCloudGC to Check Term File
**File**: `file_gc.cpp`
**Changes**: Add term file check at the beginning of `ExecuteCloudGC()`

```cpp
KvError ExecuteCloudGC(const TableIdent &tbl_id,
                       const std::unordered_set<FileId> &retained_files,
                       CloudStoreMgr *cloud_mgr) {
    // Check term file before proceeding
    uint64_t process_term = cloud_mgr->ProcessTerm();
    auto [term_file_term, etag, err] = cloud_mgr->ReadTermFile(tbl_id);
    
    if (err == KvError::NotFound) {
        // Legacy table - proceed with existing manifest term validation
        // (backward compatible behavior)
        LOG(INFO) << "ExecuteCloudGC: term file not found for table "
                 << tbl_id << ", using legacy manifest term validation";
    } else if (err != KvError::NoError) {
        LOG(ERROR) << "ExecuteCloudGC: failed to read term file for table "
                  << tbl_id << " : " << ErrorString(err);
        return err;
    } else {
        // Term file exists - validate
        if (term_file_term != process_term) {
            LOG(WARNING) << "ExecuteCloudGC: term file term " << term_file_term
                        << " != process_term " << process_term
                        << " for table " << tbl_id << ", skipping GC";
            return KvError::ExpiredTerm;
        }
        // term_file_term == process_term, proceed with GC
    }
    
    // Continue with existing GC logic
    // 1. list all files in cloud.
    std::vector<std::string> cloud_files;
    KvError list_err = ListCloudFiles(tbl_id, cloud_files, cloud_mgr);
    // ... rest of existing code ...
}
```

#### 2. Add ReadTermFile Public Method
**File**: `async_io_manager.h`
**Changes**: Make `ReadTermFile` accessible to `FileGarbageCollector`

```cpp
class CloudStoreMgr : public IouringMgr {
    // ... existing public methods ...
    
    // Read term file from cloud (for GC and other operations)
    std::tuple<uint64_t, std::string, KvError> ReadTermFile(
        const TableIdent &tbl_id);
};
```

### Success Criteria:

#### Automated Verification:
- [x] Code compiles without errors
- [ ] Existing GC tests still pass
- [ ] New test: GC with term file (term == process_term) proceeds
- [ ] New test: GC with term file (term != process_term) returns ExpiredTerm
- [ ] New test: GC with legacy table (no term file) proceeds with manifest validation
- [ ] Integration test: GC only executes when term file matches process_term

#### Manual Verification:
- [ ] GC executes correctly when term file == process_term
- [ ] GC skips execution when term file != process_term
- [ ] GC works correctly for legacy tables (backward compatible)
- [ ] No files deleted incorrectly when term file doesn't match
- [ ] Multiple instances: GC from one instance doesn't interfere with another

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation from the human that the manual testing was successful before proceeding.

---

## Testing Strategy

### Unit Tests

#### Term File Operations:
- `ReadTermFile()` with existing file
- `ReadTermFile()` with non-existent file (404)
- `ReadTermFile()` with corrupted content
- `CasCreateTermFile()` success case
- `CasCreateTermFile()` when file already exists (412)
- `UpsertTermFile()` success case
- `UpsertTermFile()` with ETag mismatch (412)
- `UpsertTermFile()` limited retry logic (max 10 attempts) with error logging
- `UpsertTermFile()` returns ExpiredTerm when condition invalid
- `UpsertTermFile()` returns CloudErr when max attempts exceeded

#### GetManifest Integration:
- GetManifest with term file (term < process_term) - updates
- GetManifest with term file (term == process_term) - skips update
- GetManifest with term file (term > process_term) - returns ExpiredTerm
- GetManifest with legacy table - creates term file
- GetManifest CAS conflict handling

#### GC Integration:
- GC with term file (term == process_term) - proceeds
- GC with term file (term != process_term) - returns ExpiredTerm
- GC with legacy table - proceeds with manifest validation

### Integration Tests

#### Concurrent Operations:
- Multiple instances updating term file simultaneously
- GetManifest from multiple instances concurrently
- GC from one instance while another updates term file

#### Error Scenarios:
- Network errors during term file operations
- Cloud storage unavailable
- Corrupted term file content
- ETag extraction failures

### Manual Testing Steps

1. **Single Instance:**
   - Start eloqstore instance with cloud mode
   - Verify CURRENT_TERM file created in cloud
   - Call GetManifest, verify term file updated if needed
   - Trigger GC, verify it executes when term matches

2. **Multiple Instances:**
   - Start two eloqstore instances with different process_term
   - Instance A (term=5) calls GetManifest
   - Instance B (term=6) calls GetManifest concurrently
   - Verify term file eventually reflects term=6
   - Verify GC from instance A is blocked when term file != 5

3. **Legacy Table Migration:**
   - Use existing table without CURRENT_TERM file
   - Call GetManifest, verify term file is created
   - Verify GC works with legacy validation initially, then with term file

4. **Error Handling:**
   - Simulate 412/409 conflicts
   - Verify retry logic works correctly
   - Verify ExpiredTerm returned when appropriate

## Performance Considerations

- **Term file operations**: Additional HTTP requests (1 read + potentially 1 write per GetManifest)
- **CAS retries**: May add latency under high contention, but necessary for correctness
- **Optimization**: Skip CAS update when term1 == process_term (already implemented in plan)
- **Caching**: Consider caching term file value locally, but must invalidate on conflicts

## Migration Notes

- **Backward compatibility**: Legacy tables without term files continue to work
- **Gradual adoption**: Term file created on first GetManifest() call
- **No data migration needed**: Term file is created automatically when needed
- **Rollback**: If issues occur, can temporarily disable term file checks (feature flag)

## References

- Research document: `.cursor/thoughts/shared/research/2025-12-26-cloud-term-file-mechanism.md`
- Current GetManifest implementation: `async_io_manager.cpp:2437-2594`
- Current GC implementation: `file_gc.cpp:577-636`
- Cloud file operations: `object_store.cpp:1017-1091`
- Error handling: `object_store.cpp:1404-1426`

