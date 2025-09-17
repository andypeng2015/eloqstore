//! FFI bindings for C compatibility
//!
//! This module provides C-compatible bindings for EloqStore to enable
//! interoperability with existing C++ code.

use std::ffi::{c_char, CStr};
use std::ptr;
use std::slice;
use std::sync::Arc;

use crate::config::KvOptions;
use crate::store::EloqStore;
use crate::api::request::{ReadRequest, BatchWriteRequest, WriteEntry, WriteOp, TableIdent};
use bytes::Bytes;

/// Opaque handle for the store
pub struct EloqStoreHandle {
    store: Box<EloqStore>,
    runtime: tokio::runtime::Runtime,
}

/// Create a new EloqStore instance
///
/// # Safety
/// The caller must ensure that the data_dir string is valid UTF-8
#[no_mangle]
pub unsafe extern "C" fn eloqstore_new(data_dir: *const c_char) -> *mut EloqStoreHandle {
    if data_dir.is_null() {
        return ptr::null_mut();
    }

    let data_dir_str = match CStr::from_ptr(data_dir).to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let mut options = KvOptions::default();
    options.data_dirs = vec![std::path::PathBuf::from(data_dir_str)];
    let options = Arc::new(options);

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };

    let store = match EloqStore::new((*options).clone()) {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    Box::into_raw(Box::new(EloqStoreHandle {
        store: Box::new(store),
        runtime,
    }))
}

/// Start the store
///
/// # Safety
/// The handle must be valid and non-null
#[no_mangle]
pub unsafe extern "C" fn eloqstore_start(handle: *mut EloqStoreHandle) -> i32 {
    if handle.is_null() {
        return -1;
    }

    let handle = &mut *handle;

    handle.runtime.block_on(async {
        match handle.store.start().await {
            Ok(_) => 0,
            Err(_) => -1,
        }
    })
}

/// Stop the store
///
/// # Safety
/// The handle must be valid and non-null
#[no_mangle]
pub unsafe extern "C" fn eloqstore_stop(handle: *mut EloqStoreHandle) -> i32 {
    if handle.is_null() {
        return -1;
    }

    let handle = &mut *handle;

    handle.runtime.block_on(async {
        handle.store.stop().await;
        0
    })
}

/// Free the store handle
///
/// # Safety
/// The handle must be valid and non-null, and not used after this call
#[no_mangle]
pub unsafe extern "C" fn eloqstore_free(handle: *mut EloqStoreHandle) {
    if !handle.is_null() {
        let _ = Box::from_raw(handle);
    }
}

/// Write a key-value pair
///
/// # Safety
/// All pointers must be valid and the lengths must be correct
#[no_mangle]
pub unsafe extern "C" fn eloqstore_write(
    handle: *mut EloqStoreHandle,
    table: *const c_char,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    if handle.is_null() || table.is_null() || key.is_null() || value.is_null() {
        return -1;
    }

    let handle = &mut *handle;

    let table_str = match CStr::from_ptr(table).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };

    let key_slice = slice::from_raw_parts(key, key_len);
    let value_slice = slice::from_raw_parts(value, value_len);

    let table_id = TableIdent::new(table_str, 0);
    let entry = WriteEntry {
        key: Bytes::from(key_slice.to_vec()),
        value: Bytes::from(value_slice.to_vec()),
        op: WriteOp::Upsert,
        timestamp: 0,
        expire_ts: None,
    };
    let req = BatchWriteRequest {
        table_id,
        entries: vec![entry],
        sync: true,
        timeout: None,
    };

    handle.runtime.block_on(async {
        match handle.store.batch_write(req).await {
            Ok(_) => 0,
            Err(_) => -1,
        }
    })
}

/// Read a value by key
///
/// # Safety
/// All pointers must be valid and the lengths must be correct
/// The caller must free the returned value using eloqstore_free_value
#[no_mangle]
pub unsafe extern "C" fn eloqstore_read(
    handle: *mut EloqStoreHandle,
    table: *const c_char,
    key: *const u8,
    key_len: usize,
    value_out: *mut *mut u8,
    value_len_out: *mut usize,
) -> i32 {
    if handle.is_null() || table.is_null() || key.is_null() || value_out.is_null() || value_len_out.is_null() {
        return -1;
    }

    let handle = &mut *handle;

    let table_str = match CStr::from_ptr(table).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };

    let key_slice = slice::from_raw_parts(key, key_len);

    let req = ReadRequest {
        table_id: TableIdent::new(table_str, 0),
        key: Bytes::from(key_slice.to_vec()),
        timeout: None,
    };

    handle.runtime.block_on(async {
        match handle.store.read(req).await {
            Ok(response) => {
                if let Some(value) = response.value {
                    let value_bytes = value.to_vec();
                    let value_len = value_bytes.len();

                    // Allocate memory for the value
                    let value_ptr = libc::malloc(value_len) as *mut u8;
                    if value_ptr.is_null() {
                        return -1;
                    }

                    // Copy the value
                    ptr::copy_nonoverlapping(value_bytes.as_ptr(), value_ptr, value_len);

                    *value_out = value_ptr;
                    *value_len_out = value_len;
                    0
                } else {
                    1 // Not found
                }
            }
            Err(_) => -1, // Error
        }
    })
}

/// Free a value returned by eloqstore_read
///
/// # Safety
/// The value pointer must have been returned by eloqstore_read
#[no_mangle]
pub unsafe extern "C" fn eloqstore_free_value(value: *mut u8, len: usize) {
    if !value.is_null() && len > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(value, len));
    }
}