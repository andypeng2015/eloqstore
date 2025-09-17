/**
 * EloqStore Rust FFI Bindings
 * 
 * This header provides C-compatible bindings for the Rust implementation
 * of EloqStore.
 */

#ifndef ELOQSTORE_H
#define ELOQSTORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Opaque handle to the EloqStore instance
 */
typedef struct EloqStoreHandle EloqStoreHandle;

/**
 * Create a new EloqStore instance
 * 
 * @param data_dir Path to the data directory (null-terminated string)
 * @return Handle to the store, or NULL on error
 */
EloqStoreHandle* eloqstore_new(const char* data_dir);

/**
 * Start the store
 * 
 * @param handle Store handle
 * @return 0 on success, -1 on error
 */
int32_t eloqstore_start(EloqStoreHandle* handle);

/**
 * Stop the store
 * 
 * @param handle Store handle
 * @return 0 on success, -1 on error
 */
int32_t eloqstore_stop(EloqStoreHandle* handle);

/**
 * Free the store handle
 * 
 * @param handle Store handle to free
 */
void eloqstore_free(EloqStoreHandle* handle);

/**
 * Write a key-value pair to the store
 * 
 * @param handle Store handle
 * @param table Table name (null-terminated string)
 * @param key Key buffer
 * @param key_len Key length in bytes
 * @param value Value buffer
 * @param value_len Value length in bytes
 * @return 0 on success, -1 on error
 */
int32_t eloqstore_write(
    EloqStoreHandle* handle,
    const char* table,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len
);

/**
 * Read a value from the store
 * 
 * @param handle Store handle
 * @param table Table name (null-terminated string)
 * @param key Key buffer
 * @param key_len Key length in bytes
 * @param value_out Pointer to store the value buffer (must be freed with eloqstore_free_value)
 * @param value_len_out Pointer to store the value length
 * @return 0 on success, -1 on error
 */
int32_t eloqstore_read(
    EloqStoreHandle* handle,
    const char* table,
    const uint8_t* key,
    size_t key_len,
    uint8_t** value_out,
    size_t* value_len_out
);

/**
 * Free a value returned by eloqstore_read
 * 
 * @param value Value buffer to free
 * @param len Length of the value buffer
 */
void eloqstore_free_value(uint8_t* value, size_t len);

#ifdef __cplusplus
}
#endif

#endif // ELOQSTORE_H