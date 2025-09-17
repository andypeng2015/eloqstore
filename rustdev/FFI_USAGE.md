# EloqStore FFI Usage Guide

## Building the Library

### Build as Dynamic Library (.so/.dll)
```bash
cargo build --release --lib
# Output: target/release/libeloqstore.so (Linux)
#         target/release/eloqstore.dll (Windows)
#         target/release/libeloqstore.dylib (macOS)
```

### Build as Static Library (.a)
```bash
cargo build --release --lib
# Output: target/release/libeloqstore.a
```

## C/C++ Integration

### Include the Header
```c
#include "eloqstore.h"
```

### Basic Usage Example
```c
#include <stdio.h>
#include <string.h>
#include "eloqstore.h"

int main() {
    // Create and start the store
    EloqStoreHandle* store = eloqstore_new("/data/mystore");
    if (!store) {
        fprintf(stderr, "Failed to create store\n");
        return 1;
    }

    if (eloqstore_start(store) != 0) {
        fprintf(stderr, "Failed to start store\n");
        eloqstore_free(store);
        return 1;
    }

    // Write a key-value pair
    const char* table = "users";
    const char* key = "user123";
    const char* value = "John Doe";
    
    if (eloqstore_write(store, table, 
                       (uint8_t*)key, strlen(key),
                       (uint8_t*)value, strlen(value)) != 0) {
        fprintf(stderr, "Write failed\n");
    }

    // Read the value back
    uint8_t* read_value;
    size_t value_len;
    
    if (eloqstore_read(store, table,
                      (uint8_t*)key, strlen(key),
                      &read_value, &value_len) == 0) {
        printf("Read value: %.*s\n", (int)value_len, read_value);
        eloqstore_free_value(read_value, value_len);
    } else {
        fprintf(stderr, "Read failed\n");
    }

    // Clean up
    eloqstore_stop(store);
    eloqstore_free(store);
    
    return 0;
}
```

### Compilation Example
```bash
# Dynamic linking
gcc -o myapp myapp.c -L./target/release -leloqstore

# Static linking
gcc -o myapp myapp.c ./target/release/libeloqstore.a -lpthread -ldl -lm
```

## API Reference

### Store Management
- `eloqstore_new(data_dir)` - Create new store instance
- `eloqstore_start(handle)` - Start the store
- `eloqstore_stop(handle)` - Stop the store gracefully
- `eloqstore_free(handle)` - Free the store handle

### Data Operations
- `eloqstore_write(handle, table, key, key_len, value, value_len)` - Write key-value
- `eloqstore_read(handle, table, key, key_len, &value, &value_len)` - Read value
- `eloqstore_free_value(value, len)` - Free read value buffer

## Thread Safety

The FFI bindings are thread-safe. Multiple threads can call read/write operations concurrently on the same handle.

## Error Handling

All functions return:
- `0` on success
- `-1` on error
- `NULL` for allocation failures

## Memory Management

- Values returned by `eloqstore_read` must be freed with `eloqstore_free_value`
- The store handle must be freed with `eloqstore_free`
- All input buffers are copied internally, so can be freed after the call

## Performance Tips

1. Reuse the store handle across operations
2. Batch writes when possible
3. Keep the store running for the application lifetime
4. Use appropriate data directory on fast storage (SSD/NVMe)