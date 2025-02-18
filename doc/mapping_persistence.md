### Recover state
All RootMeta in IndexPageManager.tbl_roots_ 

```cpp
std::unordered_map<TableIdent, RootMeta> tbl_roots_;

class ReplayTask {
    uint32_t root_{UINT32_MAX};
    std::unique_ptr<PageMapper> mapper_{nullptr};
};

struct RootMeta
{
    MemIndexPage *root_page_;
    std::unique_ptr<PageMapper> mapper_;
    std::unordered_set<MappingSnapshot *> mapping_snapshots_;
};

class PageMapper {
    std::vector<uint64_t> mapping_tbl_; // from MappingSnapshot
    uint32_t free_page_head_{UINT32_MAX};

    std::set<uint32_t> free_file_pages_;
    uint32_t min_file_page_id_{1};
    uint32_t max_file_page_id_{0};
};
```

### Replay process
```cpp
enum class MetaLogType : uint8_t
{
    Snapshot = 0,
    NewRoot,
    UpdateMapping,
    FreePage,
};
```

Update ReplayTask state according to WAL entry:

**UpdateMapping**: <uint32_t, uint32_t> // logical page id -> physical page id

For insert:
1. Remove logical page id from link list free_page_head_  or expand mapping_tbl_ 
2. Remove physical page id from set free_file_pages_ or update min_file_page_id_/max_file_page_id_ field
3. Update mapping_tbl_ field

For update:
1. Add old physical page id to free_file_pages_
2. Remove physical page id from set free_file_pages_ or update min_file_page_id_/max_file_page_id_ field
3. Update  mapping_tbl_ field

**FreePage**: <uint32_t> // free logical page id

1. Add physical page into free_file_pages_ set
2. Add logical page into free_page_head_  link list

**NewRoot**: <uint32_t> // root logical page id

Update root_ field

**Snapshot**: <char[]> // serialized RootMeta

Deserialize to MapReplayState.

#### Finish process
After reach the end of the WAL, convert state from ReplayTask to RootMeta:

1. Update root_page_ by reading root page (root_ field) 
2. Update mapping_snapshots_ field

### Flush to disk
LevelDB log format: [https://github.com/google/leveldb/blob/main/doc/log_format.md](https://github.com/google/leveldb/blob/main/doc/log_format.md)
