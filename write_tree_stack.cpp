#include "write_tree_stack.h"

#include "index_page_manager.h"
#include "mem_index_page.h"

namespace kvstore
{
WriteIndexStack::WriteIndexStack(IndexPageManager *idx_page_manager)
    : idx_page_manager_(idx_page_manager)
{
}

void WriteIndexStack::Seek(MemIndexPage *root, std::string_view key)
{
}

void WriteIndexStack::Pop()
{
}
}  // namespace kvstore