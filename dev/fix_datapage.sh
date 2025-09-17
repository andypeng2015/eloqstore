#!/bin/bash

# Replace all incorrect DataPage constructor calls with proper initialization
sed -i 's/DataPage page(const_cast<char\*>(page_data\.data()), page_data\.size());/auto page = std::make_unique<DataPage>(1);\n        Page page_mem(page_data.size());\n        std::memcpy(page_mem.Data(), page_data.data(), page_data.size());\n        page->SetPage(std::move(page_mem));/g' tests/core/data_page_builder_test.cpp

# Replace page.EntryCount() with helper call
sed -i 's/page\.EntryCount()/DataPageTestHelper::NumEntries(page.get(), \&opt_)/g' tests/core/data_page_builder_test.cpp

# Replace page.Get with helper call
sed -i 's/page\.Get(/DataPageTestHelper::Get(page.get(), \&opt_, /g' tests/core/data_page_builder_test.cpp

# Replace page.IsEmpty() with helper
sed -i 's/page\.IsEmpty()/DataPageTestHelper::Empty(page.get())/g' tests/core/data_page_builder_test.cpp

# Fix references to page as object when it's now a pointer
sed -i 's/REQUIRE(DataPageTestHelper::NumEntries(&page, /REQUIRE(DataPageTestHelper::NumEntries(page.get(), /g' tests/core/data_page_builder_test.cpp

# Fix iterator usage
sed -i 's/DataPage::Iterator iter(&page,/DataPageTestHelper::Iterator iter(page.get(),/g' tests/core/data_page_builder_test.cpp

