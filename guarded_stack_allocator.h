#pragma once

#include <sys/mman.h>
#include <unistd.h>

#include <boost/context/stack_context.hpp>
#include <cstddef>
#include <glog/logging.h>
#include <new>
#include <stdexcept>
#include <vector>

namespace eloqstore
{

// Allocates fixed-size stacks backed by mmap with guard pages at both ends.
class GuardedMmapStackAllocator
{
    struct StackBlock
    {
        void *base{nullptr};
        std::size_t stack_size{0};
    };

    struct ThreadPool
    {
        ~ThreadPool()
        {
            const std::size_t page = PageSize();
            for (auto &block : blocks)
            {
                munmap(block.base, block.stack_size + 2 * page);
            }
        }

        std::vector<StackBlock> blocks;
    };

public:
    explicit GuardedMmapStackAllocator(std::size_t stack_size) noexcept
        : stack_size_(AlignToPage(stack_size, PageSize()))
    {
    }

    boost::context::stack_context allocate()
    {
        return AllocateContext();
    }

    boost::context::stack_context allocate(std::size_t)
    {
        return AllocateContext();
    }

    void deallocate(boost::context::stack_context &sctx) noexcept
    {
        if (sctx.sp == nullptr || sctx.size == 0)
        {
            return;
        }

        const std::size_t page = PageSize();
        void *stack_base = static_cast<char *>(sctx.sp) - sctx.size;
        void *base = static_cast<char *>(stack_base) - page;
        Pool().blocks.push_back(StackBlock{base, sctx.size});
        sctx.sp = nullptr;
        sctx.size = 0;
    }

private:
    static ThreadPool &Pool()
    {
        thread_local ThreadPool pool;
        return pool;
    }

    boost::context::stack_context AllocateContext()
    {
        const std::size_t page = PageSize();
        StackBlock block = Acquire();
        boost::context::stack_context sctx{};
        void *stack_base = static_cast<char *>(block.base) + page;
        sctx.size = block.stack_size;
        sctx.sp = static_cast<char *>(stack_base) + block.stack_size;
        LOG(INFO) << "sp:" << sctx.sp << ", top:" << sctx.sp - sctx.size << "\n";
        return sctx;
    }

    StackBlock Acquire()
    {
        auto &blocks = Pool().blocks;
        if (!blocks.empty())
        {
            StackBlock block = blocks.back();
            blocks.pop_back();
            return block;
        }
        return Create();
    }

    StackBlock Create()
    {
        const std::size_t page = PageSize();
        const std::size_t total_size = stack_size_ + 2 * page;
        void *base = mmap(nullptr,
                          total_size,
                          PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS,
                          -1,
                          0);
        if (base == MAP_FAILED)
        {
            throw std::bad_alloc();
        }

        void *underflow_guard = static_cast<char *>(base) + page + stack_size_;
        if (mprotect(base, page, PROT_NONE) != 0 ||
            mprotect(underflow_guard, page, PROT_NONE) != 0)
        {
            munmap(base, total_size);
            throw std::runtime_error("mprotect(PROT_NONE) failed");
        }

        return StackBlock{base, stack_size_};
    }

    static std::size_t AlignToPage(std::size_t size, std::size_t page) noexcept
    {
        return ((size + page - 1) / page) * page;
    }

    static std::size_t PageSize() noexcept
    {
        static const std::size_t cached = []
        {
            long p = sysconf(_SC_PAGESIZE);
            return p > 0 ? static_cast<std::size_t>(p) : 4096U;
        }();
        return cached;
    }

    std::size_t stack_size_{0};
};

}  // namespace eloqstore
