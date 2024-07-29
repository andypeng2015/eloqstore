#pragma once

#include <string>

namespace kvstore
{
class ReadResult
{
    std::string blob_;
    int status_;
};
}  // namespace kvstore