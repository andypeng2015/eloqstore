#include "fault_injector.h"
#include <iostream>
#include <iomanip>
#include <cstring>
#include <cerrno>

namespace eloqstore {
namespace test {

FaultInjector::FaultInjector() {
    std::random_device rd;
    rng_.seed(rd());
}

void FaultInjector::SetFault(FaultType type, const FaultConfig& config) {
    std::lock_guard<std::mutex> lock(config_mutex_);
    fault_configs_[type] = config;
}

void FaultInjector::ClearFault(FaultType type) {
    std::lock_guard<std::mutex> lock(config_mutex_);
    fault_configs_.erase(type);
}

void FaultInjector::ClearAllFaults() {
    std::lock_guard<std::mutex> lock(config_mutex_);
    fault_configs_.clear();
}

bool FaultInjector::ShouldInject(FaultType type) {
    if (!enabled_) {
        return false;
    }

    stats_.total_checks++;

    std::lock_guard<std::mutex> lock(config_mutex_);
    auto it = fault_configs_.find(type);
    if (it == fault_configs_.end() || !it->second.enabled) {
        return false;
    }

    // Check condition if provided
    if (it->second.condition && !it->second.condition()) {
        return false;
    }

    // Check probability
    if (dist_(rng_) <= it->second.probability) {
        stats_.faults_injected++;
        stats_.fault_counts[type]++;

        // Disable if one-shot
        if (it->second.one_shot) {
            it->second.enabled = false;
        }

        return true;
    }

    return false;
}

bool FaultInjector::InjectIOError() {
    if (ShouldInject(FaultType::IO_ERROR)) {
        errno = EIO;
        return true;
    }
    return false;
}

bool FaultInjector::InjectMemoryFailure() {
    return ShouldInject(FaultType::MEMORY_ALLOCATION_FAIL);
}

bool FaultInjector::InjectNetworkError() {
    if (ShouldInject(FaultType::NETWORK_ERROR)) {
        errno = ENETDOWN;
        return true;
    }
    return false;
}

bool FaultInjector::InjectTimeout() {
    if (ShouldInject(FaultType::TIMEOUT)) {
        errno = ETIMEDOUT;
        return true;
    }
    return false;
}

bool FaultInjector::InjectDataCorruption(void* data, size_t size) {
    if (ShouldInject(FaultType::DATA_CORRUPTION) && data && size > 0) {
        // Corrupt random byte
        uint8_t* bytes = static_cast<uint8_t*>(data);
        std::uniform_int_distribution<size_t> pos_dist(0, size - 1);
        std::uniform_int_distribution<int> byte_dist(0, 255);

        size_t pos = pos_dist(rng_);
        bytes[pos] = static_cast<uint8_t>(byte_dist(rng_));
        return true;
    }
    return false;
}

bool FaultInjector::InjectPartialWrite(size_t& actual_size, size_t requested_size) {
    if (ShouldInject(FaultType::PARTIAL_WRITE)) {
        std::uniform_int_distribution<size_t> size_dist(1, requested_size - 1);
        actual_size = size_dist(rng_);
        return true;
    }
    actual_size = requested_size;
    return false;
}

void FaultInjector::Stats::Print() const {
    std::cout << "\n=== Fault Injection Statistics ===" << std::endl;
    std::cout << "Total checks: " << total_checks.load() << std::endl;
    std::cout << "Faults injected: " << faults_injected.load() << std::endl;

    if (!fault_counts.empty()) {
        std::cout << "Fault breakdown:" << std::endl;
        for (const auto& [type, count] : fault_counts) {
            std::cout << "  ";
            switch (type) {
                case FaultType::IO_ERROR: std::cout << "IO_ERROR"; break;
                case FaultType::MEMORY_ALLOCATION_FAIL: std::cout << "MEMORY_FAIL"; break;
                case FaultType::NETWORK_ERROR: std::cout << "NETWORK_ERROR"; break;
                case FaultType::TIMEOUT: std::cout << "TIMEOUT"; break;
                case FaultType::DATA_CORRUPTION: std::cout << "DATA_CORRUPTION"; break;
                case FaultType::PARTIAL_WRITE: std::cout << "PARTIAL_WRITE"; break;
                case FaultType::CRASH_SIMULATION: std::cout << "CRASH"; break;
                default: std::cout << "UNKNOWN"; break;
            }
            std::cout << ": " << count << std::endl;
        }
    }
}

} // namespace test
} // namespace eloqstore