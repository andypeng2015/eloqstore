#pragma once

#include <atomic>
#include <functional>
#include <random>
#include <string>
#include <mutex>
#include <map>

namespace eloqstore {
namespace test {

class FaultInjector {
public:
    enum class FaultType {
        NONE,
        IO_ERROR,
        MEMORY_ALLOCATION_FAIL,
        NETWORK_ERROR,
        TIMEOUT,
        DATA_CORRUPTION,
        PARTIAL_WRITE,
        CRASH_SIMULATION
    };

    struct FaultConfig {
        double probability = 0.0;  // 0.0 to 1.0
        bool enabled = false;
        bool one_shot = false;  // Only trigger once
        std::function<bool()> condition = nullptr;  // Optional condition
    };

    static FaultInjector& GetInstance() {
        static FaultInjector instance;
        return instance;
    }

    // Configure fault injection
    void SetFault(FaultType type, const FaultConfig& config);
    void ClearFault(FaultType type);
    void ClearAllFaults();

    // Check if fault should be triggered
    bool ShouldInject(FaultType type);

    // Inject specific faults
    bool InjectIOError();
    bool InjectMemoryFailure();
    bool InjectNetworkError();
    bool InjectTimeout();
    bool InjectDataCorruption(void* data, size_t size);
    bool InjectPartialWrite(size_t& actual_size, size_t requested_size);

    // Statistics
    struct Stats {
        std::atomic<uint64_t> total_checks{0};
        std::atomic<uint64_t> faults_injected{0};
        std::map<FaultType, uint64_t> fault_counts;

        void Reset() {
            total_checks = 0;
            faults_injected = 0;
            fault_counts.clear();
        }

        void Print() const;
    };

    const Stats& GetStats() const { return stats_; }
    void ResetStats() { stats_.Reset(); }

    // Enable/disable globally
    void Enable() { enabled_ = true; }
    void Disable() { enabled_ = false; }
    bool IsEnabled() const { return enabled_; }

private:
    FaultInjector();

    std::atomic<bool> enabled_{false};
    std::map<FaultType, FaultConfig> fault_configs_;
    std::mutex config_mutex_;
    Stats stats_;
    std::mt19937 rng_;
    std::uniform_real_distribution<double> dist_{0.0, 1.0};

    bool CheckAndInject(FaultType type);
};

// Helper macros for conditional fault injection
#define INJECT_FAULT(type) \
    if (FaultInjector::GetInstance().IsEnabled() && \
        FaultInjector::GetInstance().ShouldInject(FaultInjector::FaultType::type))

#define INJECT_IO_ERROR() \
    INJECT_FAULT(IO_ERROR) { \
        errno = EIO; \
        return -1; \
    }

#define INJECT_MEMORY_FAILURE() \
    INJECT_FAULT(MEMORY_ALLOCATION_FAIL) { \
        return nullptr; \
    }

// Scoped fault injection for testing
class ScopedFaultInjection {
public:
    ScopedFaultInjection(FaultInjector::FaultType type, double probability)
        : type_(type), was_enabled_(FaultInjector::GetInstance().IsEnabled()) {
        FaultInjector::FaultConfig config;
        config.probability = probability;
        config.enabled = true;
        FaultInjector::GetInstance().SetFault(type_, config);
        FaultInjector::GetInstance().Enable();
    }

    ~ScopedFaultInjection() {
        FaultInjector::GetInstance().ClearFault(type_);
        if (!was_enabled_) {
            FaultInjector::GetInstance().Disable();
        }
    }

private:
    FaultInjector::FaultType type_;
    bool was_enabled_;
};

} // namespace test
} // namespace eloqstore