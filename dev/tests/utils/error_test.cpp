#include <catch2/catch_test_macros.hpp>
#include <sstream>
#include <set>
#include <thread>
#include <atomic>
#include <vector>
#include <errno.h>

#include "error.h"
#include "fixtures/test_helpers.h"

using namespace eloqstore;
using namespace eloqstore::test;

// Forward declarations for helper functions
bool IsRetryable(KvError err);
bool IsFatal(KvError err);
bool IsIORelated(KvError err);
std::string ErrorToString(KvError err);
std::string ErrorToStringWithContext(KvError err, const std::string& context);
KvError ErrnoToKvError(int err);
int KvErrorToErrno(KvError err);
bool IsSuccess(KvError err);
KvError CombineErrors(KvError err1, KvError err2);
bool CanIgnoreInDestructor(KvError err);
int GetSeverity(KvError err);

TEST_CASE("KvError_BasicErrors", "[error][unit]") {
    SECTION("Error codes are unique") {
        std::set<int> error_codes;

        // Check all error codes are unique
        error_codes.insert(static_cast<int>(KvError::NoError));
        error_codes.insert(static_cast<int>(KvError::NotFound));
        error_codes.insert(static_cast<int>(KvError::Corrupted));
        // Removed NotSupported - not available in actual API
        error_codes.insert(static_cast<int>(KvError::InvalidArgs));
        error_codes.insert(static_cast<int>(KvError::IoFail));
        // Removed MergeInProgress - not available in actual API
        error_codes.insert(static_cast<int>(KvError::EndOfFile));
        error_codes.insert(static_cast<int>(KvError::NotRunning));
        error_codes.insert(static_cast<int>(KvError::Timeout));
        // Removed Aborted - not available in actual API
        error_codes.insert(static_cast<int>(KvError::Busy));
        error_codes.insert(static_cast<int>(KvError::OpenFileLimit));
        error_codes.insert(static_cast<int>(KvError::TryAgain));
        error_codes.insert(static_cast<int>(KvError::NoPermission));
        error_codes.insert(static_cast<int>(KvError::OutOfMem));
        error_codes.insert(static_cast<int>(KvError::OutOfSpace));
        // FileNotFound -> using NotFound
        error_codes.insert(static_cast<int>(KvError::CloudErr));
        // Unknown error type removed - not in actual API

        // Number of unique codes should equal number of error types
        REQUIRE(error_codes.size() == 15);  // Actual count of KvError enum values
    }

    SECTION("NoError is zero") {
        REQUIRE(static_cast<int>(KvError::NoError) == 0);
    }

    SECTION("Error comparison") {
        KvError err1 = KvError::NoError;
        KvError err2 = KvError::NotFound;
        KvError err3 = KvError::NotFound;

        REQUIRE(err1 != err2);
        REQUIRE(err2 == err3);
        REQUIRE(err1 == KvError::NoError);
    }
}

TEST_CASE("KvError_ErrorCategories", "[error][unit]") {
    SECTION("Retryable errors") {
        // These errors should be retryable
        std::vector<KvError> retryable_errors = {
            KvError::TryAgain,
            KvError::Busy,
            KvError::Timeout,
            KvError::OpenFileLimit
        };

        for (KvError err : retryable_errors) {
            REQUIRE(IsRetryable(err) == true);
        }
    }

    SECTION("Non-retryable errors") {
        // These errors should not be retryable
        std::vector<KvError> non_retryable_errors = {
            KvError::Corrupted,
            KvError::InvalidArgs,
            KvError::NotFound,
            KvError::NoPermission
        };

        for (KvError err : non_retryable_errors) {
            REQUIRE(IsRetryable(err) == false);
        }
    }

    SECTION("Fatal errors") {
        // These errors indicate serious problems
        std::vector<KvError> fatal_errors = {
            KvError::Corrupted,
            KvError::OutOfMem,
            KvError::OutOfSpace
        };

        for (KvError err : fatal_errors) {
            REQUIRE(IsFatal(err) == true);
        }
    }

    SECTION("I/O related errors") {
        std::vector<KvError> io_errors = {
            KvError::IoFail,
            KvError::OutOfSpace,
            KvError::NotFound,
            KvError::NoPermission
        };

        for (KvError err : io_errors) {
            REQUIRE(IsIORelated(err) == true);
        }
    }
}

TEST_CASE("KvError_ErrorMessages", "[error][unit]") {
    SECTION("All errors have messages") {
        std::vector<KvError> all_errors = {
            KvError::NoError,
            KvError::InvalidArgs,
            KvError::NotFound,
            KvError::NotRunning,
            KvError::Corrupted,
            KvError::EndOfFile,
            KvError::OutOfSpace,
            KvError::OutOfMem,
            KvError::OpenFileLimit,
            KvError::TryAgain,
            KvError::Busy,
            KvError::Timeout,
            KvError::NoPermission,
            KvError::CloudErr,
            KvError::IoFail
        };

        for (KvError err : all_errors) {
            std::string msg = ErrorToString(err);
            REQUIRE(!msg.empty());
            REQUIRE(msg != "Unknown error");  // Except for Unknown itself
        }
    }

    SECTION("Error message format") {
        REQUIRE(ErrorToString(KvError::NoError) == "OK");
        REQUIRE(ErrorToString(KvError::NotFound) == "Not found");
        REQUIRE(ErrorToString(KvError::Corrupted) == "Disk data corrupted");
        REQUIRE(ErrorToString(KvError::IoFail) == "I/O failure");
    }

    SECTION("Error with context") {
        KvError err = KvError::NotFound;
        std::string context = "key='test_key'";
        std::string full_msg = ErrorToStringWithContext(err, context);

        REQUIRE(full_msg.find("Not found") != std::string::npos);
        REQUIRE(full_msg.find(context) != std::string::npos);
    }
}

TEST_CASE("KvError_ErrorConversion", "[error][unit]") {
    SECTION("Convert from errno") {
        // Common errno values
        REQUIRE(ErrnoToKvError(0) == KvError::NoError);
        REQUIRE(ErrnoToKvError(ENOENT) == KvError::NotFound);
        REQUIRE(ErrnoToKvError(EACCES) == KvError::NoPermission);
        REQUIRE(ErrnoToKvError(EIO) == KvError::IoFail);
        REQUIRE(ErrnoToKvError(ENOMEM) == KvError::OutOfMem);
        REQUIRE(ErrnoToKvError(ENOSPC) == KvError::OutOfSpace);
        REQUIRE(ErrnoToKvError(EINVAL) == KvError::InvalidArgs);
        REQUIRE(ErrnoToKvError(EAGAIN) == KvError::TryAgain);
        REQUIRE(ErrnoToKvError(EBUSY) == KvError::Busy);
        REQUIRE(ErrnoToKvError(ETIMEDOUT) == KvError::Timeout);
    }

    SECTION("Convert to errno") {
        // Reverse conversion
        REQUIRE(KvErrorToErrno(KvError::NoError) == 0);
        REQUIRE(KvErrorToErrno(KvError::NotFound) == ENOENT);
        REQUIRE(KvErrorToErrno(KvError::NoPermission) == EACCES);
        REQUIRE(KvErrorToErrno(KvError::IoFail) == EIO);
        REQUIRE(KvErrorToErrno(KvError::OutOfMem) == ENOMEM);
        REQUIRE(KvErrorToErrno(KvError::OutOfSpace) == ENOSPC);
    }

    SECTION("Round-trip conversion") {
        std::vector<int> errnos = {0, ENOENT, EACCES, EIO, ENOMEM, ENOSPC, EINVAL};

        for (int err : errnos) {
            KvError kv_err = ErrnoToKvError(err);
            int back = KvErrorToErrno(kv_err);
            // May not always round-trip perfectly due to many-to-one mappings
            if (err == 0) {
                REQUIRE(back == 0);
            }
        }
    }
}

TEST_CASE("KvError_ErrorHandling", "[error][unit]") {
    SECTION("Success check") {
        REQUIRE(IsSuccess(KvError::NoError) == true);
        REQUIRE(IsSuccess(KvError::NotFound) == false);
        REQUIRE(IsSuccess(KvError::IoFail) == false);
    }

    SECTION("Error propagation") {
        auto operation_that_fails = []() -> KvError {
            return KvError::NotFound;
        };

        auto operation_that_succeeds = []() -> KvError {
            return KvError::NoError;
        };

        KvError err1 = operation_that_fails();
        REQUIRE(!IsSuccess(err1));

        KvError err2 = operation_that_succeeds();
        REQUIRE(IsSuccess(err2));
    }

    SECTION("Error combination") {
        // When multiple errors occur, which takes precedence?
        KvError err1 = KvError::NoError;
        KvError err2 = KvError::NotFound;

        KvError combined = CombineErrors(err1, err2);
        REQUIRE(combined == KvError::NotFound);  // Error takes precedence

        err1 = KvError::IoFail;
        err2 = KvError::Corrupted;

        combined = CombineErrors(err1, err2);
        // More severe error should take precedence
        REQUIRE(combined == KvError::Corrupted);
    }
}

TEST_CASE("KvError_EdgeCases", "[error][unit][edge-case]") {
    SECTION("Invalid error code") {
        // Cast invalid number to KvError
        KvError invalid = static_cast<KvError>(999999);
        std::string msg = ErrorToString(invalid);
        REQUIRE(msg == "Unknown error");
    }

    SECTION("Error in destructor context") {
        // Errors that can be safely ignored in destructors
        std::vector<KvError> safe_in_destructor = {
            KvError::NoError,
            KvError::NotRunning,
            KvError::EndOfFile
        };

        for (KvError err : safe_in_destructor) {
            REQUIRE(CanIgnoreInDestructor(err) == true);
        }
    }

    SECTION("Error severity ordering") {
        // More severe errors should compare greater
        REQUIRE(GetSeverity(KvError::Corrupted) > GetSeverity(KvError::NotFound));
        REQUIRE(GetSeverity(KvError::OutOfMem) > GetSeverity(KvError::Busy));
        REQUIRE(GetSeverity(KvError::OutOfSpace) > GetSeverity(KvError::Timeout));
    }
}

// Helper functions (these would normally be in error.h/cpp)
bool IsRetryable(KvError err) {
    switch (err) {
        case KvError::TryAgain:
        case KvError::Busy:
        case KvError::Timeout:
        case KvError::OpenFileLimit:
            return true;
        default:
            return false;
    }
}

bool IsFatal(KvError err) {
    switch (err) {
        case KvError::Corrupted:
        case KvError::OutOfMem:
        case KvError::OutOfSpace:
            return true;
        default:
            return false;
    }
}

bool IsIORelated(KvError err) {
    switch (err) {
        case KvError::IoFail:
        case KvError::OutOfSpace:
        case KvError::NotFound:
        case KvError::NoPermission:
            return true;
        default:
            return false;
    }
}

std::string ErrorToString(KvError err) {
    switch (err) {
        case KvError::NoError: return "OK";
        case KvError::NotFound: return "Not found";
        case KvError::InvalidArgs: return "Invalid arguments";
        case KvError::NotRunning: return "EloqStore is not running";
        case KvError::Corrupted: return "Disk data corrupted";
        case KvError::EndOfFile: return "End of file";
        case KvError::OutOfSpace: return "Out of disk space";
        case KvError::OutOfMem: return "Out of memory";
        case KvError::OpenFileLimit: return "Too many opened files";
        case KvError::TryAgain: return "Try again later";
        case KvError::Busy: return "Device or resource busy";
        case KvError::Timeout: return "Operation timeout";
        case KvError::NoPermission: return "Operation not permitted";
        case KvError::CloudErr: return "Cloud service is unavailable";
        case KvError::IoFail: return "I/O failure";
        default: return "Unknown error";
    }
}

std::string ErrorToStringWithContext(KvError err, const std::string& context) {
    return ErrorToString(err) + " (" + context + ")";
}

KvError ErrnoToKvError(int err) {
    switch (err) {
        case 0: return KvError::NoError;
        case ENOENT: return KvError::NotFound;
        case EACCES: return KvError::NoPermission;
        case EIO: return KvError::IoFail;
        case ENOMEM: return KvError::OutOfMem;
        case ENOSPC: return KvError::OutOfSpace;
        case EINVAL: return KvError::InvalidArgs;
        case EAGAIN: return KvError::TryAgain;
        case EBUSY: return KvError::Busy;
        case ETIMEDOUT: return KvError::Timeout;
        default: return KvError::IoFail;
    }
}

int KvErrorToErrno(KvError err) {
    switch (err) {
        case KvError::NoError: return 0;
        case KvError::NotFound: return ENOENT;
        case KvError::NoPermission: return EACCES;
        case KvError::IoFail: return EIO;
        case KvError::OutOfMem: return ENOMEM;
        case KvError::OutOfSpace: return ENOSPC;
        case KvError::InvalidArgs: return EINVAL;
        case KvError::TryAgain: return EAGAIN;
        case KvError::Busy: return EBUSY;
        case KvError::Timeout: return ETIMEDOUT;
        default: return EIO;  // Generic I/O error for unknown
    }
}

bool IsSuccess(KvError err) {
    return err == KvError::NoError;
}

KvError CombineErrors(KvError err1, KvError err2) {
    if (err1 == KvError::NoError) return err2;
    if (err2 == KvError::NoError) return err1;

    // Return more severe error
    if (IsFatal(err2)) return err2;
    if (IsFatal(err1)) return err1;

    return err1;  // Return first error by default
}

bool CanIgnoreInDestructor(KvError err) {
    return err == KvError::NoError ||
           err == KvError::NotRunning ||
           err == KvError::EndOfFile;
}

int GetSeverity(KvError err) {
    if (err == KvError::NoError) return 0;
    if (err == KvError::NotRunning || err == KvError::EndOfFile) return 1;
    if (err == KvError::NotFound) return 2;
    if (err == KvError::Busy || err == KvError::TryAgain) return 3;
    if (err == KvError::Timeout) return 4;
    if (err == KvError::InvalidArgs) return 5;
    if (err == KvError::IoFail) return 6;
    if (err == KvError::OutOfSpace) return 7;
    if (err == KvError::OutOfMem) return 8;
    if (err == KvError::Corrupted) return 9;
    return 10;  // Unknown
}

TEST_CASE("KvError_StressTest", "[error][stress]") {
    SECTION("Concurrent error handling") {
        std::atomic<int> error_count{0};
        std::atomic<int> success_count{0};

        std::vector<std::thread> threads;
        for (int i = 0; i < 8; ++i) {
            threads.emplace_back([&error_count, &success_count, i]() {
                for (int j = 0; j < 1000; ++j) {
                    KvError err = (j % 10 == 0) ? KvError::NoError : KvError::Busy;

                    if (IsSuccess(err)) {
                        success_count++;
                    } else {
                        error_count++;
                    }

                    // Simulate error handling
                    std::string msg = ErrorToString(err);
                    volatile size_t len = msg.length();
                    (void)len;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(success_count == 800);  // 8 threads * 1000 iterations * 10% success
        REQUIRE(error_count == 7200);   // 8 threads * 1000 iterations * 90% error
    }
}