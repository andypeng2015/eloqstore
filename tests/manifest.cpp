#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "kv_options.h"
#include "replayer.h"
#include "root_meta.h"

class ManifestVerifier
{
public:
    ManifestVerifier(kvstore::KvOptions opts);
    void NewMapping();
    void UpdateMapping();
    void FreeMapping();
    void Finish();
    void Snapshot();

    void Verify() const;
    uint32_t Size() const;

private:
    std::pair<uint32_t, uint32_t> RandChoose();

    kvstore::KvOptions options_;
    uint32_t root_id_;
    kvstore::PageMapper answer_;
    std::unordered_map<uint32_t, uint32_t> helper_;

    kvstore::ManifestBuilder builder_;
    std::string file_;
};

ManifestVerifier::ManifestVerifier(kvstore::KvOptions opts) : options_(opts)
{
    answer_.InitPages(opts.init_page_count);
}

std::pair<uint32_t, uint32_t> ManifestVerifier::RandChoose()
{
    assert(!helper_.empty());
    auto it = std::next(helper_.begin(), rand() % helper_.size());
    return *it;
}

uint32_t ManifestVerifier::Size() const
{
    return helper_.size();
}

void ManifestVerifier::NewMapping()
{
    uint32_t page_id = answer_.GetPage();
    uint32_t file_page_id = answer_.GetFilePage();
    answer_.UpdateMapping(page_id, file_page_id);

    builder_.UpdateMapping(page_id, file_page_id);
    helper_[page_id] = file_page_id;
}

void ManifestVerifier::UpdateMapping()
{
    auto [page_id, old_fp_id] = RandChoose();
    root_id_ = page_id;

    uint32_t new_fp_id = answer_.GetFilePage();
    answer_.UpdateMapping(page_id, new_fp_id);
    answer_.FreeFilePage(old_fp_id);

    builder_.UpdateMapping(page_id, new_fp_id);

    helper_[page_id] = new_fp_id;
}

void ManifestVerifier::FreeMapping()
{
    auto [page_id, file_page_id] = RandChoose();
    helper_.erase(page_id);
    if (page_id == root_id_)
    {
        root_id_ = Size() == 0 ? UINT32_MAX : RandChoose().first;
    }

    answer_.FreePage(page_id);
    answer_.FreeFilePage(file_page_id);

    builder_.UpdateMapping(page_id, UINT32_MAX);
}

void ManifestVerifier::Finish()
{
    if (!builder_.Empty())
    {
        if (file_.empty())
        {
            Snapshot();
        }
        else
        {
            std::string_view sv = builder_.Finalize(root_id_);
            file_.append(sv);
            builder_.Reset();
        }
    }
}

void ManifestVerifier::Snapshot()
{
    std::string_view sv = builder_.Snapshot(root_id_, answer_);
    file_ = sv;
    builder_.Reset();
}

void ManifestVerifier::Verify() const
{
    auto file = std::make_unique<kvstore::MemStoreMgr::Manifest>(file_);

    kvstore::Replayer replayer;
    kvstore::KvError err = replayer.Replay(std::move(file), &options_);
    REQUIRE(err == kvstore::KvError::NoError);
    auto mapper = replayer.Mapper(nullptr, nullptr);

    REQUIRE(replayer.root_ == root_id_);
    REQUIRE(mapper->EqualTo(answer_));
}

TEST_CASE("simple manifest recovery", "[manifest]")
{
    kvstore::KvOptions opts;
    opts.init_page_count = 100;
    ManifestVerifier verifier(opts);

    verifier.NewMapping();
    verifier.NewMapping();
    verifier.UpdateMapping();
    verifier.FreeMapping();
    verifier.Finish();
    verifier.Verify();

    verifier.FreeMapping();
    verifier.Finish();
    verifier.Verify();
}

TEST_CASE("medium manifest recovery", "[manifest]")
{
    kvstore::KvOptions opts;
    opts.init_page_count = 100;
    ManifestVerifier verifier(opts);

    for (int i = 0; i < 100; i++)
    {
        verifier.NewMapping();
        verifier.NewMapping();
        verifier.FreeMapping();
        verifier.NewMapping();
        verifier.UpdateMapping();
        verifier.Finish();
    }
    verifier.Verify();

    verifier.Snapshot();
    verifier.Verify();

    for (int i = 0; i < 10; i++)
    {
        verifier.NewMapping();
        verifier.NewMapping();
        verifier.FreeMapping();
        verifier.NewMapping();
        verifier.UpdateMapping();
        verifier.Finish();

        verifier.Verify();
    }
}
