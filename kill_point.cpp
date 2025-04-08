#include "kill_point.h"

namespace kvstore
{
KillPoint &KillPoint::GetInstance()
{
    static KillPoint kill_point;
    return kill_point;
}

void KillPoint::TestKillRandom(std::string kill_point,
                               const char *file,
                               uint32_t line,
                               const char *fn,
                               uint32_t odds_weight)
{
    if (kill_odds_ == 0)
    {
        return;
    }

    uint64_t odds = kill_odds_ * odds_weight;
    assert(odds > 0);
    bool crash = dis(gen) % odds == 0;
    if (crash)
    {
        fprintf(stdout, "Crashing at %s:%d, function %s\n", file, line, fn);
        fflush(stdout);
        exit(100);
    }
}
}  // namespace kvstore