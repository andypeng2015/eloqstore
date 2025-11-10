#include <optional>
#include <unordered_map>

#include "types.h"

namespace eloqstore
{
constexpr uint32_t num_reserved_fd = 100;

inline std::pair<std::string_view, std::string_view> ParseFileName(
    std::string_view name)
{
    size_t pos = name.find(FileNameSeparator);
    std::string_view file_type;
    std::string_view file_id;

    if (pos == std::string::npos)
    {
        file_type = name;
    }
    else
    {
        file_type = name.substr(0, pos);
        file_id = name.substr(pos + 1);
    }

    return {file_type, file_id};
}

inline std::string DataFileName(FileId file_id, Term term = 0)
{
    std::string name;
    name.reserve(std::size(FileNameData) + 22);
    name.append(FileNameData);
    name.push_back(FileNameSeparator);
    name.append(std::to_string(file_id));
    if (term != 0)
    {
        name.push_back(FileNameSeparator);
        name.append(std::to_string(term));
    }
    return name;
}

inline bool ParseDecimal(std::string_view str, uint64_t &out)
{
    if (str.empty())
    {
        return false;
    }
    uint64_t value = 0;
    for (char ch : str)
    {
        if (ch < '0' || ch > '9')
        {
            return false;
        }
        value = value * 10 + static_cast<uint64_t>(ch - '0');
    }
    out = value;
    return true;
}

inline bool ParseDataFileComponents(std::string_view token,
                                   FileId &file_id,
                                   Term &term)
{
    size_t sep = token.find(FileNameSeparator);
    std::string_view id_str = sep == std::string_view::npos ? token
                                                            : token.substr(0,
                                                                           sep);
    uint64_t id_val = 0;
    if (!ParseDecimal(id_str, id_val))
    {
        return false;
    }
    file_id = id_val;
    term = 0;
    if (sep != std::string_view::npos)
    {
        std::string_view term_str = token.substr(sep + 1);
        if (!term_str.empty())
        {
            uint64_t term_val = 0;
            if (!ParseDecimal(term_str, term_val))
            {
                return false;
            }
            term = term_val;
        }
    }
    return true;
}

inline std::string ManifestFileName(std::optional<uint64_t> archive_ts,
                                    Term term)
{
    std::string name;
    name.reserve(std::size(FileNameManifest) + 32);
    name.append(FileNameManifest);
    if (archive_ts.has_value())
    {
        name.push_back(FileNameSeparator);
        name.append(std::to_string(*archive_ts));
    }
    name.push_back(FileNameSeparator);
    name.append(std::to_string(term));
    return name;
}

inline bool ParseManifestComponents(std::string_view filename,
                                    std::optional<uint64_t> &archive_ts,
                                    Term &term)
{
    auto [file_type, suffix] = ParseFileName(filename);
    if (file_type != FileNameManifest)
    {
        return false;
    }
    archive_ts.reset();
    term = 0;
    if (suffix.empty())
    {
        // Legacy manifest without timestamp/term
        return true;
    }

    size_t sep = suffix.rfind(FileNameSeparator);
    if (sep == std::string::npos)
    {
        // Legacy archive manifest: manifest_<ts>
        uint64_t ts_val;
        if (!ParseDecimal(suffix, ts_val))
        {
            return false;
        }
        archive_ts = ts_val;
        return true;
    }

    std::string_view ts_part = suffix.substr(0, sep);
    std::string_view term_part = suffix.substr(sep + 1);
    if (!term_part.empty())
    {
        uint64_t parsed_term = 0;
        if (!ParseDecimal(term_part, parsed_term))
        {
            return false;
        }
        term = parsed_term;
    }
    if (!ts_part.empty())
    {
        uint64_t ts_val = 0;
        if (!ParseDecimal(ts_part, ts_val))
        {
            return false;
        }
        archive_ts = ts_val;
    }
    return true;
}

inline std::string ArchiveName(uint64_t ts, Term term)
{
    return ManifestFileName(ts, term);
}
}  // namespace eloqstore
