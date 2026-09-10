#include "util/fs.h"
#include "util/logger.h"

#include <filesystem>

namespace flashshare {

namespace fs = std::filesystem;

bool fs_is_directory(const std::string& path) {
    std::error_code ec;
    return fs::is_directory(path, ec);
}

std::vector<FileEntry> fs_walk_directory(const std::string& dir_path) {
    std::vector<FileEntry> entries;

    std::error_code ec;
    fs::path root = fs::canonical(fs::path(dir_path), ec);
    if (ec) {
        LOG_ERROR("Cannot resolve directory: %s — %s", dir_path.c_str(), ec.message().c_str());
        return entries;
    }

    std::string base_name = root.filename().string();
    if (base_name.empty()) base_name = root.string();

    auto it = fs::recursive_directory_iterator(root, fs::directory_options::skip_permission_denied, ec);
    auto end = fs::recursive_directory_iterator();
    for (; !ec && it != end; it.increment(ec)) {
        const fs::directory_entry& de = *it;

        std::error_code fec;
        if (!de.is_regular_file(fec) || fec) continue;

        std::error_code rec;
        fs::path rel = fs::relative(de.path(), root, rec);
        if (rec) continue;

        uint64_t size = static_cast<uint64_t>(de.file_size(fec));
        if (fec) size = 0;

        FileEntry entry;
        entry.relpath = base_name + "/" + rel.generic_string();
        entry.abspath = de.path().string();
        entry.size = size;
        entry.sha256 = "";
        LOG_DEBUG("Discovered file: %s (%llu bytes)", entry.relpath.c_str(), (unsigned long long)size);
        entries.push_back(std::move(entry));
    }
    if (ec) {
        LOG_WARN("Error while walking directory %s — %s", dir_path.c_str(), ec.message().c_str());
    }

    return entries;
}

bool fs_is_safe_relpath(const std::string& relpath) {
    if (relpath.empty()) return false;

    fs::path p(relpath);
    if (p.is_absolute()) return false;

    for (const auto& part : p) {
        if (part == "..") return false;
    }
    return true;
}

std::string fs_join(const std::string& dir, const std::string& relpath) {
    return (fs::path(dir) / relpath).string();
}

bool fs_ensure_parent_dirs(const std::string& file_path) {
    fs::path parent = fs::path(file_path).parent_path();
    if (parent.empty()) return true;

    std::error_code ec;
    fs::create_directories(parent, ec);
    if (ec) {
        LOG_ERROR("Failed to create directory %s — %s", parent.string().c_str(), ec.message().c_str());
        return false;
    }
    return true;
}

} // namespace flashshare
