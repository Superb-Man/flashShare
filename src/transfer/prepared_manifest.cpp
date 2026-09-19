#include "transfer/prepared_manifest.h"

#include "util/fs.h"
#include "util/logger.h"
#include "util/sha256.h"

#include <filesystem>
#include <limits>
#include <stdexcept>
#include <utility>

namespace flashshare {

namespace fs = std::filesystem;

std::shared_ptr<const PreparedManifest> prepare_manifest(
    const std::vector<std::string>& paths)
{
    if (paths.empty()) {
        throw std::runtime_error("No source paths provided");
    }

    auto manifest = std::make_shared<PreparedManifest>();

    // Collect metadata for one regular file.
    auto add_file = [&](const fs::path& source, const std::string& relative_path) {
        if (!fs_is_safe_relpath(relative_path)) {
            throw std::runtime_error("Invalid destination path: " + relative_path);
        }

        FileEntry entry{};
        entry.abspath = fs::absolute(source).string();
        entry.relpath = relative_path;

        const auto file_size = fs::file_size(source);
        const auto remaining_capacity = std::numeric_limits<std::uint64_t>::max() - manifest->total_size;

        if (file_size > remaining_capacity) {
            throw std::runtime_error("Total source size exceeds the supported limit");
        }

        entry.size = static_cast<std::uint64_t>(file_size);

        manifest->total_size += entry.size;
        manifest->files.push_back(std::move(entry));
    };

    for (const auto& raw_path : paths) {
        const fs::path source(raw_path);
        const auto status = fs::status(source);

        if (!fs::exists(status)) {
            throw std::runtime_error("Source does not exist: " + raw_path);
        }

        if (fs::is_regular_file(status)) {
            add_file(source, source.filename().generic_string());
            continue;
        }

        if (!fs::is_directory(status)) {
            throw std::runtime_error("Source is not a regular file or directory: " + raw_path);
        }

        const fs::path root = fs::canonical(source);
        const std::string directory_name = root.filename().generic_string();

        if (directory_name.empty()) {
            throw std::runtime_error("Source directory needs a directory name: " + raw_path);
        }

        // Preserve the existing directory layout:
        // project/subdir/file.txt
        for (const auto& item : fs::recursive_directory_iterator(
            root, 
            fs::directory_options::skip_permission_denied)) {
            if (!item.is_regular_file()) {
                continue;
            }

            const fs::path relative = item.path().lexically_relative(root);

            add_file(item.path(), directory_name + "/" + relative.generic_string());
        }
    }

    if (manifest->files.empty()) {
        throw std::runtime_error(
            "No regular files found in the source paths");
    }

    // no need of hashing per destination, do it once during preparation.
    for (auto& entry : manifest->files) {
        entry.sha256 = SHA256::file_hash(entry.abspath);

        if (entry.sha256.size() != 64) {
            throw std::runtime_error(
                "Failed to calculate SHA-256: " + entry.abspath);
        }

        if (fs::file_size(entry.abspath) != entry.size) {
            throw std::runtime_error(
                "Source size changed during preparation: "
                + entry.abspath);
        }

        LOG_DEBUG(
            "Prepared %s: %llu bytes, SHA-256 %s",
            entry.relpath.c_str(),
            static_cast<unsigned long long>(entry.size),
            entry.sha256.c_str());
    }

    LOG_INFO(
        "Prepared manifest: %zu file(s), %llu byte(s)", manifest->files.size(),
        static_cast<unsigned long long>(manifest->total_size)
    );

    return manifest;
}

} // namespace flashshare