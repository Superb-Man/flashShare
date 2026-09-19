#pragma once

#include "net/connection.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace flashshare {

struct PreparedManifest {
    std::vector<FileEntry> files;
    std::uint64_t total_size = 0; // Total size of all files in the manifest
};

std::shared_ptr<const PreparedManifest> prepare_manifest(const std::vector<std::string>& paths);

} // namespace flashshare