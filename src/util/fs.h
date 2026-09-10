#pragma once

#include "net/connection.h"
#include <string>
#include <vector>

namespace flashshare {

// True if path exists and is a directory.
bool fs_is_directory(const std::string& path);

// Recursively walk a directory, returning a FileEntry for every regular file
// found. Each relpath is prefixed with the directory's own basename, e.g.
// walking "/home/x/project" yields relpaths like "project/src/main.cpp" so
// the directory structure is preserved on the receiving side.
std::vector<FileEntry> fs_walk_directory(const std::string& dir_path);

// Rejects relpaths that could escape the output directory (absolute paths,
// or any ".." path segment). Manifest relpaths come from the remote peer and
// must be validated before being joined onto a local output directory.
bool fs_is_safe_relpath(const std::string& relpath);

// Join a directory and a (validated) relative path, cross-platform.
std::string fs_join(const std::string& dir, const std::string& relpath);

// mkdir -p for the parent directory of file_path.
bool fs_ensure_parent_dirs(const std::string& file_path);

} // namespace flashshare
