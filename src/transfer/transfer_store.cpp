#include "transfer/transfer_store.h"

#include "util/fs.h"
#include "util/logger.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <system_error>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

// Simple storage utility
// Co authored By - Cline

namespace flashshare {

namespace fs = std::filesystem;

namespace {

std::string session_key(const std::string& sender_public_ip, const std::string& transfer_id) {
    return sender_public_ip + "/" + transfer_id;
}

std::string safe_ip_directory_name(const std::string& ip) {
    std::string result;
    result.reserve(ip.size());

    for (unsigned char ch : ip) {
        result += (std::isalnum(ch) || ch == '.' || ch == '-' || ch == '_')
            ? static_cast<char>(ch)
            : '_';
    }

    return result.empty() ? "unknown-sender" : result;
}

bool sync_file(const std::string& path) {
#ifndef _WIN32
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;

    bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
#else
    (void)path;
    return true;
#endif
}


bool write_transfer(std::ostream& out, const StoredTransfer& transfer) {
    out << "FLASHSHARE_TRANSFER_V1\n";
    out << std::quoted(transfer.transfer_id) << '\n';
    out << std::quoted(transfer.sender_public_ip) << '\n';
    out << std::quoted(transfer.manifest_fingerprint) << '\n';
    out << transfer.encrypted << ' '
        << transfer.completed << '\n';
    out << transfer.files.size() << '\n';

    for (const auto& file : transfer.files) {
        out << file.file_index << ' '
            << file.expected_size << ' '
            << file.durable_bytes << ' '
            << file.completed << '\n';

        out << std::quoted(file.requested_relpath) << '\n';
        out << std::quoted(file.final_relpath) << '\n';
        out << std::quoted(file.partial_path) << '\n';
        out << std::quoted(file.expected_sha256) << '\n';
    }

    return static_cast<bool>(out);
}

bool read_transfer(std::istream& in, StoredTransfer& transfer) {
    std::string format;
    if (!std::getline(in, format) || format != "FLASHSHARE_TRANSFER_V1") {
        return false;
    }

    if (!(in >> std::quoted(transfer.transfer_id)) ||
        !(in >> std::quoted(transfer.sender_public_ip)) ||
        !(in >> std::quoted(transfer.manifest_fingerprint)) ||
        !(in >> transfer.encrypted >> transfer.completed)) {
        return false;
    }

    size_t file_count = 0;
    if (!(in >> file_count)) return false;

    transfer.files.clear();
    transfer.files.reserve(file_count);

    for (size_t i = 0; i < file_count; ++i) {
        StoredFileProgress file;

        if (!(in >> file.file_index
                 >> file.expected_size
                 >> file.durable_bytes
                 >> file.completed) ||
            !(in >> std::quoted(file.requested_relpath)) ||
            !(in >> std::quoted(file.final_relpath)) ||
            !(in >> std::quoted(file.partial_path)) ||
            !(in >> std::quoted(file.expected_sha256))) {
            return false;
        }

        if (file.durable_bytes > file.expected_size) {
            return false;
        }

        transfer.files.push_back(std::move(file));
    }

    return true;
}

bool same_manifest(const StoredTransfer& transfer,
                   const TransferRequest& request,
                   const std::string& fingerprint) {
    if (transfer.manifest_fingerprint != fingerprint ||
        transfer.encrypted != request.encrypted ||
        transfer.files.size() != request.files.size()) {
        return false;
    }

    for (size_t i = 0; i < request.files.size(); ++i) {
        const auto& stored = transfer.files[i];
        const auto& received = request.files[i];

        if (stored.file_index != i ||
            stored.requested_relpath != received.relpath ||
            stored.expected_size != received.size ||
            stored.expected_sha256 != received.sha256) {
            return false;
        }
    }

    return true;
}

} // namespace

TransferStore::TransferStore(std::string out_dir) : out_dir_(std::move(out_dir)) {}

std::string TransferStore::sender_dir(const std::string& sender_public_ip) const {
    return (fs::path(out_dir_) / safe_ip_directory_name(sender_public_ip)).string();
}

std::string TransferStore::journal_path(const std::string& sender_public_ip, const std::string& transfer_id) const {
    return (fs::path(sender_dir(sender_public_ip)) /
            ".flashshare" / "transfers" /
            (transfer_id + ".state")).string();
}

std::string TransferStore::partial_path(const std::string& sender_public_ip, const std::string& transfer_id, uint32_t file_index) const {
    return (fs::path(sender_dir(sender_public_ip)) /
            ".flashshare" / "partial" / transfer_id /
            (std::to_string(file_index) + ".part")).string();
}

std::string TransferStore::fingerprint_manifest(const TransferRequest& request) const {
    // This is used together with exact per-file comparisons in same_manifest().
    std::ostringstream input;
    input << request.encrypted << '\n' << request.files.size() << '\n';

    for (const auto& file : request.files) {
        input << file.relpath << '\n'
              << file.size << '\n'
              << file.sha256 << '\n';
    }

    // Stable FNV-1a identifier for the journal. Exact validation above is
    // still required; this value is not relied upon by itself for security.
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char ch : input.str()) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }

    std::ostringstream result;
    result << std::hex << hash;
    return result.str();
}

bool TransferStore::load(const std::string& sender_public_ip, const std::string& transfer_id, StoredTransfer& transfer) const {
    std::ifstream in(journal_path(sender_public_ip, transfer_id));
    if (!in) return false;

    if (!read_transfer(in, transfer) ||
        transfer.transfer_id != transfer_id ||
        transfer.sender_public_ip != sender_public_ip) {
        LOG_ERROR("Invalid transfer journal for %s", transfer_id.c_str());
        return false;
    }

    for (auto& file : transfer.files) {
        if (file.completed) continue;

        std::error_code ec;
        uint64_t actual_size = fs::exists(file.partial_path, ec)
            ? fs::file_size(file.partial_path, ec)
            : 0;

        if (ec) {
            LOG_ERROR("Cannot inspect partial file %s: %s",
                      file.partial_path.c_str(), ec.message().c_str());
            return false;
        }

        file.durable_bytes = std::min(
            { file.durable_bytes, actual_size, file.expected_size });
    }

    return true;
}

bool TransferStore::save(const StoredTransfer& transfer) const {
    const std::string path = journal_path(transfer.sender_public_ip, transfer.transfer_id);
    const std::string temporary = path + ".tmp";

    std::error_code ec;
    fs::create_directories(fs::path(path).parent_path(), ec);
    if (ec) {
        LOG_ERROR("Cannot create transfer journal directory: %s", ec.message().c_str());
        return false;
    }

    {
        std::ofstream out(temporary, std::ios::trunc);
        if (!out || !write_transfer(out, transfer)) {
            LOG_ERROR("Cannot write transfer journal: %s", temporary.c_str());
            return false;
        }
        out.flush();
        if (!out) return false;
    }

    if (!sync_file(temporary)) {
        LOG_ERROR("Cannot sync transfer journal: %s", temporary.c_str());
        return false;
    }

    fs::rename(temporary, path, ec);
    if (ec) {
        LOG_ERROR("Cannot replace transfer journal %s: %s",
                  path.c_str(), ec.message().c_str());
        return false;
    }

    return true;
}

std::optional<StoredTransfer> TransferStore::open_or_create(
    const std::string& sender_public_ip,
    const TransferRequest& request) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (request.transfer_id.empty()) {
        LOG_ERROR("Cannot create recovery state without transfer ID");
        return std::nullopt;
    }

    const std::string key = session_key(sender_public_ip, request.transfer_id);
    const std::string fingerprint = fingerprint_manifest(request);

    StoredTransfer transfer;
    if (load(sender_public_ip, request.transfer_id, transfer)) {
        if (!same_manifest(transfer, request, fingerprint)) {
            LOG_ERROR("Transfer %s has a different manifest than its journal",
                      request.transfer_id.c_str());
            return std::nullopt;
        }

        open_transfers_[key] = transfer;
        return transfer;
    }

    transfer.transfer_id = request.transfer_id;
    transfer.sender_public_ip = sender_public_ip;
    transfer.manifest_fingerprint = fingerprint;
    transfer.encrypted = request.encrypted;

    for (size_t i = 0; i < request.files.size(); ++i) {
        const auto& requested = request.files[i];

        if (!fs_is_safe_relpath(requested.relpath)) {
            LOG_ERROR("Unsafe manifest path: %s", requested.relpath.c_str());
            return std::nullopt;
        }

        StoredFileProgress file;
        file.file_index = static_cast<uint32_t>(i);
        file.requested_relpath = requested.relpath;
        file.final_relpath = allocate_unique_relpath(
            sender_dir(sender_public_ip), requested.relpath);
        file.partial_path = partial_path(
            sender_public_ip, request.transfer_id, file.file_index);
        file.expected_size = requested.size;
        file.expected_sha256 = requested.sha256;

        transfer.files.push_back(std::move(file));
    }

    if (!save(transfer)) return std::nullopt;

    open_transfers_[key] = transfer;
    return transfer;
}

std::vector<uint64_t> TransferStore::resume_offsets(
    const StoredTransfer& transfer) const {
    std::vector<uint64_t> offsets;
    offsets.reserve(transfer.files.size());

    for (const auto& file : transfer.files) {
        offsets.push_back(file.completed ? file.expected_size : file.durable_bytes);
    }

    return offsets;
}

std::string TransferStore::final_path(const std::string& sender_public_ip,
                                      const StoredFileProgress& file) const {
    return (fs::path(sender_dir(sender_public_ip)) / file.final_relpath).string();
}

bool TransferStore::acquire_session(const std::string& sender_public_ip,
                                    const std::string& transfer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_sessions_.insert(
        session_key(sender_public_ip, transfer_id)).second;
}

void TransferStore::release_session(const std::string& sender_public_ip, const std::string& transfer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = session_key(sender_public_ip, transfer_id);

    active_sessions_.erase(key);
    open_transfers_.erase(key);
}

std::string TransferStore::allocate_unique_relpath(
    const std::string& sender_directory,
    const std::string& requested_relpath) const {
    fs::path requested(requested_relpath);
    fs::path parent = requested.parent_path();

    const std::string stem = requested.stem().string();
    const std::string extension = requested.extension().string();

    for (uint64_t counter = 0; ; ++counter) {
        const std::string filename = counter == 0
            ? requested.filename().string()
            : stem + " (" + std::to_string(counter) + ")" + extension;

        const fs::path candidate = parent / filename;
        const fs::path final_path = fs::path(sender_directory) / candidate;

        // A reservation protects incomplete transfers too. Without it, a
        // restarted receiver could assign the same name to another transfer
        // because the first transfer has only a hidden .part file.
        const fs::path reservation =
            fs::path(sender_directory) / ".flashshare" / "reservations" / candidate;

        std::error_code ec;
        if (fs::exists(final_path, ec) || fs::exists(reservation, ec)) {
            continue;
        }

        fs::create_directories(reservation.parent_path(), ec);
        if (ec) {
            LOG_ERROR("Cannot create name-reservation directory: %s",
                      ec.message().c_str());
            return "";
        }

        // Receiver workers share TransferStore's mutex, so checking then
        // creating this marker is safe within this receiver process.
        std::ofstream marker(reservation, std::ios::trunc);
        if (!marker) {
            LOG_ERROR("Cannot reserve destination name: %s",
                      candidate.string().c_str());
            return "";
        }

        return candidate.generic_string();
    }
}

bool TransferStore::checkpoint(const std::string& sender_public_ip,
                               const std::string& transfer_id,
                               uint32_t file_index,
                               uint64_t durable_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);

    const std::string key = session_key(sender_public_ip, transfer_id);
    auto transfer_it = open_transfers_.find(key);
    if (transfer_it == open_transfers_.end()) {
        LOG_ERROR("Checkpoint requested for unopened transfer %s",
                  transfer_id.c_str());
        return false;
    }

    StoredTransfer& transfer = transfer_it->second;
    if (file_index >= transfer.files.size()) {
        LOG_ERROR("Invalid file index %u in transfer %s",
                  file_index, transfer_id.c_str());
        return false;
    }

    StoredFileProgress& file = transfer.files[file_index];

    // Recovery progress must only move forward and never beyond the manifest.
    if (durable_bytes < file.durable_bytes ||
        durable_bytes > file.expected_size) {
        LOG_ERROR("Invalid checkpoint %llu for file %u in transfer %s",
                  static_cast<unsigned long long>(durable_bytes),
                  file_index, transfer_id.c_str());
        return false;
    }

    if (file.completed && durable_bytes != file.expected_size) {
        LOG_ERROR("Cannot alter completed file %u in transfer %s",
                  file_index, transfer_id.c_str());
        return false;
    }

    const uint64_t previous = file.durable_bytes;
    file.durable_bytes = durable_bytes;

    if (!save(transfer)) {
        file.durable_bytes = previous;
        return false;
    }

    return true;
}

bool TransferStore::complete_file(const std::string& sender_public_ip,
                                  const std::string& transfer_id,
                                  uint32_t file_index) {
    std::lock_guard<std::mutex> lock(mutex_);

    const std::string key = session_key(sender_public_ip, transfer_id);
    auto transfer_it = open_transfers_.find(key);
    if (transfer_it == open_transfers_.end()) {
        LOG_ERROR("Completion requested for unopened transfer %s",
                  transfer_id.c_str());
        return false;
    }

    StoredTransfer& transfer = transfer_it->second;
    if (file_index >= transfer.files.size()) {
        return false;
    }

    StoredFileProgress& file = transfer.files[file_index];
    if (file.completed) {
        return true;
    }

    if (file.durable_bytes != file.expected_size) {
        LOG_ERROR("Cannot complete file %u: only %llu/%llu bytes received",
                  file_index,
                  static_cast<unsigned long long>(file.durable_bytes),
                  static_cast<unsigned long long>(file.expected_size));
        return false;
    }

    const fs::path final_path =
        fs::path(sender_dir(sender_public_ip)) / file.final_relpath;

    if (!fs_ensure_parent_dirs(final_path.string())) {
        return false;
    }

    std::error_code ec;
    if (fs::exists(final_path, ec)) {
        LOG_ERROR("Refusing to overwrite existing destination: %s",
                  final_path.string().c_str());
        return false;
    }

    // filesystem, which they are inside the sender-IP directory.
    fs::rename(file.partial_path, final_path, ec);
    if (ec) {
        LOG_ERROR("Cannot finalize %s: %s",
                  final_path.string().c_str(), ec.message().c_str());
        return false;
    }

    const fs::path reservation = fs::path(sender_dir(sender_public_ip)) / ".flashshare" / "reservations" / file.final_relpath;

    fs::remove(reservation, ec);
    if (ec) {
        LOG_WARN("Cannot remove destination reservation %s: %s",
                 reservation.string().c_str(), ec.message().c_str());
    }

    file.completed = true;

    if (!save(transfer)) {
        LOG_ERROR("File was renamed but completion journal update failed: %s",
                  final_path.string().c_str());
        return false;
    }

    return true;
}

bool TransferStore::complete_transfer(const std::string& sender_public_ip, const std::string& transfer_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    const std::string key = session_key(sender_public_ip, transfer_id);
    auto transfer_it = open_transfers_.find(key);
    if (transfer_it == open_transfers_.end()) {
        return false;
    }

    StoredTransfer& transfer = transfer_it->second;

    for (const auto& file : transfer.files) {
        if (!file.completed) {
            LOG_ERROR("Cannot complete transfer %s: file %u is incomplete",
                      transfer_id.c_str(), file.file_index);
            return false;
        }
    }

    transfer.completed = true;
    return save(transfer);
}

} // namespace flashshare