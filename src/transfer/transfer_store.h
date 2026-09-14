#pragma once

#include "net/connection.h"

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>

namespace flashshare {

struct StoredFileProgress {
    uint32_t file_index = 0;

    // Original path sent in the manifest, such as "project/report.pdf".
    std::string requested_relpath;

    // Unique final destination chosen once at acceptance time.
    // Example: "project/report (1).pdf".
    std::string final_relpath;

    // Receiver-owned temporary file. Never exposed as a completed file.
    std::string partial_path;

    uint64_t expected_size = 0;
    std::string expected_sha256;

    // Bytes known to have been durably written to partial_path.
    uint64_t durable_bytes = 0;

    bool completed = false;
};

struct StoredTransfer {
    std::string transfer_id;

    // Directory namespace selected from the connected sender's public IP.
    std::string sender_public_ip;

    // Protects against accepting a different manifest for the same ID.
    std::string manifest_fingerprint;

    bool encrypted = false;
    bool active = false;
    bool completed = false;

    std::vector<StoredFileProgress> files;
};

/*
 * Owns persistent receiver recovery state.
 *
 * Disk layout:
 *   <out_dir>/<sender_public_ip>/
 *     <unique final files>
 *     .flashshare/
 *       transfers/<transfer_id>.json
 *       partial/<transfer_id>/<file_index>.part
 *
 * transfer_id, not IP, is the resume identity. The public IP only selects
 * the sender's destination directory.
 */
class TransferStore {
public:
    explicit TransferStore(std::string out_dir);

    // Load an existing matching transfer, or create and persist a new one.
    // For a new transfer this allocates unique final filenames once.
    std::optional<StoredTransfer> open_or_create(const std::string& sender_public_ip, const TransferRequest& request);

    // Returns receiver-confirmed offsets in manifest/file-index order.
    std::vector<uint64_t> resume_offsets(const StoredTransfer& transfer) const;

    // Mark a connection as owning this transfer. False means another live
    // connection already owns it; this prevents concurrent writes to .part files.
    bool acquire_session(const std::string& sender_public_ip, const std::string& transfer_id);

    void release_session(const std::string& sender_public_ip, const std::string& transfer_id);

    bool checkpoint(const std::string& sender_public_ip, const std::string& transfer_id, uint32_t file_index, uint64_t durable_bytes);

    bool complete_file(const std::string& sender_public_ip, const std::string& transfer_id, uint32_t file_index);

    bool complete_transfer(const std::string& sender_public_ip, const std::string& transfer_id);

private:
    std::string out_dir_;
    mutable std::mutex mutex_;
    std::unordered_set<std::string> active_sessions_;
    std::unordered_map<std::string, StoredTransfer> open_transfers_;

    std::string sender_dir(const std::string& sender_public_ip) const;
    std::string journal_path(const std::string& sender_public_ip, const std::string& transfer_id) const;
    std::string partial_path(const std::string& sender_public_ip, const std::string& transfer_id, uint32_t file_index) const;

    // Produces "name (1).ext", "name (2).ext", etc. without changing an
    // already selected name stored in a recovery journal.
    std::string allocate_unique_relpath(const std::string& sender_dir, const std::string& requested_relpath) const;
    std::string fingerprint_manifest(const TransferRequest& request) const;

    bool load(const std::string& sender_public_ip,
              const std::string& transfer_id,
              StoredTransfer& transfer) const;
    bool save(const StoredTransfer& transfer) const;
};

} // namespace flashshare