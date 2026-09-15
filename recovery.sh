#!/usr/bin/env bash
set -euo pipefail

# Integration test:
# - begin one resumable transfer
# - wait until the receiver has written partial bytes
# - kill the receiver
# - restart it with the same output directory
# - verify sender reconnects and final file hash matches

binary="${1:-./build/flashshare}"
port=5120
work_dir="$(mktemp -d /tmp/flashshare-resume-test.XXXXXX)"
source_file="$work_dir/source.bin"
output_dir="$work_dir/output"
receiver_log="$work_dir/receiver.log"

receiver_pid=""
sender_pid=""

cleanup() {
    if [[ -n "$sender_pid" ]] && kill -0 "$sender_pid" 2>/dev/null; then
        kill "$sender_pid" 2>/dev/null || true
    fi

    if [[ -n "$receiver_pid" ]] && kill -0 "$receiver_pid" 2>/dev/null; then
        kill "$receiver_pid" 2>/dev/null || true
    fi

    rm -rf "$work_dir"
}
trap cleanup EXIT

start_receiver() {
    "$binary" recv \
        --port "$port" \
        --out "$output_dir" \
        --accept-all \
        >"$receiver_log" 2>&1 &

    receiver_pid="$!"
    sleep 0.2
}

mkdir -p "$output_dir"

# A large file makes it likely the test catches an active partial transfer,
# even on a fast loopback interface.
dd if=/dev/urandom of="$source_file" bs=1M count=10240 status=none

start_receiver

FLASHSHARE_TEST_SEND_DELAY_US=5000 "$binary" send "$source_file" \
    --to 127.0.0.1 \
    --port "$port" \
    --resume \
    >"$work_dir/sender.log" 2>&1 &

sender_pid="$!"

partial_seen=false

# The receiver stores localhost sender state under output/127.0.0.1/.
for _ in $(seq 1 500); do
    partial_file="$(find "$output_dir/127.0.0.1/.flashshare/partial" \
        -type f -name '*.part' -size +0c 2>/dev/null | head -n 1 || true)"

    if [[ -n "$partial_file" ]]; then
        partial_seen=true
        break
    fi

    sleep 0.02
done

if [[ "$partial_seen" != true ]]; then
    echo "FAIL: transfer completed before partial progress could be observed"
    exit 1
fi

# Simulate receiver-process destruction. The sender remains alive and retries.
kill "$receiver_pid"
wait "$receiver_pid" 2>/dev/null || true
receiver_pid=""

start_receiver

wait "$sender_pid"
sender_pid=""

received_file="$output_dir/127.0.0.1/source.bin"

if [[ ! -f "$received_file" ]]; then
    echo "FAIL: final received file does not exist"
    exit 1
fi

if find "$output_dir/127.0.0.1/.flashshare/partial" \
    -type f -name '*.part' 2>/dev/null | grep -q .; then
    echo "FAIL: partial file remains after successful completion"
    exit 1
fi

sha256sum --check <(
    sha256sum "$source_file" |
    sed "s|  $source_file$|  $received_file|"
)

echo "PASS: receiver restart recovered the interrupted transfer"