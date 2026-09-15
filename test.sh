dd if=/dev/urandom of=/tmp/testfile.bin bs=1M count=1024

# Start receiver in background
rm -rf /tmp/flashshare_recv && mkdir -p /tmp/flashshare_recv
./build/flashshare recv --port 5119 --out /tmp/flashshare_recv --accept-all &>/tmp/recv.log &
receiver_pid=$!

# Wait a moment, then send
sleep 1
./build/flashshare send /tmp/testfile.bin --to 127.0.0.1 --port 5119

# Sender completion means all bytes were handed to TCP. The receiver may
# still be writing and verifying the destination file, so wait for it before
# stopping the listener.
received_file="/tmp/flashshare_recv/127.0.0.1/testfile.bin"
for _ in $(seq 1 100); do
    [[ -f "$received_file" ]] && break
    sleep 0.1
done

# Verify the transfer
echo "=== Receiver log ==="
cat /tmp/recv.log
echo "=== Verify ==="
ls -la /tmp/flashshare_recv/127.0.0.1/
md5sum /tmp/testfile.bin "$received_file"

# Stop receiver after verification.
kill "$receiver_pid"
wait "$receiver_pid" 2>/dev/null || true

#The end
