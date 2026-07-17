dd if=/dev/urandom of=/tmp/testfile.bin bs=1M count=10

# Start receiver in background
rm -rf /tmp/flashshare_recv && mkdir -p /tmp/flashshare_recv
./build/flashshare recv --port 5119 --out /tmp/flashshare_recv --accept-all &>/tmp/recv.log &

# Wait a moment, then send
sleep 1
./build/flashshare send /tmp/testfile.bin --to 127.0.0.1 --port 5119

# Stop receiver
kill %1

# Verify the transfer
echo "=== Receiver log ==="
cat /tmp/recv.log
echo "=== Verify ==="
ls -la /tmp/flashshare_recv/
md5sum /tmp/testfile.bin /tmp/flashshare_recv/testfile.bin

#The end