# FlashShare

High-speed LAN file transfer CLI tool for fast large file transfers, with resume support, multi-receiver fan-out, and relay chaining.

## Build

### Prerequisites

**Ubuntu/Debian:**
```bash
sudo apt install cmake build-essential libssl-dev
```

**Fedora/RHEL:**
```bash
sudo dnf install cmake gcc-c++ openssl-devel
```

### Normal build

```bash
cd flashshare
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

The binary is at `build/flashshare`.

### Static build (portable — runs on any Linux without dependencies)

```bash
cd flashshare
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DOPENSSL_USE_STATIC_LIBS=TRUE \
         -DCMAKE_EXE_LINKER_FLAGS="-static -lpthread -ldl"
make -j$(nproc)
```

Produces a ~7.5 MB binary with no external library dependencies. Copy it to any Linux machine and run.

## Usage

```
FlashShare — High-speed LAN file transfer

USAGE:
  flashshare send <path> [<path>...] --to <ip[,ip...]> [options]
  flashshare recv [options]

COMMANDS:
  send <path> [<path>...]   Send one or more files and/or directories
  recv                      Listen for incoming transfers
  help                      Show this help message
  version                   Show version

OPTIONS:
  --to <ip[,ip...]>  Receiver IPv4 addresses, separated by commas
  --port <n>         Port number (default: 5117)
  --out <dir>        Output directory (default: current dir)
  --encrypt          Enable AES-256-GCM encryption
  --relay            Chain the --to receivers (S -> R1 -> R2 -> R3) instead
                     of fanning out to each of them from the sender
  --resume           Resume an interrupted transfer
  --accept-all       Auto-accept incoming transfers (no prompt)
  --verbose          Enable debug logging
  --log-file <f>     Mirror log output to file (default in --daemon mode:
                     /tmp/flashshare_receiver.log)
  -h, --help         Show help
  -v, --version      Show version
```

## Examples

### Basic file transfer

```bash
# Receiver (machine B):
flashshare recv --port 5117 --out ~/downloads --accept-all

# Sender (machine A):
flashshare send bigfile.iso --to 192.168.1.50 --port 5117
```

### Send multiple files and directories

```bash
flashshare send a.txt b.txt ./project/ --to 192.168.1.50
```

### Encrypted transfer

```bash
flashshare send secret.zip --to 192.168.1.50 --encrypt
```

### Resume an interrupted transfer

```bash
flashshare send bigfile.iso --to 192.168.1.50 --resume
```

If the connection drops, the sender automatically retries with exponential backoff and resumes from the receiver's last checkpointed offset.

### Multi-receiver fan-out

Send to multiple receivers simultaneously — each receiver gets a full copy:

```bash
flashshare send bigfile.iso --to 192.168.1.50,192.168.1.51,192.168.1.52 --resume
```

### Relay chain

Chain receivers: S → R1 → R2 → R3. Each receiver forwards to the next:

```bash
flashshare send bigfile.iso --to 192.168.1.50,192.168.1.51,192.168.1.52 --relay
```

### Next Direction
- Adapting it to low thread creation ovehead / epoll.
- Splice Tee instead of sendfile. And during recovery for aligning nodes to same offset first -> sendFile(). Then -> again Tee().
- Can possibly include io/uring to avoid kernel-userspace trap overhead(Maybe not needed for a small LAN).
- Testing one node in various connection/relay and design the sceanrio, if needed- implement then(Need to experiment whether io uring can actually help here).
- Currently no mechanism is done for slow reciver which hinders the overall file sharing. Can adapt to a hybrid chain rebuilding/recovery algorithm later.