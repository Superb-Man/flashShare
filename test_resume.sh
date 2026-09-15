# Sending machine
./flashshare send \
  /home/torique/Downloads/hello.zip \
  --to 107.109.215.115 \
  --port 5117 \
  --resume

# Receiving machine
# Ensure this machine/router accepts TCP port 5117.
./flashshare recv \
  --port 5117 \
  --out /mnt/c/Users/kz.toriqe/Downloads \
  --accept-all