#!/bin/bash

##
## Start a Minio instance with I/O throttling to simulate connecting to a busy/slow Minio server.
## Used by the S3SlowMinioIntegrationSpec test
##

# Create a loopback device to throttle
LO_FILE="/tmp/minio-loopback.img"
LO_DEV=$(losetup -f)

dd if=/dev/zero of="$LO_FILE" bs=100M count=50
mkfs.ext4 "$LO_FILE"
sudo losetup -P "$LO_DEV" "$LO_FILE"

# Start Minio
# - Use the local volume driver to mount /dev/loopX directly - this allows us to pass the correct device to
#   device-write-bps for throttling. I wasn't able to get throttling to work with the usual -v/bind mounts
# - Set CONFIG_BLK_CGROUP and CONFIG_BLK_DEV_THROTTLING to enable throttling, following this guide:
#   https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v1/blkio-controller.html#throttling-upper-limit-policy
# - MINIO_FS_OSYNC needs to be set so that Mionio fsyncs to the disk, and throttling can take effect
docker run -i --rm \
  --name minio \
  --device "$LO_DEV" \
  --device-read-bps "$LO_DEV":1mb --device-write-bps "$LO_DEV":1mb \
  -e MINIO_ACCESS_KEY=TESTKEY -e MINIO_SECRET_KEY=TESTSECRET -e MINIO_DOMAIN=s3minio.alpakka -e MINIO_FS_OSYNC=true \
  -e CONFIG_BLK_CGROUP=y -e CONFIG_BLK_DEV_THROTTLING=y \
  --mount "type=volume,source=miniodata,target=/data,volume-driver=local,volume-opt=type=ext4,volume-opt=device=$LO_DEV" \
  -p 9001:9000 \
  minio/minio \
  server /data

# We can verify that the throttling is properly configured by running dd
# This command should take about 5 seconds to write 5 x 1mb blocks to our disk throttled to 1mb/s throttle
# docker exec minio sh -c "dd if=/dev/zero of=/data/test5 bs=1M count=5 conv=fsync"

# Cleanup loopback device and image
sudo losetup -d "$LO_DEV"
rm "$LO_FILE"
docker volume rm miniodata