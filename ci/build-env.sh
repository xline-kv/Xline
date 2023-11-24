#!/usr/bin/env bash

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=== Toolchain ==="
cat rust-toolchain.toml

export DOCKER_BUILDKIT=1

# Change this version to update the build env image
export BUILD_ENV_VERSION=v20231124
export BUILD_TAG="ghcr.io/igxnon/build-env:${BUILD_ENV_VERSION}"

# Change this version if rust-rocksdb updates in `engine`
export LIB_ROCKS_SYS_VER="0.11.0+8.1.1"

echo "=== Arch ==="
arch

echo "=== Docker login ==="
echo -n $GITHUB_TOKEN | docker login --username igxnon --password-stdin ghcr.io/igxnon

echo "=== Check image existence ==="
set +e
# remove all local images to ensure we fetch remote images
docker image rm ${BUILD_TAG}
# check manifest
if docker manifest inspect "${BUILD_TAG}"; then
    echo "=== Image already exists ==="
    echo "${BUILD_TAG} already exists -- skipping build image"
    exit 0
fi
set -e

echo "=== Docker build ==="

set -x

docker build -t ${BUILD_TAG} --progress=plain --no-cache --build-arg LIB_ROCKS_SYS_VER=$LIB_ROCKS_SYS_VER .

echo "=== Docker push ==="
docker push ${BUILD_TAG}
