#!/usr/bin/env bash

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=== Toolchain ==="
cat rust-toolchain.toml

export DOCKER_BUILDKIT=1
export GHCR_ORG=xline-kv
export BUILD_TAG="ghcr.io/${GHCR_ORG}/build-env:latest"

# Change this version if rust-rocksdb updates in `engine`
export LIB_ROCKS_SYS_VER="0.11.0+8.1.1"

echo "=== Arch ==="
arch

echo "=== Docker build ==="
set -x

docker build -t ${BUILD_TAG} --progress=plain --no-cache --build-arg LIB_ROCKS_SYS_VER=$LIB_ROCKS_SYS_VER .

set +x

echo "=== Docker login ==="
echo -n $GITHUB_TOKEN | docker login --username $GHCR_ORG --password-stdin "ghcr.io/${GHCR_ORG}"

echo "=== Docker push ==="
docker push ${BUILD_TAG}
