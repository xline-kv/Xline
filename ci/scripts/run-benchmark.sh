#!/usr/bin/env bash

set -euo pipefail

cargo build --release --bin xline --bin benchmark

cd scripts
cp ../target/release/{xline,benchmark} .

echo "=== Prepare Images ==="
docker build . -t ghcr.io/xline-kv/xline:local
docker pull ghcr.io/xline-kv/xline:latest
docker pull datenlord/etcd:v3.5.5


echo "=== Benchmark local changes ==="
export XLINE_IMAGE="ghcr.io/xline-kv/xline:local"
bash ./benchmark.sh xline
mv out out_local


echo "=== Benchmark remote changes ==="
export XLINE_IMAGE="ghcr.io/xline-kv/xline:latest"
bash ./benchmark.sh xline
mv out out_remote

echo "=== Merge Outputs ==="
echo -e "## Benchmark\r\n" >> outputs
echo -e "### PR \r\n\`\`\`txt" >> outputs
cat out_local/xline.txt >> outputs
echo "\`\`\`" >> outputs

echo -e "\r\n### Base \r\n\`\`\`txt" >> outputs
cat out_remote/xline.txt >> outputs
echo "\`\`\`" >> outputs