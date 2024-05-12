#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

K8S_VERSION=${K8S_VERSION:-"v1.27.3"}
KIND_VERSION=${KIND_VERSION:-"0.22.0"}

wget -q https://github.com/kubernetes-sigs/kind/releases/download/v$KIND_VERSION/kind-linux-amd64
chmod +x kind-linux-amd64 && mv kind-linux-amd64 /usr/local/bin/kind

# print the config file
WORKSPACE=$PWD envsubst

WORKSPACE=$PWD envsubst < ci/artifacts/kind.yaml | kind create cluster -v7 --retain --wait 4m --config -
kubectl wait node --all --for condition=ready
