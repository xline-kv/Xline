#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail


wget -q https://github.com/kubernetes-sigs/kind/releases/download/v0.22.0/kind-linux-amd64
chmod +x kind-linux-amd64 && mv kind-linux-amd64 /usr/local/bin/kind

K8SVERSION=${K8SVERSION:-"v1.25.3"}
WORKSPACE=$PWD

sed -i  's/K8SVERSION/'$K8SVERSION'/g' $WORKSPACE/.github/scripts/kind.yaml
sed -i  's#WORKSPACE#'$WORKSPACE'#g' $WORKSPACE/.github/scripts/kind.yaml
kind create cluster --config $WORKSPACE/.github/scripts/kind.yaml
kubectl wait node --all --for condition=ready