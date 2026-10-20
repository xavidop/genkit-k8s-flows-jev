#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
kind delete cluster --name "${KIND_CLUSTER:-senro-ai}"
rm -rf .kind
