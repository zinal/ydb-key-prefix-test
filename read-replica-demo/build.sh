#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
cd "$root"

out="${1:-read-replica-demo}"
exec go build -o "$out" .
