#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
delta_path="${repository_root}/.local/delta/customer_activity_snapshots"
checkpoint_path="${repository_root}/.local/checkpoints/customer-activity-state"

for target in "${delta_path}" "${checkpoint_path}"; do
  case "${target}" in
    "${repository_root}/.local/"*) rm -rf -- "${target}" ;;
    *)
      echo "Refusing to remove path outside the repository's .local directory: ${target}" >&2
      exit 1
      ;;
  esac
done

echo "Removed the default local customer-state snapshots and coupled state checkpoint."
