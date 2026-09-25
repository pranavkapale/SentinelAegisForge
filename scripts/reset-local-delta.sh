#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
delta_path="${repository_root}/.local/delta/transactions_validated"
checkpoint_path="${repository_root}/.local/checkpoints/delta-transaction-ingestion"

for target in "${delta_path}" "${checkpoint_path}"; do
  case "${target}" in
    "${repository_root}/.local/"*) rm -rf -- "${target}" ;;
    *)
      echo "Refusing to remove path outside the repository's .local directory: ${target}" >&2
      exit 1
      ;;
  esac
done

echo "Removed the default local Delta table and its coupled streaming checkpoint."
