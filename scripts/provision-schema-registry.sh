#!/usr/bin/env bash

set -euo pipefail

readonly subject="transactions.raw-value"
readonly compatibility="BACKWARD_TRANSITIVE"
readonly registry_url="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly repository_root="$(cd "${script_dir}/.." && pwd)"
readonly schema_file="${repository_root}/contracts/events/transaction-event-v1.avsc"
readonly temporary_directory="$(mktemp -d)"
readonly registration_payload="${temporary_directory}/registration.json"
readonly registration_response="${temporary_directory}/registration-response.json"

cleanup() {
  rm -rf "${temporary_directory}"
}
trap cleanup EXIT

fail() {
  printf 'Schema Registry provisioning failed: %s\n' "$1" >&2
  exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is unavailable"
command -v python3 >/dev/null 2>&1 || fail "python3 is unavailable"
[[ -f "${schema_file}" ]] || fail "canonical schema is missing: ${schema_file}"

registry_ready="false"
for _ in $(seq 1 60); do
  if curl --fail --silent --show-error "${registry_url}/subjects" >/dev/null 2>&1; then
    registry_ready="true"
    break
  fi
  sleep 2
done
[[ "${registry_ready}" == "true" ]] || fail "${registry_url} did not become ready"

python3 - "${schema_file}" "${registration_payload}" <<'PY'
import json
import pathlib
import sys

schema_path = pathlib.Path(sys.argv[1])
payload_path = pathlib.Path(sys.argv[2])
schema_text = schema_path.read_text(encoding="utf-8")
payload_path.write_text(
    json.dumps({"schemaType": "AVRO", "schema": schema_text}),
    encoding="utf-8",
)
PY

curl --fail --silent --show-error \
  --request POST \
  --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data-binary "@${registration_payload}" \
  "${registry_url}/subjects/${subject}/versions" >"${registration_response}" ||
  fail "${subject} could not be registered"

curl --fail --silent --show-error \
  --request PUT \
  --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data "{\"compatibility\":\"${compatibility}\"}" \
  "${registry_url}/config/${subject}" >/dev/null ||
  fail "${subject} compatibility could not be configured"

versions="$(curl --fail --silent --show-error "${registry_url}/subjects/${subject}/versions")" ||
  fail "${subject} versions could not be read"

python3 - "${registration_response}" "${versions}" <<'PY'
import json
import pathlib
import sys

registration = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
versions = json.loads(sys.argv[2])
schema_id = registration.get("id")
if not isinstance(schema_id, int) or schema_id < 1:
    raise SystemExit("registration response did not contain a positive schema ID")
if versions != [1]:
    raise SystemExit(f"expected exactly schema version [1], received {versions}")
print(f"Schema Registry provisioned: transactions.raw-value version=1 id={schema_id} compatibility=BACKWARD_TRANSITIVE")
PY
