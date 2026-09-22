#!/usr/bin/env bash

set -euo pipefail

readonly topic="transactions.raw"
readonly expected_partitions="3"
readonly expected_replication_factor="1"
readonly bootstrap_server="localhost:19092"
readonly registry_subject="transactions.raw-value"
readonly expected_compatibility="BACKWARD_TRANSITIVE"
readonly registry_url="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly repository_root="$(cd "${script_dir}/.." && pwd)"
readonly canonical_schema="${repository_root}/contracts/events/transaction-event-v1.avsc"
readonly temporary_directory="$(mktemp -d)"

cleanup() {
  rm -rf "${temporary_directory}"
}
trap cleanup EXIT

fail() {
  printf 'Kafka infrastructure verification failed: %s\n' "$1" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is unavailable"
command -v curl >/dev/null 2>&1 || fail "curl is unavailable"
command -v python3 >/dev/null 2>&1 || fail "python3 is unavailable"
docker compose version >/dev/null 2>&1 || fail "Docker Compose is unavailable"
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server "${bootstrap_server}" >/dev/null 2>&1 || fail "Kafka is unavailable"

topic_list="$({
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap_server}" \
    --list
} 2>/dev/null)" || fail "Kafka topic metadata could not be listed"

printf '%s\n' "${topic_list}" | grep -Fx "${topic}" >/dev/null || fail "${topic} does not exist"

description="$({
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap_server}" \
    --describe \
    --topic "${topic}"
} 2>/dev/null)" || fail "${topic} metadata could not be described"

summary="$(printf '%s\n' "${description}" | awk -v topic="${topic}" '
  $0 ~ "^Topic: " topic "[[:space:]]" && $0 ~ /PartitionCount:/ { print; exit }
')"

[[ -n "${summary}" ]] || fail "${topic} summary metadata is missing"
[[ "${summary}" == *"PartitionCount: ${expected_partitions}"* ]] ||
  fail "${topic} partition count is not ${expected_partitions}"
[[ "${summary}" == *"ReplicationFactor: ${expected_replication_factor}"* ]] ||
  fail "${topic} replication factor is not ${expected_replication_factor}"

schema_topic_description="$({
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap_server}" \
    --describe \
    --topic _schemas
} 2>/dev/null)" || fail "Schema Registry metadata topic is unavailable"

[[ "${schema_topic_description}" == *"ReplicationFactor: 1"* ]] ||
  fail "Schema Registry metadata topic replication factor is not 1"

curl --fail --silent --show-error "${registry_url}/subjects" >/dev/null 2>&1 ||
  fail "Schema Registry is unavailable at ${registry_url}"

readonly schema_response="${temporary_directory}/schema-version.json"
readonly compatibility_response="${temporary_directory}/compatibility.json"

curl --fail --silent --show-error \
  "${registry_url}/subjects/${registry_subject}/versions/1" >"${schema_response}" ||
  fail "${registry_subject} version 1 does not exist"

curl --fail --silent --show-error \
  "${registry_url}/config/${registry_subject}" >"${compatibility_response}" ||
  fail "${registry_subject} subject-level compatibility is not configured"

registry_summary="$({
  python3 - "${canonical_schema}" "${schema_response}" "${compatibility_response}" <<'PY'
import json
import pathlib
import sys

canonical = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
registered_response = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
compatibility_response = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding="utf-8"))
registered = json.loads(registered_response["schema"])

if registered_response.get("subject") != "transactions.raw-value":
    raise SystemExit("unexpected Schema Registry subject")
if registered_response.get("version") != 1:
    raise SystemExit("unexpected Schema Registry version")
if not isinstance(registered_response.get("id"), int) or registered_response["id"] < 1:
    raise SystemExit("invalid Schema Registry schema ID")
if registered != canonical:
    raise SystemExit("registered schema does not match the canonical repository schema")

compatibility = compatibility_response.get(
    "compatibilityLevel", compatibility_response.get("compatibility")
)
if compatibility != "BACKWARD_TRANSITIVE":
    raise SystemExit(f"unexpected compatibility: {compatibility}")

print(
    f"subject=transactions.raw-value version=1 id={registered_response['id']} "
    "compatibility=BACKWARD_TRANSITIVE"
)
PY
} 2>&1)" || fail "${registry_summary}"

printf 'Kafka infrastructure verified: %s has %s partitions and replication factor %s.\n' \
  "${topic}" \
  "${expected_partitions}" \
  "${expected_replication_factor}"
printf 'Schema Registry verified: %s.\n' "${registry_summary}"
