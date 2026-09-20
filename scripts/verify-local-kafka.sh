#!/usr/bin/env bash

set -euo pipefail

readonly topic="transactions.raw"
readonly expected_partitions="3"
readonly expected_replication_factor="1"
readonly bootstrap_server="localhost:19092"

fail() {
  printf 'Kafka infrastructure verification failed: %s\n' "$1" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is unavailable"
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

printf 'Kafka infrastructure verified: %s has %s partitions and replication factor %s.\n' \
  "${topic}" \
  "${expected_partitions}" \
  "${expected_replication_factor}"
