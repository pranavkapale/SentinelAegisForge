#!/usr/bin/env bash

set -euo pipefail

readonly topic="features.ready"
readonly bootstrap="localhost:19092"

docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server "${bootstrap}" >/dev/null

if ! docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${bootstrap}" --list | grep -Fx "${topic}" >/dev/null; then
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap}" --create --if-not-exists \
    --topic "${topic}" --partitions 3 --replication-factor 1 \
    --config cleanup.policy=delete
fi

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${bootstrap}" --describe --topic "${topic}"
