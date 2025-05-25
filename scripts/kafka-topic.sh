#!/bin/bash

echo "Start creating topic"

IFS=',' read -ra TOPIC_ARRAY <<<"$KAFKA_TOPICS"

for topic in "${TOPIC_ARRAY[@]}"; do
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --create \
    --topic "$topic" \
    --partitions "$KAFKA_PARTITIONS" \
    --replication-factor 1 --config "cleanup.policy=compact"
  echo "Topic $topic created"
done

echo "All topics created"
kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" --list
