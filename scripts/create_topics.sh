#!/usr/bin/env bash

# Create Kafka topics for Holded data pipeline
# This script sets up the necessary Kafka topics for the event streaming pipeline

set -e

KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-"localhost:9092"}

echo "Creating Kafka topics for Holded data pipeline..."

# Create raw events topic (bronze layer)
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create \
    --topic events.raw \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --if-not-exists

# Create silver events topic (processed events)
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create \
    --topic events.silver \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=2592000000 \
    --config cleanup.policy=delete \
    --if-not-exists

# Create dead letter queue topic
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create \
    --topic events.dead.letter \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=2592000000 \
    --config cleanup.policy=delete \
    --if-not-exists

# Create analytics topic for aggregated data
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create \
    --topic events.analytics \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=2592000000 \
    --config cleanup.policy=delete \
    --if-not-exists

echo "Kafka topics created successfully!"
echo ""
echo "Topics created:"
echo "- events.raw (bronze layer - raw events)"
echo "- events.silver (silver layer - processed events)"
echo "- events.dead.letter (invalid events)"
echo "- events.analytics (aggregated analytics data)"
echo ""
echo "You can list all topics with:"
echo "kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --list"
