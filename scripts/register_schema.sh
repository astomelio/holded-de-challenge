#!/usr/bin/env bash

# Register Avro schema with Schema Registry
# This script registers the HoldedEvent schema with Confluent Schema Registry

set -e

SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-"http://localhost:8081"}
SCHEMA_FILE="schemas/event.avsc"
TOPIC_NAME="events.raw"

echo "Registering Avro schema for Holded events..."

# Check if schema registry is available
if ! curl -s "$SCHEMA_REGISTRY_URL/subjects" > /dev/null; then
    echo "Error: Schema Registry is not available at $SCHEMA_REGISTRY_URL"
    echo "Please make sure Confluent Schema Registry is running"
    exit 1
fi

# Check if schema file exists
if [ ! -f "$SCHEMA_FILE" ]; then
    echo "Error: Schema file $SCHEMA_FILE not found"
    exit 1
fi

# Register the schema
echo "Registering schema for topic: $TOPIC_NAME"
curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": \"$(cat $SCHEMA_FILE | jq -c .)\"}" \
    "$SCHEMA_REGISTRY_URL/subjects/$TOPIC_NAME-value/versions"

echo ""
echo "Schema registered successfully!"
echo ""
echo "You can verify the schema with:"
echo "curl -X GET $SCHEMA_REGISTRY_URL/subjects/$TOPIC_NAME-value/versions/latest"
