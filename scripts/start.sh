#!/usr/bin/env bash

# Start script for Holded Data Engineer Challenge
# This script sets up and starts the complete data pipeline

set -e

echo "🚀 Starting Holded Data Engineer Challenge Pipeline"
echo "=================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install docker-compose first."
    exit 1
fi

echo "📦 Starting infrastructure services..."
docker-compose up -d zookeeper kafka schema-registry mysql

echo "⏳ Waiting for services to be ready..."
sleep 30

echo "📋 Creating Kafka topics..."
./scripts/create_topics.sh

echo "📝 Registering Avro schema..."
./scripts/register_schema.sh

echo "🏃‍♂️ Starting application services..."
docker-compose up -d api beam-pipeline

echo "✅ Pipeline started successfully!"
echo ""
echo "🌐 Services available at:"
echo "  - API: http://localhost:8000"
echo "  - API Docs: http://localhost:8000/docs"
echo "  - Schema Registry: http://localhost:8081"
echo "  - MySQL: localhost:3306"
echo ""
echo "📊 To test the pipeline:"
echo "  curl -X POST 'http://localhost:8000/events' \\"
echo "       -H 'Content-Type: application/json' \\"
echo "       -d '{\"event_type\": \"USER_LOGIN\", \"company_id\": \"test_company\", \"user_id\": \"test_user\"}'"
echo ""
echo "🔍 To view logs:"
echo "  docker-compose logs -f api"
echo "  docker-compose logs -f beam-pipeline"
echo ""
echo "🛑 To stop the pipeline:"
echo "  docker-compose down"
