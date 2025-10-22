#!/bin/bash

echo "🚀 Starting Holded Data Pipeline (Correct Implementation)"
echo "=================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"

# Start services
echo "🐳 Starting Docker services..."
docker-compose -f docker-compose-minio.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check MinIO
echo "📦 Checking MinIO..."
until curl -s http://localhost:9000/minio/health/live > /dev/null; do
    echo "   Waiting for MinIO..."
    sleep 2
done
echo "✅ MinIO is ready"

# Check PostgreSQL
echo "🐘 Checking PostgreSQL..."
until docker exec postgres pg_isready -U holded > /dev/null 2>&1; do
    echo "   Waiting for PostgreSQL..."
    sleep 2
done
echo "✅ PostgreSQL is ready"

# Create PostgreSQL tables
echo "📊 Creating PostgreSQL tables..."
docker exec -i postgres psql -U holded -d holded_business < sql/002_create_business_tables.sql
echo "✅ PostgreSQL tables created"

# Install Python dependencies
echo "📦 Installing Python dependencies..."
cd api && poetry install --no-dev
cd ../beam && poetry install --no-dev
cd ..

# Start API
echo "🚀 Starting API..."
cd api
poetry run python main_minio.py &
API_PID=$!
cd ..

# Wait for API to be ready
echo "⏳ Waiting for API to be ready..."
until curl -s http://localhost:8000/health > /dev/null; do
    echo "   Waiting for API..."
    sleep 2
done
echo "✅ API is ready"

# Run Apache Beam pipeline
echo "⚡ Running Apache Beam pipeline..."
cd beam
poetry run python pipeline_direct.py &
BEAM_PID=$!
cd ..

echo "🎉 All services are running!"
echo ""
echo "📊 Services:"
echo "  - API: http://localhost:8000"
echo "  - MinIO Console: http://localhost:9001 (admin/minioadmin123)"
echo "  - PostgreSQL: localhost:5432 (holded/holded123)"
echo ""
echo "🔧 To stop all services:"
echo "  kill $API_PID $BEAM_PID"
echo "  docker-compose -f docker-compose-minio.yml down"
echo ""
echo "📋 To test the system:"
echo "  python test_events.py"
echo "  curl http://localhost:8000/health"
echo "  curl http://localhost:8000/events"
