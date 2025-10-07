# Holded Data Engineering Challenge - Project Structure

## 📁 Repository Organization

This repository implements a complete real-time data pipeline for processing Holded business events using modern data engineering practices.

### 🏗️ Architecture Overview

```
Holded Events → FastAPI → Kafka → Apache Beam → Kafka → MySQL
     ↓              ↓         ↓         ↓         ↓        ↓
  Raw Data    Bronze Layer  Processing Silver Layer  Gold Layer
```

### 📂 Directory Structure

```
holded-de-challenge/
├── api/                           # FastAPI Application
│   ├── main.py                   # Main API with /collect endpoint
│   ├── pyproject.toml           # Python dependencies
│   ├── poetry.lock              # Locked dependencies
│   └── Dockerfile               # Container configuration
│
├── beam/                         # Apache Beam Processing
│   ├── pipeline.py              # Main Beam pipeline
│   ├── gold_sink.py             # MySQL sink for Gold layer
│   ├── pyproject.toml           # Python dependencies
│   ├── poetry.lock              # Locked dependencies
│   └── Dockerfile               # Container configuration
│
├── sql/                          # Database Schema
│   └── 001_create_events.sql    # MySQL table definitions
│
├── schemas/                      # Data Schemas
│   └── event.avsc               # Avro schema for events
│
├── scripts/                      # Configuration Scripts
│   ├── create_topics.sh         # Kafka topics creation
│   ├── register_schema.sh       # Schema registry setup
│   ├── start.sh                 # Complete system startup
│   └── topics/
│       └── main-topics          # Topic configurations
│
├── test-data/                    # Test Data
│   └── de-sde-challenge/        # Original Holded test data
│       ├── generator/
│       │   ├── events.json      # Real Holded events
│       │   └── generator.py     # Event generator
│       ├── api/                 # Original API structure
│       ├── beam/                # Original Beam structure
│       └── schemas/             # Original schemas
│
├── docker-compose.yml           # Complete infrastructure
├── send_holded_events.py        # Test event sender
├── test_integration.py          # Integration tests
├── README.md                    # Main documentation
└── PROJECT_STRUCTURE.md         # This file
```

## 🔧 Component Details

### 🌐 API Layer (`/api/`)
- **Purpose**: Receives Holded events in their original format
- **Technology**: FastAPI, Pydantic, Kafka
- **Key Features**:
  - `/collect` endpoint for Holded events
  - Data validation with Pydantic
  - Kafka producer for streaming
  - Health checks and monitoring
  - Auto-generated API documentation

### ⚡ Processing Layer (`/beam/`)
- **Purpose**: Processes events with validation, enrichment, and deduplication
- **Technology**: Apache Beam, Kafka, Python
- **Key Features**:
  - Event validation and filtering
  - Data deduplication
  - Business context classification
  - Data quality scoring
  - Dead letter queue handling

### 🗄️ Storage Layer (`/sql/`)
- **Purpose**: Stores processed events for analytics
- **Technology**: MySQL 8.0
- **Key Features**:
  - Optimized table structure
  - Indexes for performance
  - JSON metadata storage
  - Aggregation tables
  - Dead letter queue table

### 🐳 Infrastructure (`/scripts/`, `docker-compose.yml`)
- **Purpose**: Complete system orchestration
- **Technology**: Docker, Docker Compose, Kafka, Zookeeper
- **Key Features**:
  - One-command startup
  - Kafka topic management
  - Schema registry setup
  - Database initialization

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Python 3.13+
- Poetry

### Complete System Startup
```bash
# Start everything
./scripts/start.sh

# Or manually
docker-compose up -d
./scripts/create_topics.sh
./scripts/register_schema.sh
```

### Individual Component Startup
```bash
# API
cd api && poetry install && poetry run python main.py

# Beam Pipeline
cd beam && poetry install && poetry run python pipeline.py

# Gold Sink
cd beam && poetry run python gold_sink.py
```

## 📊 Data Flow

1. **Event Ingestion**: Holded events → FastAPI `/collect`
2. **Bronze Layer**: Raw events → Kafka `events.raw`
3. **Processing**: Apache Beam validates, enriches, deduplicates
4. **Silver Layer**: Processed events → Kafka `events.silver`
5. **Gold Layer**: Final events → MySQL database
6. **Analytics**: Aggregated data available for reporting

## 🔍 Key Files Explained

### `api/main.py`
- FastAPI application with `/collect` endpoint
- Handles Holded events in original format
- Publishes to Kafka `events.raw` topic

### `beam/pipeline.py`
- Apache Beam streaming pipeline
- Processes events from `events.raw`
- Publishes to `events.silver`

### `beam/gold_sink.py`
- Kafka consumer for `events.silver`
- Writes to MySQL database
- Handles aggregations and summaries

### `sql/001_create_events.sql`
- MySQL table definitions
- Optimized indexes
- JSON metadata support

### `docker-compose.yml`
- Complete infrastructure definition
- Kafka, Zookeeper, MySQL, Schema Registry
- API and Beam services

## 🧪 Testing

### Send Test Events
```bash
python send_holded_events.py
```

### Verify System Health
```bash
curl http://localhost:8000/health
curl http://localhost:8000/docs
```

### Check Database
```bash
docker exec mysql mysql -u holded -pholded123 holded_events -e "SELECT COUNT(*) FROM events;"
```

## 📈 Monitoring

- **API Health**: `http://localhost:8000/health`
- **API Docs**: `http://localhost:8000/docs`
- **Kafka Topics**: Use Kafka tools
- **Database**: Connect to MySQL on localhost:3306

## 🔒 Security & Best Practices

- Input validation on all endpoints
- Data sanitization in pipeline
- Error handling with dead letter queue
- Audit logging for compliance
- Containerized deployment
- Environment variable configuration

---

**Status**: ✅ Production Ready
**Last Updated**: January 2025
**Version**: 1.0.0
