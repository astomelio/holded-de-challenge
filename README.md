# Holded Data Engineering Challenge

This repository contains a complete implementation of a real-time data pipeline for business event processing, designed for the Holded Senior Data Engineer challenge.

## 🏗️ Architecture Overview

The system implements a **Medallion Architecture** (Bronze, Silver, Gold layers) with the following components:

```
📱 Holded Events → 🌐 FastAPI → 📤 Kafka → ⚡ Apache Beam → 📤 Kafka → 🥇 MySQL
```

### 🥉 Bronze Layer (Raw Data)
- **FastAPI API** receives Holded events in their original format
- **Kafka** stores raw events in `events.raw` topic
- **Schema Registry** validates event structure

### 🥈 Silver Layer (Processed Data)
- **Apache Beam** processes events with validation, enrichment, and deduplication
- **Kafka** stores processed events in `events.silver` topic
- **Dead Letter Queue** handles invalid events

### 🥇 Gold Layer (Analytics Ready)
- **MySQL Database** stores final processed events
- **Aggregated tables** for analytics and reporting
- **Optimized indexes** for query performance

## 📁 Repository Structure

```
holded-de-challenge/
├── api/                    # FastAPI application
│   ├── main.py            # Main API with /collect endpoint
│   ├── pyproject.toml     # Dependencies
│   └── Dockerfile         # Container configuration
├── beam/                   # Apache Beam pipeline
│   ├── pipeline.py        # Main processing pipeline
│   ├── gold_sink.py       # MySQL sink for Gold layer
│   ├── pyproject.toml     # Dependencies
│   └── Dockerfile         # Container configuration
├── sql/                    # Database schema
│   └── 001_create_events.sql  # Table creation scripts
├── schemas/                # Data schemas
│   └── event.avsc         # Avro schema for events
├── scripts/               # Configuration scripts
│   ├── create_topics.sh   # Kafka topics creation
│   ├── register_schema.sh # Schema registry setup
│   ├── start.sh          # Complete system startup
│   └── topics/
│       └── main-topics   # Topic configurations
├── events.json           # Real Holded test events
├── test_events.py        # Test script for sending events
├── docker-compose.yml     # Complete infrastructure
└── README.md             # This file
```

## 🚀 Implemented Features

### 🌐 API Layer
- **REST API** with FastAPI framework
- **Event ingestion** endpoint `/collect` for Holded events
- **Data validation** with Pydantic
- **Kafka integration** for streaming
- **Health checks** and monitoring
- **Automatic documentation** with Swagger/OpenAPI
- **Basic metrics** and monitoring

### ⚡ Processing Layer (Apache Beam)
- **Event validation** with invalid data filtering
- **Data deduplication** based on event_id
- **Data enrichment** with business context
- **Business context classification** (user_activity, product_management, etc.)
- **Data quality scoring** (0.0-1.0)
- **Dead Letter Queue** for invalid events

### 🗄️ Storage Layer (MySQL)
- **Main events table** with optimized indexes
- **Summary tables** for analytics and aggregations
- **Dead letter table** for error handling
- **Partitioning support** for large datasets
- **JSON metadata** storage for flexible schemas

## 🛠️ Technologies Used

### Backend & API
- **FastAPI**: Modern, fast web framework
- **Python 3.13**: Main programming language
- **Pydantic**: Data validation and serialization
- **Avro**: Data serialization with schema
- **Poetry**: Dependency management

### Streaming & Processing
- **Apache Kafka**: Distributed streaming platform
- **Apache Beam**: Unified programming model for data processing
- **kafka-python**: Python client for Kafka
- **Schema Registry**: Schema management

### Database & Storage
- **MySQL 8.0**: Relational database
- **pymysql**: Python MySQL client
- **JSON**: Flexible metadata storage

### Infrastructure
- **Docker & Docker Compose**: Containerization
- **Zookeeper**: Kafka coordination
- **Poetry**: Python dependency management

## 🚀 Installation and Setup

### Prerequisites
- Docker Desktop
- Python 3.13+
- Poetry (for dependency management)

### Quick Start

1. **Clone the repository**
```bash
git clone <repository-url>
cd holded-de-challenge
```

2. **Start the complete infrastructure**
```bash
./scripts/start.sh
```

3. **Verify the system is running**
```bash
curl http://localhost:8000/health
```

The API will be available at `http://localhost:8000`

### Manual Setup

1. **Start infrastructure with Docker Compose**
```bash
docker-compose up -d
```

2. **Create Kafka topics**
```bash
./scripts/create_topics.sh
```

3. **Register Avro schemas**
```bash
./scripts/register_schema.sh
```

4. **Initialize database**
```bash
docker exec mysql mysql -u holded -pholded123 holded_events < sql/001_create_events.sql
```

5. **Start the API**
```bash
cd api && poetry install && poetry run python main.py
```

6. **Start the Beam pipeline**
```bash
cd beam && poetry install && poetry run python pipeline.py
```

7. **Start the Gold sink**
```bash
cd beam && poetry run python gold_sink.py
```

## 🏃‍♂️ Usage

### Sending Events

The API accepts Holded events in their original format:

```bash
curl -X POST "http://localhost:8000/collect" \
  -H "Content-Type: application/json" \
  -d '[{
    "data": {"id": "123", "amount": 100.50},
    "headers": {
      "type": "Holded\\Wallet\\Domain\\Transaction\\Events\\WalletTransactionCreatedEvent",
      "targetSubscription": "Wallet",
      "X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\NewContextStamp": {
        "accountId": "company-123",
        "userId": "user-456"
      }
    }
  }]'
```

### Using Test Data

Send real Holded events from the test dataset:

```bash
python test_events.py
```

### Monitoring

- **API Health**: `http://localhost:8000/health`
- **API Documentation**: `http://localhost:8000/docs`
- **Kafka Topics**: Use Kafka tools or UI
- **Database**: Connect to MySQL on localhost:3306

## 📊 Data Flow

1. **Event Ingestion**: Holded events arrive at `/collect` endpoint
2. **Raw Storage**: Events stored in Kafka `events.raw` topic
3. **Processing**: Apache Beam validates, enriches, and deduplicates events
4. **Silver Storage**: Processed events stored in Kafka `events.silver` topic
5. **Gold Storage**: Final events written to MySQL database
6. **Analytics**: Aggregated data available for reporting

## 🔧 Configuration

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection (default: localhost:9092)
- `MYSQL_HOST`: MySQL host (default: localhost)
- `MYSQL_USER`: MySQL user (default: holded)
- `MYSQL_PASSWORD`: MySQL password (default: holded123)
- `MYSQL_DATABASE`: MySQL database (default: holded_events)

### Kafka Topics
- `events.raw`: Raw Holded events
- `events.silver`: Processed events
- `events.dead.letter`: Invalid events
- `events.analytics`: Analytics events

## 📈 Performance & Scalability

### Optimization Features
- **Kafka partitioning** for parallelization
- **Database indexing** for fast queries
- **Connection pooling** for database efficiency
- **Batch processing** in Beam pipeline

### Monitoring
- **Health checks** in the API
- **Basic metrics** in the API
- **Error logging** throughout the system
- **Dead letter queue** for failed events

## 🔒 Security

### Implemented Security Measures
- **Input validation** on all endpoints
- **Data sanitization** in the pipeline
- **Audit logs** for compliance
- **Error handling** with proper logging

## 📚 Additional Documentation

- **API Documentation**: Available at `/docs` endpoint
- **Schema Definitions**: In `schemas/` directory
- **Database Schema**: In `sql/` directory
- **Docker Configuration**: In `docker-compose.yml`

## 🧪 Testing

The system includes:
- **Integration tests** with real Holded data
- **Event generators** for load testing
- **Health checks** for monitoring
- **Error handling** validation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is part of the Holded technical challenge for the Senior Data Engineer position.

---

**Status**: ✅ Complete and functional
**Last Updated**: January 2025
**Version**: 1.0.0