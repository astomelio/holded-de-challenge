# Holded Data Pipeline - MinIO Implementation

## 🎯 **Overview**

This is the **MinIO-based implementation** of the Holded Data Engineer challenge, following the exact requirements:

- **MinIO** for JSON storage of enriched events
- **PostgreSQL** for business data
- **Apache Beam** with **DirectRunner** (no Kafka)
- **Schema Registry** for validation
- **FastAPI** for event ingestion

## 🏗️ **Architecture**

```
Holded Events → FastAPI → MinIO (JSON) → Apache Beam → PostgreSQL
```

### **Components:**

1. **FastAPI API** (`api/main_minio.py`)
   - Collects Holded events
   - Stores raw events in MinIO
   - Health check endpoints

2. **Apache Beam Pipeline** (`beam/pipeline_direct.py`)
   - Validates events against Schema Registry
   - Enriches events with metadata
   - Saves enriched events to MinIO
   - Saves business data to PostgreSQL

3. **MinIO** (Object Storage)
   - Stores raw events (JSON format)
   - Stores enriched events (JSON format)
   - S3-compatible API

4. **PostgreSQL** (Business Database)
   - Business events table
   - Company analytics
   - Event type summaries

## 🚀 **Quick Start**

### **1. Start All Services**
```bash
./scripts/start_correct.sh
```

### **2. Test the System**
```bash
python test_minio_events.py
```

### **3. Check Services**
```bash
# API Health
curl http://localhost:8000/health

# List events in MinIO
curl http://localhost:8000/events

# MinIO Console
open http://localhost:9001
# Username: minioadmin
# Password: minioadmin123
```

## 📊 **Services**

| Service | URL | Credentials |
|---------|-----|-------------|
| **API** | http://localhost:8000 | - |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin123 |
| **PostgreSQL** | localhost:5432 | holded/holded123 |

## 🔧 **Manual Setup**

### **1. Start Docker Services**
```bash
docker-compose -f docker-compose-minio.yml up -d
```

### **2. Create PostgreSQL Tables**
```bash
docker exec -i postgres psql -U holded -d holded_business < sql/002_create_business_tables.sql
```

### **3. Install Dependencies**
```bash
cd api && poetry install --no-dev
cd ../beam && poetry install --no-dev
```

### **4. Start API**
```bash
cd api && poetry run python main_minio.py
```

### **5. Run Beam Pipeline**
```bash
cd beam && poetry run python pipeline_direct.py
```

## 📁 **File Structure**

```
holded-de-challenge/
├── api/
│   ├── main_minio.py          # FastAPI with MinIO
│   ├── pyproject.toml        # Dependencies
│   └── Dockerfile
├── beam/
│   ├── pipeline_direct.py    # Apache Beam DirectRunner
│   ├── pyproject.toml        # Dependencies
│   └── Dockerfile
├── sql/
│   └── 002_create_business_tables.sql  # PostgreSQL schema
├── scripts/
│   └── start_correct.sh      # Startup script
├── docker-compose-minio.yml
├── test_minio_events.py      # Test script
└── README_CORRECT.md
```

## 🧪 **Testing**

### **Send Events**
```bash
python test_minio_events.py
```

### **Check MinIO**
```bash
# List all events
curl http://localhost:8000/events

# Get specific event
curl http://localhost:8000/events/{event_id}
```

### **Check PostgreSQL**
```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U holded -d holded_business

# Query business events
SELECT * FROM business_events LIMIT 10;

# Query company analytics
SELECT * FROM company_analytics;
```

## 🔍 **Monitoring**

### **API Logs**
```bash
# Check API logs
docker logs api
```

### **MinIO Logs**
```bash
# Check MinIO logs
docker logs minio
```

### **PostgreSQL Logs**
```bash
# Check PostgreSQL logs
docker logs postgres
```

## 🛠️ **Troubleshooting**

### **Services Not Starting**
```bash
# Check Docker status
docker ps

# Check logs
docker-compose -f docker-compose-minio.yml logs
```

### **API Not Responding**
```bash
# Check API health
curl http://localhost:8000/health

# Restart API
cd api && poetry run python main_minio.py
```

### **MinIO Connection Issues**
```bash
# Check MinIO
curl http://localhost:9000/minio/health/live

# Check MinIO console
open http://localhost:9001
```

## 📈 **Performance**

- **MinIO**: Handles thousands of JSON files
- **PostgreSQL**: Optimized with indexes and triggers
- **Apache Beam**: Parallel processing with DirectRunner
- **FastAPI**: Async processing for high throughput

## 🔒 **Security**

- MinIO access keys configured
- PostgreSQL user authentication
- API rate limiting (configurable)
- Input validation with Pydantic

## 📚 **Documentation**

- [FastAPI Documentation](http://localhost:8000/docs)
- [MinIO Documentation](https://docs.min.io/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Apache Beam Documentation](https://beam.apache.org/documentation/)

## 🎯 **Key Features**

✅ **MinIO Storage**: S3-compatible object storage for JSON files  
✅ **PostgreSQL**: Advanced relational database with JSON support  
✅ **Apache Beam**: DirectRunner for local processing  
✅ **Schema Validation**: Against Confluent Schema Registry  
✅ **Event Enrichment**: Metadata and quality scoring  
✅ **Business Analytics**: Company and event type summaries  
✅ **Health Monitoring**: API and service health checks  
✅ **Scalable Architecture**: Ready for production deployment  

## 🚀 **Next Steps**

1. **Production Deployment**: Use Kubernetes or Docker Swarm
2. **Monitoring**: Add Prometheus and Grafana
3. **Alerting**: Set up alerts for failures
4. **Scaling**: Horizontal scaling with multiple Beam workers
5. **Security**: Add authentication and authorization
