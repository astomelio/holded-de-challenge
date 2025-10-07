import json
import uuid
import time
from datetime import datetime
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Holded Events API",
    description="API for ingesting business events into the data pipeline",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kafka producer configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "events.raw"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8') if v else None
)

# Pydantic models
class EventMetadata(BaseModel):
    """Additional metadata for the event"""
    pass

class EventRequest(BaseModel):
    """Request model for event ingestion"""
    event_type: str = Field(..., description="Type of event")
    user_id: Optional[str] = Field(None, description="User ID")
    company_id: str = Field(..., description="Company ID")
    metadata: Optional[Dict[str, str]] = Field(default_factory=dict, description="Additional metadata")
    ip_address: Optional[str] = Field(None, description="IP address")
    user_agent: Optional[str] = Field(None, description="User agent")
    session_id: Optional[str] = Field(None, description="Session ID")

class HoldedEventData(BaseModel):
    """Data section of Holded event"""
    id: str
    amount: Optional[float] = None

class HoldedEventHeaders(BaseModel):
    """Headers section of Holded event"""
    type: str
    targetSubscription: str
    # Usar Field con alias para manejar los nombres complejos
    new_context_stamp: Optional[Dict[str, Any]] = Field(None, alias="X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\NewContextStamp")
    timestamp_stamp: Optional[Dict[str, Any]] = Field(None, alias="X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\TimestampStamp")
    message_id_stamp: Optional[Dict[str, Any]] = Field(None, alias="X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\MessageIdStamp")
    amplitude_track_stamp: Optional[Dict[str, Any]] = Field(None, alias="X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\AmplitudeTrackStamp")

class HoldedEventRequest(BaseModel):
    """Request model for Holded events (exact format from Holded)"""
    data: HoldedEventData
    headers: HoldedEventHeaders

class EventResponse(BaseModel):
    """Response model for event ingestion"""
    event_id: str
    status: str
    timestamp: int

@app.get("/")
def root():
    """Health check endpoint"""
    return {"status": "ok", "service": "holded-events-api"}

@app.get("/health")
def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "timestamp": int(time.time() * 1000),
        "kafka_connected": True  # In production, check actual Kafka connection
    }

@app.post("/collect")
async def collect_holded_events(request: Request):
    """
    Collect Holded events in their original format.
    
    This endpoint receives Holded events exactly as they come from Holded
    and publishes them to Kafka for processing.
    """
    try:
        # Parse JSON body
        body = await request.json()
        
        # Handle both single event and list of events
        if isinstance(body, dict):
            events = [body]
        elif isinstance(body, list):
            events = body
        else:
            raise HTTPException(status_code=400, detail="Invalid JSON format")
        
        results = []
        
        for holded_event in events:
            # Generate unique event ID
            event_id = str(uuid.uuid4())
            
            # Extract information from Holded event
            data = holded_event.get('data', {})
            headers = holded_event.get('headers', {})
            
            # Extract context information
            context_stamp = headers.get('X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\NewContextStamp', {})
            timestamp_stamp = headers.get('X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\TimestampStamp', {})
            
            # Create event payload preserving Holded structure
            event_payload = {
                "event_id": event_id,
                "original_holded_event": holded_event,  # Keep original event
                "event_type": headers.get('type', 'UNKNOWN'),
                "user_id": context_stamp.get('userId'),
                "company_id": context_stamp.get('accountId', 'unknown'),
                "timestamp": int(time.time() * 1000),
                "holded_timestamp": timestamp_stamp.get('dispatchedAt'),
                "target_subscription": headers.get('targetSubscription'),
                "message_id": headers.get('X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\MessageIdStamp', {}).get('id'),
                "platform": context_stamp.get('requestPlatform'),
                "tracked_device": headers.get('X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\AmplitudeTrackStamp', {}).get('trackedDevice'),
                "ip_address": request.client.host,
                "user_agent": request.headers.get('user-agent'),
                "session_id": None  # Not available in Holded events
            }
            
            # Publish to Kafka
            producer.send(
                TOPIC_NAME,
                key=event_id,
                value=event_payload
            )
            
            results.append({
                "event_id": event_id,
                "status": "accepted",
                "timestamp": event_payload["timestamp"]
            })
            
            logger.info(f"Holded event {event_id} published to Kafka")
        
        return {"results": results, "total_processed": len(results)}
        
    except Exception as e:
        logger.error(f"Error processing Holded events: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/events", response_model=EventResponse)
async def create_event(event: EventRequest, request: Request):
    """
    Ingest a new business event into the data pipeline.
    
    This endpoint receives business events and publishes them to Kafka
    for further processing by the data pipeline.
    """
    try:
        # Generate unique event ID
        event_id = str(uuid.uuid4())
        
        # Get client IP if not provided
        if not event.ip_address:
            event.ip_address = request.client.host
        
        # Create event payload
        event_payload = {
            "event_id": event_id,
            "event_type": event.event_type,
            "user_id": event.user_id,
            "company_id": event.company_id,
            "timestamp": int(time.time() * 1000),
            "metadata": event.metadata or {},
            "ip_address": event.ip_address,
            "user_agent": event.user_agent,
            "session_id": event.session_id
        }
        
        # Publish to Kafka
        producer.send(
            TOPIC_NAME,
            key=event_id,
            value=event_payload
        )
        
        logger.info(f"Event {event_id} published to Kafka")
        
        return EventResponse(
            event_id=event_id,
            status="accepted",
            timestamp=event_payload["timestamp"]
        )
        
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/events/{event_id}")
async def get_event_status(event_id: str):
    """
    Get the status of a specific event.
    
    Note: This is a placeholder implementation. In a real system,
    you would query the database or event store for the event status.
    """
    return {
        "event_id": event_id,
        "status": "processed",  # Placeholder
        "message": "Event status endpoint - implement based on your requirements"
    }

@app.get("/metrics")
async def get_metrics():
    """Get API metrics (placeholder implementation)"""
    return {
        "total_events_processed": 0,  # Implement actual metrics collection
        "events_per_second": 0,
        "error_rate": 0.0
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
