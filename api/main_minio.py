#!/usr/bin/env python3
"""
FastAPI application for Holded events with MinIO storage
"""

import json
import uuid
import time
import logging
from datetime import datetime
from typing import Dict, Any, List
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
import requests
from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Holded Events API",
    description="API for collecting and processing Holded events",
    version="1.0.0"
)

# MinIO configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "holded-events"

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create bucket if not exists
try:
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        logger.info(f"Created bucket: {MINIO_BUCKET}")
except S3Error as e:
    logger.error(f"MinIO error: {e}")

# Pydantic models
class HoldedEventData(BaseModel):
    """Data section of Holded event"""
    id: str
    amount: float = None

class HoldedEventHeaders(BaseModel):
    """Headers section of Holded event"""
    type: str
    targetSubscription: str
    # Use Field with alias to handle complex names
    new_context_stamp: Dict[str, Any] = Field(None, alias="X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\NewContextStamp")
    timestamp_stamp: Dict[str, Any] = Field(None, alias="X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\TimestampStamp")
    message_id_stamp: Dict[str, Any] = Field(None, alias="X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\MessageIdStamp")
    amplitude_track_stamp: Dict[str, Any] = Field(None, alias="X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\AmplitudeTrackStamp")

class HoldedEventRequest(BaseModel):
    """Request model for Holded events (exact format from Holded)"""
    data: HoldedEventData
    headers: HoldedEventHeaders

class EventResponse(BaseModel):
    """Response model for event creation"""
    event_id: str
    status: str
    timestamp: int

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check MinIO connection
        minio_client.list_buckets()
        return {
            "status": "healthy",
            "timestamp": int(time.time() * 1000),
            "minio_connected": True
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": int(time.time() * 1000),
            "minio_connected": False,
            "error": str(e)
        }

@app.post("/collect")
async def collect_holded_events(request: Request):
    """
    Collect Holded events in their original format.
    
    This endpoint receives Holded events exactly as they come from Holded
    and stores them in MinIO for processing by Apache Beam.
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
            
            # Save to MinIO
            await save_to_minio(event_payload)
            
            results.append({
                "event_id": event_id,
                "status": "accepted",
                "timestamp": event_payload["timestamp"]
            })
            
            logger.info(f"Holded event {event_id} saved to MinIO")
        
        return {"results": results, "total_processed": len(results)}
        
    except Exception as e:
        logger.error(f"Error processing Holded events: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def save_to_minio(event_payload: Dict[str, Any]):
    """Save event to MinIO"""
    try:
        # Create object key
        event_id = event_payload['event_id']
        timestamp = datetime.now().strftime('%Y/%m/%d')
        object_key = f"raw-events/{timestamp}/{event_id}.json"
        
        # Convert to JSON
        json_data = json.dumps(event_payload, indent=2)
        
        # Upload to MinIO
        from io import BytesIO
        data = BytesIO(json_data.encode('utf-8'))
        minio_client.put_object(
            MINIO_BUCKET,
            object_key,
            data,
            length=len(json_data),
            content_type='application/json'
        )
        
        logger.info(f"Event {event_id} saved to MinIO: {object_key}")
        
    except Exception as e:
        logger.error(f"MinIO error: {e}")
        raise

@app.get("/events")
async def list_events():
    """List events from MinIO"""
    try:
        events = []
        objects = minio_client.list_objects(MINIO_BUCKET, prefix="raw-events/", recursive=True)
        
        for obj in objects:
            events.append({
                "object_name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified
            })
        
        return {"events": events, "total": len(events)}
        
    except Exception as e:
        logger.error(f"Error listing events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/{event_id}")
async def get_event(event_id: str):
    """Get specific event from MinIO"""
    try:
        # Find the object
        objects = minio_client.list_objects(MINIO_BUCKET, prefix=f"raw-events/", recursive=True)
        
        for obj in objects:
            if event_id in obj.object_name:
                # Get the object
                response = minio_client.get_object(MINIO_BUCKET, obj.object_name)
                event_data = json.loads(response.read().decode('utf-8'))
                return event_data
        
        raise HTTPException(status_code=404, detail="Event not found")
        
    except Exception as e:
        logger.error(f"Error getting event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
