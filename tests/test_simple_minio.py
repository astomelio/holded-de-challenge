#!/usr/bin/env python3
"""
Simple test script for MinIO pipeline
"""

import json
import time
import requests
from datetime import datetime

def test_minio_pipeline():
    """Test the MinIO pipeline with simple events"""
    
    print("🧪 Simple MinIO Pipeline Test")
    print("=" * 40)
    
    # Test data
    test_events = [
        {
            "event_id": f"test-{int(time.time())}-001",
            "event_type": "USER_LOGIN",
            "user_id": "user123",
            "company_id": "company456",
            "timestamp": int(time.time() * 1000),
            "metadata": {"source": "test", "version": "1.0"}
        },
        {
            "event_id": f"test-{int(time.time())}-002", 
            "event_type": "INVOICE_CREATED",
            "user_id": "user789",
            "company_id": "company456",
            "timestamp": int(time.time() * 1000),
            "amount": 150.50,
            "metadata": {"source": "test", "version": "1.0"}
        }
    ]
    
    print("1️⃣ Testing API health...")
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            health_data = response.json()
            print(f"✅ API Status: {health_data.get('status', 'unknown')}")
            print(f"✅ MinIO Connected: {health_data.get('minio_connected', False)}")
        else:
            print(f"❌ API Health Check Failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to API: {e}")
        return False
    
    print("\n2️⃣ Sending test events...")
    try:
        response = requests.post(
            "http://localhost:8000/collect",
            json=test_events,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Events sent successfully!")
            print(f"✅ Total processed: {result.get('total_processed', 0)}")
            for event_result in result.get('results', []):
                print(f"   - Event {event_result.get('event_id')}: {event_result.get('status')}")
            return True
        else:
            print(f"❌ Failed to send events: {response.status_code}")
            print(f"❌ Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error sending events: {e}")
        return False

if __name__ == "__main__":
    success = test_minio_pipeline()
    if success:
        print("\n🎉 Test completed successfully!")
    else:
        print("\n💥 Test failed!")
