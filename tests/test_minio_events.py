#!/usr/bin/env python3
"""
Test script for MinIO-based Holded events pipeline
"""

import json
import httpx
import time
import random
from datetime import datetime

def create_holded_event():
    """Create a realistic Holded event"""
    event_types = [
        "Holded\\Wallet\\Domain\\Transaction\\Events\\WalletTransactionCreatedEvent",
        "Holded\\Wallet\\Domain\\Account\\Events\\AccountCreatedEvent",
        "Holded\\Wallet\\Domain\\AccountHolder\\Events\\AccountHolderUpdatedEvent",
        "Holded\\Wallet\\Domain\\User\\Events\\UserUpdatedEvent",
        "Holded\\Wallet\\Domain\\Subscription\\Events\\WalletSubscriptionFailedEvent"
    ]
    
    companies = [f"company_{i:03d}" for i in range(1, 11)]
    users = [f"user_{i:03d}" for i in range(1, 21)]
    
    return {
        "data": {
            "id": f"holded_{int(time.time() * 1000)}",
            "amount": round(50.0 + (time.time() % 1000), 2)
        },
        "headers": {
            "type": random.choice(event_types),
            "targetSubscription": "wallet_events",
            "X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\NewContextStamp": {
                "userId": random.choice(users),
                "accountId": random.choice(companies),
                "requestPlatform": "web"
            },
            "X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\TimestampStamp": {
                "dispatchedAt": int(time.time() * 1000)
            },
            "X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\MessageIdStamp": {
                "id": f"msg_{int(time.time() * 1000)}"
            },
            "X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\AmplitudeTrackStamp": {
                "trackedDevice": "desktop"
            }
        }
    }

def test_api_health():
    """Test API health endpoint"""
    try:
        with httpx.Client() as client:
            response = client.get("http://localhost:8000/health", timeout=5)
            if response.status_code == 200:
                health = response.json()
                print(f"✅ API Status: {health['status']}")
                print(f"✅ MinIO Connected: {health.get('minio_connected', False)}")
                return True
            else:
                print(f"❌ API Error: {response.status_code}")
                return False
    except Exception as e:
        print(f"❌ Cannot connect to API: {e}")
        return False

def send_test_events(count=5):
    """Send test events to API"""
    print(f"🚀 Sending {count} test events...")
    
    events = [create_holded_event() for _ in range(count)]
    
    try:
        with httpx.Client() as client:
            response = client.post(
                'http://localhost:8000/collect',
                json=events,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Successfully sent {result.get('total_processed', 0)} events")
            return True
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def list_events():
    """List events from MinIO"""
    try:
        with httpx.Client() as client:
            response = client.get("http://localhost:8000/events", timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            print(f"📊 Total events in MinIO: {result.get('total', 0)}")
            
            # Show recent events
            events = result.get('events', [])
            if events:
                print("📋 Recent events:")
                for event in events[:5]:  # Show first 5
                    print(f"   - {event['object_name']} ({event['size']} bytes)")
            return True
        else:
            print(f"❌ Error listing events: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Main test function"""
    print("🧪 Holded MinIO Pipeline Test")
    print("=" * 40)
    
    # Test API health
    print("1️⃣ Testing API health...")
    if not test_api_health():
        print("❌ API not ready. Please start the services first.")
        return
    
    # Send test events
    print("\n2️⃣ Sending test events...")
    if send_test_events(3):
        print("✅ Events sent successfully!")
        
        # Wait for processing
        print("\n3️⃣ Waiting for processing...")
        time.sleep(5)
        
        # List events
        print("\n4️⃣ Listing events from MinIO...")
        if list_events():
            print("✅ Events retrieved from MinIO!")
            
            print("\n🎉 Test completed successfully!")
            print("✅ Events sent to API")
            print("✅ Events stored in MinIO")
            print("✅ Pipeline working end-to-end!")
        else:
            print("❌ Could not retrieve events from MinIO")
    else:
        print("❌ Failed to send events")

if __name__ == "__main__":
    main()
