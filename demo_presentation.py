#!/usr/bin/env python3
"""
Holded Data Pipeline - Live Demo Script
This script demonstrates the complete data pipeline in action
"""

import json
import httpx
import time
import random
from datetime import datetime

def create_demo_event():
    """Create a realistic Holded event for demonstration"""
    event_types = [
        "Holded\\Wallet\\Domain\\Transaction\\Events\\WalletTransactionCreatedEvent",
        "Holded\\Wallet\\Domain\\Account\\Events\\AccountCreatedEvent", 
        "Holded\\Wallet\\Domain\\AccountHolder\\Events\\AccountHolderUpdatedEvent",
        "Holded\\Wallet\\Domain\\User\\Events\\UserUpdatedEvent",
        "Holded\\Wallet\\Domain\\Subscription\\Events\\WalletSubscriptionFailedEvent"
    ]
    
    companies = [
        "demo_company_001", "demo_company_002", "demo_company_003",
        "demo_company_004", "demo_company_005"
    ]
    
    users = [
        "user_001", "user_002", "user_003", "user_004", "user_005"
    ]
    
    return {
        "data": {
            "id": f"demo_{int(time.time() * 1000)}",
            "amount": round(random.uniform(10.0, 1000.0), 2)
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

def send_demo_events(count=5):
    """Send demo events to the API"""
    print(f"🚀 Sending {count} demo events to the API...")
    
    events = [create_demo_event() for _ in range(count)]
    
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
        print(f"❌ Error sending events: {e}")
        return False

def check_database_status():
    """Check current database status"""
    import subprocess
    
    try:
        result = subprocess.run([
            'docker', 'exec', 'mysql', 'mysql', '-u', 'holded', '-pholded123', 
            '-e', 'SELECT COUNT(*) as total_events FROM holded_events.events;'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                count = lines[1].strip()
                print(f"📊 Current events in database: {count}")
                return int(count)
        return 0
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        return 0

def main():
    """Main demo function"""
    print("🎯 Holded Data Pipeline - Live Demo")
    print("=" * 50)
    
    # Check API health
    print("1️⃣ Checking API health...")
    try:
        with httpx.Client() as client:
            response = client.get("http://localhost:8000/health", timeout=5)
            if response.status_code == 200:
                health = response.json()
                print(f"✅ API Status: {health['status']}")
                print(f"✅ Kafka Connected: {health['kafka_connected']}")
            else:
                print("❌ API not responding")
                return
    except Exception as e:
        print(f"❌ Cannot connect to API: {e}")
        return
    
    # Check initial database state
    print("\n2️⃣ Checking initial database state...")
    initial_count = check_database_status()
    
    # Send demo events
    print(f"\n3️⃣ Sending demo events...")
    if send_demo_events(5):
        print("✅ Events sent successfully!")
        
        # Wait for processing
        print("\n4️⃣ Waiting for processing (5 seconds)...")
        time.sleep(5)
        
        # Check final database state
        print("\n5️⃣ Checking final database state...")
        final_count = check_database_status()
        
        new_events = final_count - initial_count
        print(f"📈 New events processed: {new_events}")
        
        if new_events > 0:
            print("\n🎉 DEMO SUCCESSFUL!")
            print("✅ Events were sent to API")
            print("✅ Events were processed by Beam")
            print("✅ Events were stored in MySQL")
            print("✅ Pipeline is working end-to-end!")
        else:
            print("\n⚠️ No new events detected")
            print("💡 Check if Gold Sink is running")
    else:
        print("❌ Failed to send events")

if __name__ == "__main__":
    main()
