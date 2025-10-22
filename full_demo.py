#!/usr/bin/env python3
"""
Complete Demo Script for Holded Data Pipeline
Shows the entire pipeline working end-to-end
"""

import json
import httpx
import time
import subprocess
from datetime import datetime

def print_header(title):
    print(f"\n{'='*60}")
    print(f"🎯 {title}")
    print('='*60)

def print_step(step, description):
    print(f"\n{step} {description}")
    print("-" * 40)

def check_api_health():
    """Check if API is healthy"""
    try:
        with httpx.Client() as client:
            response = client.get("http://localhost:8000/health", timeout=5)
            if response.status_code == 200:
                health = response.json()
                print(f"✅ API Status: {health['status']}")
                print(f"✅ Kafka Connected: {health['kafka_connected']}")
                return True
            else:
                print(f"❌ API Error: {response.status_code}")
                return False
    except Exception as e:
        print(f"❌ Cannot connect to API: {e}")
        return False

def get_database_stats():
    """Get database statistics"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'mysql', 'mysql', '-u', 'holded', '-pholded123', 
            '-e', '''
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT company_id) as companies,
                COUNT(DISTINCT event_type) as event_types,
                AVG(data_quality_score) as avg_quality
            FROM holded_events.events;
            '''
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                data = lines[1].split('\t')
                return {
                    'total_events': int(data[0]),
                    'companies': int(data[1]),
                    'event_types': int(data[2]),
                    'avg_quality': float(data[3]) if data[3] != 'NULL' else 0
                }
        return None
    except:
        return None

def create_realistic_event():
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
            "id": f"demo_{int(time.time() * 1000)}",
            "amount": round(50.0 + (time.time() % 1000), 2)
        },
        "headers": {
            "type": event_types[int(time.time()) % len(event_types)],
            "targetSubscription": "wallet_events",
            "X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\NewContextStamp": {
                "userId": users[int(time.time()) % len(users)],
                "accountId": companies[int(time.time()) % len(companies)],
                "requestPlatform": "web"
            },
            "X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\TimestampStamp": {
                "dispatchedAt": int(time.time() * 1000)
            },
            "X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\MessageIdStamp": {
                "id": f"msg_{int(time.time() * 1000)}"
            }
        }
    }

def send_events_batch(count=3):
    """Send a batch of events"""
    events = [create_realistic_event() for _ in range(count)]
    
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
            print(f"✅ Sent {result.get('total_processed', 0)} events successfully")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def show_recent_events():
    """Show recent events from database"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'mysql', 'mysql', '-u', 'holded', '-pholded123', 
            '-e', '''
            SELECT 
                event_id,
                event_type,
                company_id,
                FROM_UNIXTIME(timestamp/1000) as event_time,
                data_quality_score
            FROM holded_events.events 
            ORDER BY timestamp DESC 
            LIMIT 5;
            '''
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("📋 Recent Events:")
            lines = result.stdout.strip().split('\n')
            for i, line in enumerate(lines):
                if i == 0:  # Header
                    print(f"   {line}")
                elif i <= 5:  # Data rows
                    print(f"   {line}")
        else:
            print("❌ Could not fetch recent events")
    except Exception as e:
        print(f"❌ Error fetching events: {e}")

def main():
    """Main demo function"""
    print_header("Holded Data Pipeline - Complete Demo")
    
    # Step 1: Check system health
    print_step("1️⃣", "Checking System Health")
    if not check_api_health():
        print("❌ System not ready. Please start the services first.")
        return
    
    # Step 2: Show initial state
    print_step("2️⃣", "Initial Database State")
    initial_stats = get_database_stats()
    if initial_stats:
        print(f"📊 Total Events: {initial_stats['total_events']}")
        print(f"🏢 Companies: {initial_stats['companies']}")
        print(f"📝 Event Types: {initial_stats['event_types']}")
        print(f"⭐ Avg Quality: {initial_stats['avg_quality']:.2f}")
    else:
        print("❌ Could not get database stats")
        return
    
    # Step 3: Send events
    print_step("3️⃣", "Sending Demo Events")
    print("🚀 Sending 3 batches of events...")
    
    for i in range(3):
        print(f"   Batch {i+1}/3...")
        if send_events_batch(2):  # 2 events per batch
            time.sleep(2)  # Wait between batches
        else:
            print("❌ Failed to send batch")
            return
    
    # Step 4: Wait for processing
    print_step("4️⃣", "Processing Events")
    print("⏳ Waiting for pipeline processing (10 seconds)...")
    time.sleep(10)
    
    # Step 5: Show final state
    print_step("5️⃣", "Final Database State")
    final_stats = get_database_stats()
    if final_stats:
        new_events = final_stats['total_events'] - initial_stats['total_events']
        print(f"📊 Total Events: {final_stats['total_events']} (+{new_events})")
        print(f"🏢 Companies: {final_stats['companies']}")
        print(f"📝 Event Types: {final_stats['event_types']}")
        print(f"⭐ Avg Quality: {final_stats['avg_quality']:.2f}")
        
        if new_events > 0:
            print("\n🎉 DEMO SUCCESSFUL!")
            print("✅ Events sent to API")
            print("✅ Events processed by Beam")
            print("✅ Events stored in MySQL")
            print("✅ Pipeline working end-to-end!")
            
            # Show recent events
            print_step("6️⃣", "Recent Events in Database")
            show_recent_events()
            
            print("\n📋 Next Steps for DBeaver:")
            print("1. Connect to localhost:3306")
            print("2. Database: holded_events")
            print("3. Username: holded, Password: holded123")
            print("4. Run queries from dbeaver_queries.sql")
        else:
            print("⚠️ No new events detected")
            print("💡 Check if Gold Sink is running")
    else:
        print("❌ Could not get final stats")

if __name__ == "__main__":
    main()
