#!/usr/bin/env python3
"""
Test script to send Holded events to the API
"""

import json
import httpx
import time
import random
import os

def send_holded_events():
    """Send Holded events to the API"""
    
    # Read events from file
    script_dir = os.path.dirname(__file__)
    events_file = os.path.join(script_dir, "events.json")
    
    try:
        with open(events_file, 'r', encoding='utf-8') as file:
            events = []
            count = 0
            
            for line in file:
                try:
                    event = json.loads(line.strip())
                    events.append(event)
                    count += 1
                    
                    # Send in batches of 3-5 events
                    if len(events) >= random.randint(3, 5):
                        print(f"📤 Sending batch of {len(events)} events...")
                        
                        # Send to API
                        with httpx.Client() as client:
                            response = client.post(
                                'http://localhost:8000/collect',
                                json=events,
                                headers={'Content-Type': 'application/json'},
                                timeout=10
                            )
                        
                        if response.status_code == 200:
                            result = response.json()
                            print(f"✅ Batch sent successfully: {result.get('total_processed', 0)} events")
                        else:
                            print(f"❌ Error sending batch: {response.status_code} - {response.text}")
                        
                        events = []
                        time.sleep(1)  # Pause between batches
                        
                        # Limit to 20 events for testing
                        if count >= 20:
                            break
                            
                except json.JSONDecodeError as e:
                    print(f"⚠️ Invalid JSON: {e}")
                    continue
                except Exception as e:
                    print(f"❌ Error processing event: {e}")
                    continue
            
            # Send remaining events
            if events:
                print(f"📤 Sending final batch of {len(events)} events...")
                with httpx.Client() as client:
                    response = client.post(
                        'http://localhost:8000/collect',
                        json=events,
                        headers={'Content-Type': 'application/json'},
                        timeout=10
                    )
                
                if response.status_code == 200:
                    result = response.json()
                    print(f"✅ Final batch sent: {result.get('total_processed', 0)} events")
                else:
                    print(f"❌ Error sending final batch: {response.status_code} - {response.text}")
            
            print(f"🎉 Total events processed: {count}")
            
    except FileNotFoundError:
        print(f"❌ Events file not found: {events_file}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🚀 Holded Events Test Sender")
    print("=" * 50)
    
    # Check that the API is running
    try:
        with httpx.Client() as client:
            response = client.get("http://localhost:8000/health", timeout=5)
            if response.status_code == 200:
                print("✅ API is running and ready")
            else:
                print("⚠️ API responded but may not be ready")
    except Exception as e:
        print(f"❌ API not available: {e}")
        print("💡 Make sure the API is running on http://localhost:8000")
        exit(1)
    
    # Send events
    send_holded_events()
