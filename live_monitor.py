#!/usr/bin/env python3
"""
Live Monitor for Holded Data Pipeline
Shows real-time processing of events
"""

import time
import subprocess
import json
from datetime import datetime

def get_database_count():
    """Get current event count from database"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'mysql', 'mysql', '-u', 'holded', '-pholded123', 
            '-e', 'SELECT COUNT(*) as count FROM holded_events.events;'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                return int(lines[1].strip())
        return 0
    except:
        return 0

def get_recent_events():
    """Get recent events from database"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'mysql', 'mysql', '-u', 'holded', '-pholded123', 
            '-e', '''
            SELECT 
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
            return result.stdout
        return "No data"
    except:
        return "Error"

def monitor_pipeline():
    """Monitor the pipeline in real-time"""
    print("🔍 Holded Data Pipeline - Live Monitor")
    print("=" * 60)
    print("Press Ctrl+C to stop monitoring")
    print()
    
    previous_count = get_database_count()
    print(f"📊 Initial event count: {previous_count}")
    print()
    
    try:
        while True:
            current_count = get_database_count()
            new_events = current_count - previous_count
            
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            if new_events > 0:
                print(f"🆕 [{timestamp}] +{new_events} new events (Total: {current_count})")
                
                # Show recent events
                recent = get_recent_events()
                if "No data" not in recent and "Error" not in recent:
                    print("📋 Recent events:")
                    lines = recent.strip().split('\n')
                    for line in lines[1:6]:  # Skip header, show 5 events
                        if line.strip():
                            print(f"   {line}")
                print()
                
                previous_count = current_count
            else:
                print(f"⏳ [{timestamp}] Monitoring... (Total: {current_count})")
            
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped")
        print(f"📊 Final event count: {get_database_count()}")

if __name__ == "__main__":
    monitor_pipeline()
