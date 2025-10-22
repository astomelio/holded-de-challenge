#!/usr/bin/env python3
"""
Avro schema testing and validation
"""

import json
import time
from typing import Dict, Any, List

class AvroSchemaTester:
    """Test Avro schema validation"""
    
    def __init__(self):
        self.schema_file = "../schemas/event.avsc"
    
    def load_avro_schema(self) -> Dict[str, Any]:
        """Load Avro schema from file"""
        try:
            with open(self.schema_file, 'r') as f:
                schema = json.load(f)
            print(f"✅ Loaded Avro schema from {self.schema_file}")
            return schema
        except Exception as e:
            print(f"❌ Failed to load Avro schema: {e}")
            return {}
    
    def validate_event_against_schema(self, event: Dict[str, Any], schema: Dict[str, Any]) -> bool:
        """Validate event against Avro schema"""
        try:
            # Check required fields
            required_fields = [field['name'] for field in schema.get('fields', []) 
                             if 'default' not in field]
            
            missing_fields = [field for field in required_fields if field not in event]
            
            if missing_fields:
                print(f"❌ Missing required fields: {missing_fields}")
                return False
            
            # Check field types (basic validation)
            for field in schema.get('fields', []):
                field_name = field['name']
                field_type = field['type']
                
                if field_name in event:
                    value = event[field_name]
                    
                    # Basic type checking
                    if field_type == 'string' and not isinstance(value, str):
                        print(f"❌ Field '{field_name}' should be string, got {type(value)}")
                        return False
                    elif field_type == 'long' and not isinstance(value, int):
                        print(f"❌ Field '{field_name}' should be long, got {type(value)}")
                        return False
                    elif field_type == ['null', 'string'] and value is not None and not isinstance(value, str):
                        print(f"❌ Field '{field_name}' should be null or string, got {type(value)}")
                        return False
            
            print(f"✅ Event validation passed")
            return True
            
        except Exception as e:
            print(f"❌ Event validation error: {e}")
            return False
    
    def test_sample_events(self) -> bool:
        """Test sample events against schema"""
        schema = self.load_avro_schema()
        if not schema:
            return False
        
        # Sample events to test
        test_events = [
            {
                "event_id": "test-001",
                "event_type": "USER_LOGIN",
                "user_id": "user123",
                "company_id": "company456",
                "timestamp": int(time.time() * 1000),
                "metadata": '{"source": "test"}',
                "ip_address": "192.168.1.1",
                "user_agent": "Mozilla/5.0",
                "session_id": "session-123"
            },
            {
                "event_id": "test-002",
                "event_type": "INVOICE_CREATED",
                "user_id": "user789",
                "company_id": "company456",
                "timestamp": int(time.time() * 1000),
                "metadata": '{"amount": 150.50}',
                "ip_address": "192.168.1.2",
                "user_agent": "Mozilla/5.0",
                "session_id": "session-456"
            },
            # Invalid event (missing required fields)
            {
                "event_id": "test-003",
                "event_type": "USER_LOGOUT",
                # Missing company_id and timestamp
                "user_id": "user999"
            }
        ]
        
        print(f"🧪 Testing {len(test_events)} sample events...")
        
        valid_count = 0
        for i, event in enumerate(test_events, 1):
            print(f"\n📋 Testing event {i}: {event.get('event_type', 'UNKNOWN')}")
            
            is_valid = self.validate_event_against_schema(event, schema)
            if is_valid:
                valid_count += 1
                print(f"✅ Event {i} is valid")
            else:
                print(f"❌ Event {i} is invalid")
        
        print(f"\n📊 Results: {valid_count}/{len(test_events)} events valid")
        return valid_count > 0

def run_avro_tests():
    """Run Avro schema tests"""
    print("🧪 Avro Schema Tests")
    print("=" * 30)
    
    tester = AvroSchemaTester()
    
    # Test 1: Load schema
    print("1️⃣ Loading Avro schema...")
    schema = tester.load_avro_schema()
    if not schema:
        print("❌ Cannot load schema - skipping tests")
        return False
    
    # Test 2: Validate sample events
    print("\n2️⃣ Testing sample events...")
    success = tester.test_sample_events()
    
    if success:
        print("\n🎉 Avro schema tests completed!")
    else:
        print("\n💥 Avro schema tests failed!")
    
    return success

if __name__ == "__main__":
    run_avro_tests()
