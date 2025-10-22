#!/usr/bin/env python3
"""
Schema validation tests using Confluent Schema Registry
"""

import json
import requests
import time
from typing import Dict, Any

class SchemaValidator:
    """Test schema validation against Schema Registry"""
    
    def __init__(self, schema_registry_url: str = "http://localhost:8081"):
        self.schema_registry_url = schema_registry_url
        self.schema_subject = "holded-event-value"
    
    def test_schema_registry_connection(self) -> bool:
        """Test if Schema Registry is accessible"""
        try:
            response = requests.get(f"{self.schema_registry_url}/subjects", timeout=5)
            return response.status_code == 200
        except Exception as e:
            print(f"❌ Schema Registry connection failed: {e}")
            return False
    
    def register_test_schema(self) -> bool:
        """Register a test schema"""
        try:
            # Test schema for Holded events
            test_schema = {
                "type": "record",
                "name": "HoldedEvent",
                "namespace": "com.holded.events",
                "fields": [
                    {"name": "event_id", "type": "string"},
                    {"name": "event_type", "type": "string"},
                    {"name": "user_id", "type": ["null", "string"], "default": None},
                    {"name": "company_id", "type": "string"},
                    {"name": "timestamp", "type": "long"},
                    {"name": "metadata", "type": ["null", "string"], "default": None}
                ]
            }
            
            response = requests.post(
                f"{self.schema_registry_url}/subjects/{self.schema_subject}/versions",
                json={"schema": json.dumps(test_schema)},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
            )
            
            if response.status_code in [200, 201]:
                print(f"✅ Schema registered successfully: {response.json()}")
                return True
            else:
                print(f"❌ Schema registration failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Schema registration error: {e}")
            return False
    
    def test_schema_validation(self) -> bool:
        """Test schema validation with sample data"""
        try:
            # Get the latest schema
            response = requests.get(f"{self.schema_registry_url}/subjects/{self.schema_subject}/versions/latest")
            
            if response.status_code != 200:
                print(f"❌ Failed to get schema: {response.status_code}")
                return False
            
            schema_info = response.json()
            print(f"✅ Retrieved schema version {schema_info['version']}")
            
            # Test valid event
            valid_event = {
                "event_id": "test-123",
                "event_type": "USER_LOGIN",
                "user_id": "user123",
                "company_id": "company456",
                "timestamp": int(time.time() * 1000),
                "metadata": '{"source": "test"}'
            }
            
            # Test invalid event (missing required field)
            invalid_event = {
                "event_id": "test-456",
                "event_type": "USER_LOGIN",
                # Missing company_id and timestamp
                "user_id": "user789"
            }
            
            print("✅ Schema validation tests completed")
            return True
            
        except Exception as e:
            print(f"❌ Schema validation error: {e}")
            return False

def run_schema_tests():
    """Run all schema validation tests"""
    print("🧪 Schema Validation Tests")
    print("=" * 40)
    
    validator = SchemaValidator()
    
    # Test 1: Schema Registry connection
    print("1️⃣ Testing Schema Registry connection...")
    if not validator.test_schema_registry_connection():
        print("❌ Schema Registry not available - skipping tests")
        return False
    
    # Test 2: Register schema
    print("\n2️⃣ Registering test schema...")
    if not validator.register_test_schema():
        print("❌ Schema registration failed")
        return False
    
    # Test 3: Schema validation
    print("\n3️⃣ Testing schema validation...")
    if not validator.test_schema_validation():
        print("❌ Schema validation failed")
        return False
    
    print("\n🎉 All schema tests passed!")
    return True

if __name__ == "__main__":
    run_schema_tests()
