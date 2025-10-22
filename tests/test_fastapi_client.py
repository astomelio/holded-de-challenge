#!/usr/bin/env python3
"""
FastAPI TestClient - Modern way to test FastAPI apps
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'api'))

from fastapi.testclient import TestClient
from main_minio import app
import json
import time

class TestFastAPIClient:
    """Test using FastAPI TestClient"""
    
    def __init__(self):
        self.client = TestClient(app)
    
    def test_health_endpoint(self):
        """Test health endpoint using TestClient"""
        print("🧪 Testing health endpoint...")
        
        response = self.client.get("/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert "minio_connected" in data
        
        print("✅ Health endpoint working")
        return True
    
    def test_collect_endpoint(self):
        """Test collect endpoint using TestClient"""
        print("🧪 Testing collect endpoint...")
        
        test_events = [
            {
                "event_id": f"test-{int(time.time())}-001",
                "event_type": "USER_LOGIN",
                "user_id": "user123",
                "company_id": "company456",
                "timestamp": int(time.time() * 1000),
                "metadata": {"source": "testclient"}
            }
        ]
        
        response = self.client.post(
            "/collect",
            json=test_events,
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200
        
        data = response.json()
        assert "results" in data
        assert "total_processed" in data
        assert data["total_processed"] == 1
        
        print("✅ Collect endpoint working")
        return True
    
    def test_invalid_json(self):
        """Test with invalid JSON"""
        print("🧪 Testing invalid JSON...")
        
        response = self.client.post(
            "/collect",
            content="invalid json",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 400
        print("✅ Invalid JSON handling working")
        return True
    
    def run_all_tests(self):
        """Run all tests"""
        print("🚀 FastAPI TestClient Tests")
        print("=" * 40)
        
        try:
            self.test_health_endpoint()
            self.test_collect_endpoint()
            self.test_invalid_json()
            
            print("\n🎉 All FastAPI tests passed!")
            return True
            
        except Exception as e:
            print(f"\n💥 Test failed: {e}")
            return False

if __name__ == "__main__":
    tester = TestFastAPIClient()
    tester.run_all_tests()
