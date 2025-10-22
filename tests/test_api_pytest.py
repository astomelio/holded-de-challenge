#!/usr/bin/env python3
"""
Modern API testing with pytest and httpx
"""

import pytest
import httpx
import json
import time
from typing import Dict, Any

class TestHoldedAPI:
    """Test suite for Holded API using pytest"""
    
    @pytest.fixture
    def api_base_url(self):
        """API base URL fixture"""
        return "http://localhost:8000"
    
    @pytest.fixture
    def sample_events(self):
        """Sample events fixture"""
        return [
            {
                "event_id": f"test-{int(time.time())}-001",
                "event_type": "USER_LOGIN",
                "user_id": "user123",
                "company_id": "company456",
                "timestamp": int(time.time() * 1000),
                "metadata": {"source": "pytest"}
            },
            {
                "event_id": f"test-{int(time.time())}-002",
                "event_type": "INVOICE_CREATED",
                "user_id": "user789",
                "company_id": "company456",
                "timestamp": int(time.time() * 1000),
                "amount": 150.50,
                "metadata": {"source": "pytest"}
            }
        ]
    
    def test_api_health(self, api_base_url):
        """Test API health endpoint"""
        with httpx.Client() as client:
            response = client.get(f"{api_base_url}/health")
            assert response.status_code == 200
            
            data = response.json()
            assert data["status"] == "healthy"
            assert data["minio_connected"] is True
    
    def test_collect_events(self, api_base_url, sample_events):
        """Test event collection endpoint"""
        with httpx.Client() as client:
            response = client.post(
                f"{api_base_url}/collect",
                json=sample_events,
                headers={"Content-Type": "application/json"}
            )
            
            assert response.status_code == 200
            
            data = response.json()
            assert "results" in data
            assert "total_processed" in data
            assert data["total_processed"] == len(sample_events)
            
            for result in data["results"]:
                assert result["status"] == "accepted"
                assert "event_id" in result
                assert "timestamp" in result
    
    def test_invalid_json(self, api_base_url):
        """Test API with invalid JSON"""
        with httpx.Client() as client:
            response = client.post(
                f"{api_base_url}/collect",
                content="invalid json",
                headers={"Content-Type": "application/json"}
            )
            
            assert response.status_code == 400
    
    def test_empty_events(self, api_base_url):
        """Test API with empty events list"""
        with httpx.Client() as client:
            response = client.post(
                f"{api_base_url}/collect",
                json=[],
                headers={"Content-Type": "application/json"}
            )
            
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_processed"] == 0
            assert data["results"] == []

# Run tests if executed directly
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
