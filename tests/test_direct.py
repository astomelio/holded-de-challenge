#!/usr/bin/env python3
"""
Direct test - no API, just test the components
"""

import json
import time
from minio import Minio
import psycopg2

def test_minio():
    """Test MinIO directly"""
    print("🧪 Testing MinIO directly...")
    
    try:
        # Connect to MinIO
        minio_client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin123",
            secure=False
        )
        
        # Test bucket
        bucket_name = "holded-events"
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"✅ Created bucket: {bucket_name}")
        else:
            print(f"✅ Bucket exists: {bucket_name}")
        
        # Test upload
        test_data = {"test": "data", "timestamp": int(time.time())}
        object_name = f"test-{int(time.time())}.json"
        
        from io import BytesIO
        data = BytesIO(json.dumps(test_data).encode('utf-8'))
        
        minio_client.put_object(
            bucket_name,
            object_name,
            data,
            length=len(json.dumps(test_data)),
            content_type='application/json'
        )
        
        print(f"✅ Uploaded test object: {object_name}")
        return True
        
    except Exception as e:
        print(f"❌ MinIO test failed: {e}")
        return False

def test_postgres():
    """Test PostgreSQL directly"""
    print("\n🧪 Testing PostgreSQL directly...")
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="holded_business",
            user="holded",
            password="holded123"
        )
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print(f"✅ PostgreSQL connection successful: {result}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Direct Component Test")
    print("=" * 30)
    
    minio_ok = test_minio()
    postgres_ok = test_postgres()
    
    if minio_ok and postgres_ok:
        print("\n🎉 All components working!")
    else:
        print("\n💥 Some components failed!")
