#!/usr/bin/env python3
"""
Apache Beam DirectRunner Pipeline for Holded Events
Saves to MinIO and PostgreSQL
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import requests
from minio import Minio
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventValidator(beam.DoFn):
    """Validate events against Schema Registry"""
    
    def __init__(self, schema_registry_url):
        self.schema_registry_url = schema_registry_url
    
    def process(self, element):
        """Validate event against schema"""
        try:
            # Get schema from Schema Registry
            response = requests.get(f"{self.schema_registry_url}/subjects/events-value/versions/latest")
            if response.status_code == 200:
                schema = response.json()
                # Basic validation (simplified)
                if 'event_id' in element and 'event_type' in element:
                    yield element
                else:
                    logger.warning(f"Invalid event: {element}")
            else:
                logger.error(f"Schema Registry error: {response.status_code}")
        except Exception as e:
            logger.error(f"Validation error: {e}")

class EventEnricher(beam.DoFn):
    """Enrich events with metadata"""
    
    def process(self, element):
        """Add metadata to events"""
        try:
            # Add processing metadata
            enriched_event = {
                **element,
                'processed_at': int(datetime.now().timestamp() * 1000),
                'pipeline_version': '1.0.0',
                'data_quality_score': self._calculate_quality_score(element),
                'business_context': self._determine_business_context(element),
                'partition_key': element.get('company_id', 'unknown')
            }
            
            yield enriched_event
        except Exception as e:
            logger.error(f"Enrichment error: {e}")
    
    def _calculate_quality_score(self, event):
        """Calculate data quality score"""
        score = 0.5  # Base score
        
        # Check required fields
        if event.get('event_id'):
            score += 0.2
        if event.get('event_type'):
            score += 0.2
        if event.get('company_id'):
            score += 0.1
        
        return min(score, 1.0)
    
    def _determine_business_context(self, event):
        """Determine business context"""
        event_type = event.get('event_type', '')
        
        if 'Transaction' in event_type:
            return 'financial'
        elif 'Account' in event_type:
            return 'account_management'
        elif 'User' in event_type:
            return 'user_management'
        else:
            return 'other'

class MinIOSink(beam.DoFn):
    """Save enriched events to MinIO"""
    
    def __init__(self, minio_endpoint, access_key, secret_key):
        self.minio_endpoint = minio_endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.client = None
    
    def setup(self):
        """Initialize MinIO client"""
        self.client = Minio(
            self.minio_endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False
        )
        
        # Create bucket if not exists
        if not self.client.bucket_exists("holded-events"):
            self.client.make_bucket("holded-events")
    
    def process(self, element):
        """Save event to MinIO"""
        try:
            # Create object key
            event_id = element.get('event_id', 'unknown')
            timestamp = datetime.now().strftime('%Y/%m/%d')
            object_key = f"events/{timestamp}/{event_id}.json"
            
            # Convert to JSON
            json_data = json.dumps(element, indent=2)
            
            # Upload to MinIO
            self.client.put_object(
                "holded-events",
                object_key,
                json_data.encode('utf-8'),
                length=len(json_data),
                content_type='application/json'
            )
            
            logger.info(f"Event {event_id} saved to MinIO: {object_key}")
            yield element
            
        except Exception as e:
            logger.error(f"MinIO error: {e}")

class PostgreSQLSink(beam.DoFn):
    """Save business data to PostgreSQL"""
    
    def __init__(self, postgres_host, postgres_port, postgres_user, postgres_password, postgres_db):
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_db = postgres_db
    
    def setup(self):
        """Initialize PostgreSQL connection"""
        self.connection = psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            user=self.postgres_user,
            password=self.postgres_password,
            database=self.postgres_db
        )
        self.cursor = self.connection.cursor()
    
    def process(self, element):
        """Save business data to PostgreSQL"""
        try:
            # Insert into business events table
            insert_query = """
            INSERT INTO business_events (
                event_id, event_type, company_id, user_id, timestamp,
                business_context, data_quality_score, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                business_context = EXCLUDED.business_context,
                data_quality_score = EXCLUDED.data_quality_score,
                metadata = EXCLUDED.metadata,
                updated_at = CURRENT_TIMESTAMP
            """
            
            self.cursor.execute(insert_query, (
                element['event_id'],
                element['event_type'],
                element.get('company_id'),
                element.get('user_id'),
                element['timestamp'],
                element.get('business_context'),
                element.get('data_quality_score'),
                json.dumps(element.get('metadata', {}))
            ))
            
            self.connection.commit()
            logger.info(f"Event {element['event_id']} saved to PostgreSQL")
            yield element
            
        except Exception as e:
            logger.error(f"PostgreSQL error: {e}")
    
    def teardown(self):
        """Close PostgreSQL connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

def run_pipeline(use_kafka=False):
    """Run the Apache Beam pipeline"""
    
    # Pipeline options
    options = PipelineOptions(
        runner='DirectRunner',
        direct_num_workers=1,
        direct_running_mode='multi_threading'
    )
    
    with beam.Pipeline(options=options) as pipeline:
        if use_kafka:
            # Read events from Kafka
            from apache_beam.io.kafka import ReadFromKafka
            events = (
                pipeline
                | 'ReadFromKafka' >> ReadFromKafka(
                    consumer_config={
                        'bootstrap.servers': 'localhost:9092',
                        'group.id': 'holded-beam-consumer',
                        'auto.offset.reset': 'earliest'
                    },
                    topics=['events.raw'],
                    key_deserializer='org.apache.kafka.common.serialization.StringDeserializer',
                    value_deserializer='org.apache.kafka.common.serialization.StringDeserializer'
                )
                | 'ParseJSON' >> beam.Map(lambda kv: json.loads(kv[1]))
            )
        else:
            # Read events from file (simplified for demo)
            events = (
                pipeline
                | 'ReadEvents' >> ReadFromText('events.json')
                | 'ParseJSON' >> beam.Map(json.loads)
            )
        
        # Validate events
        validated_events = (
            events
            | 'ValidateEvents' >> beam.ParDo(EventValidator('http://localhost:8081'))
        )
        
        # Enrich events
        enriched_events = (
            validated_events
            | 'EnrichEvents' >> beam.ParDo(EventEnricher())
        )
        
        # Save to MinIO
        minio_events = (
            enriched_events
            | 'SaveToMinIO' >> beam.ParDo(MinIOSink(
                'localhost:9000',
                'minioadmin',
                'minioadmin123'
            ))
        )
        
        # Save to PostgreSQL
        postgres_events = (
            minio_events
            | 'SaveToPostgreSQL' >> beam.ParDo(PostgreSQLSink(
                'localhost',
                5432,
                'holded',
                'holded123',
                'holded_business'
            ))
        )
        
        # Write results to file (for debugging)
        (
            postgres_events
            | 'WriteResults' >> WriteToText('output/processed_events', file_name_suffix='.json')
        )

if __name__ == '__main__':
    import sys
    use_kafka = '--kafka' in sys.argv
    run_pipeline(use_kafka=use_kafka)
