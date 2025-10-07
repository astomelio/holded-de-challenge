import json
import logging
from datetime import datetime
from typing import Dict, Any, List
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromKafka, WriteToKafka
from apache_beam.transforms import DoFn, ParDo
from apache_beam.pvalue import TaggedOutput

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventValidator(DoFn):
    """Validates and filters events"""
    
    def process(self, element):
        try:
            # Parse the Kafka message
            if isinstance(element, tuple):
                key, value = element
                event = json.loads(value.decode('utf-8'))
            else:
                event = json.loads(element.decode('utf-8'))
            
            # Validate required fields
            required_fields = ['event_id', 'event_type', 'company_id', 'timestamp']
            if not all(field in event for field in required_fields):
                logger.warning(f"Invalid event missing required fields: {event}")
                yield TaggedOutput('invalid', event)
                return
            
            # Validate event type
            valid_event_types = [
                'USER_LOGIN', 'USER_LOGOUT', 'INVOICE_CREATED', 'INVOICE_UPDATED',
                'INVOICE_DELETED', 'PAYMENT_RECEIVED', 'CUSTOMER_CREATED',
                'CUSTOMER_UPDATED', 'PRODUCT_CREATED', 'PRODUCT_UPDATED'
            ]
            
            if event['event_type'] not in valid_event_types:
                logger.warning(f"Invalid event type: {event['event_type']}")
                yield TaggedOutput('invalid', event)
                return
            
            # Add processing timestamp
            event['processed_at'] = int(datetime.now().timestamp() * 1000)
            
            yield TaggedOutput('valid', event)
            
        except Exception as e:
            logger.error(f"Error processing event: {str(e)}")
            yield TaggedOutput('invalid', element)

class EventEnricher(DoFn):
    """Enriches events with additional data"""
    
    def process(self, element):
        try:
            event = element
            
            # Add data quality score
            event['data_quality_score'] = self._calculate_quality_score(event)
            
            # Add partition key for downstream processing
            event['partition_key'] = f"{event['company_id']}_{event['event_type']}"
            
            # Add business context
            event['business_context'] = self._get_business_context(event)
            
            yield event
            
        except Exception as e:
            logger.error(f"Error enriching event: {str(e)}")
            yield element
    
    def _calculate_quality_score(self, event: Dict[str, Any]) -> float:
        """Calculate data quality score based on event completeness"""
        score = 1.0
        
        # Deduct points for missing optional fields
        optional_fields = ['user_id', 'ip_address', 'user_agent', 'session_id']
        for field in optional_fields:
            if field not in event or event[field] is None:
                score -= 0.1
        
        # Deduct points for empty metadata
        if not event.get('metadata') or len(event['metadata']) == 0:
            score -= 0.1
        
        return max(0.0, score)
    
    def _get_business_context(self, event: Dict[str, Any]) -> str:
        """Determine business context based on event type"""
        event_type = event['event_type']
        
        if event_type.startswith('USER_'):
            return 'user_activity'
        elif event_type.startswith('INVOICE_'):
            return 'billing'
        elif event_type.startswith('PAYMENT_'):
            return 'payments'
        elif event_type.startswith('CUSTOMER_'):
            return 'customer_management'
        elif event_type.startswith('PRODUCT_'):
            return 'product_management'
        else:
            return 'other'

class EventDeduplicator(DoFn):
    """Removes duplicate events based on event_id"""
    
    def __init__(self):
        self.seen_events = set()
    
    def process(self, element):
        event = element
        event_id = event['event_id']
        
        if event_id not in self.seen_events:
            self.seen_events.add(event_id)
            yield event
        else:
            logger.info(f"Duplicate event detected: {event_id}")

class EventAggregator(DoFn):
    """Aggregates events by company and event type"""
    
    def __init__(self):
        self.aggregated_data = {}
    
    def process(self, element):
        event = element
        company_id = event['company_id']
        event_type = event['event_type']
        
        key = f"{company_id}_{event_type}"
        
        if key not in self.aggregated_data:
            self.aggregated_data[key] = {
                'company_id': company_id,
                'event_type': event_type,
                'count': 0,
                'first_seen': event['timestamp'],
                'last_seen': event['timestamp']
            }
        
        self.aggregated_data[key]['count'] += 1
        self.aggregated_data[key]['last_seen'] = event['timestamp']
        
        # Yield individual event for downstream processing
        yield event

def run_pipeline():
    """Main pipeline function"""
    
    # Pipeline options
    options = PipelineOptions([
        '--runner=DirectRunner',  # Use DirectRunner for local development
        '--streaming',  # Enable streaming mode
    ])
    
    with beam.Pipeline(options=options) as pipeline:
        # Read from Kafka
        events = (
            pipeline
            | 'ReadFromKafka' >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'holded-beam-consumer',
                    'auto.offset.reset': 'earliest'
                },
                topics=['events.raw']
            )
        )
        
        # Validate events
        validation_results = (
            events
            | 'ValidateEvents' >> ParDo(EventValidator()).with_outputs('valid', 'invalid')
        )
        
        valid_events = validation_results.valid
        invalid_events = validation_results.invalid
        
        # Process valid events
        processed_events = (
            valid_events
            | 'RemoveDuplicates' >> ParDo(EventDeduplicator())
            | 'EnrichEvents' >> ParDo(EventEnricher())
            | 'AggregateEvents' >> ParDo(EventAggregator())
        )
        
        # Write valid events to silver layer topic
        _ = (
            processed_events
            | 'WriteToSilver' >> WriteToKafka(
                producer_config={
                    'bootstrap.servers': 'localhost:9092'
                },
                topic='events.silver'
            )
        )
        
        # Write invalid events to dead letter queue
        _ = (
            invalid_events
            | 'WriteToDeadLetter' >> WriteToKafka(
                producer_config={
                    'bootstrap.servers': 'localhost:9092'
                },
                topic='events.dead.letter'
            )
        )

if __name__ == '__main__':
    run_pipeline()
