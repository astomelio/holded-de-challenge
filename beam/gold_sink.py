import json
import logging
from datetime import datetime
from typing import Dict, Any
import pymysql
from kafka import KafkaConsumer
import threading
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoldLayerSink:
    """Gold layer sink that writes processed events to MySQL"""
    
    def __init__(self):
        # Kafka consumer for silver layer
        self.kafka_consumer = KafkaConsumer(
            'events.silver',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='gold-layer-sink'
        )
        
        # MySQL connection
        self.mysql_connection = pymysql.connect(
            host='localhost',
            port=3306,
            user='holded',
            password='holded123',
            database='holded_events',
            autocommit=True
        )
        
        self.cursor = self.mysql_connection.cursor()
        self.processed_count = 0
    
    def process_events(self):
        """Process events from silver layer and write to MySQL"""
        logger.info("🥇 Starting Gold Layer Sink...")
        
        try:
            for message in self.kafka_consumer:
                event = message.value
                logger.info(f"📥 Processing silver event: {event['event_id']}")
                
                # Write to main events table
                self._write_to_events_table(event)
                
                # Update summary table
                self._update_summary_table(event)
                
                self.processed_count += 1
                logger.info(f"✅ Event {event['event_id']} written to Gold layer")
                
        except KeyboardInterrupt:
            logger.info("🛑 Gold layer sink stopped by user")
        except Exception as e:
            logger.error(f"❌ Error in gold layer sink: {str(e)}")
        finally:
            self.kafka_consumer.close()
            self.mysql_connection.close()
    
    def _write_to_events_table(self, event: Dict[str, Any]):
        """Write event to main events table"""
        try:
            # Keep timestamps as BIGINT (milliseconds since epoch)
            event_timestamp = event['timestamp']
            processed_timestamp = event['processed_at']
            
            insert_query = """
            INSERT INTO events (
                event_id, event_type, user_id, company_id, timestamp, processed_at,
                ip_address, user_agent, session_id, data_quality_score, 
                partition_key, business_context, metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                processed_at = VALUES(processed_at),
                data_quality_score = VALUES(data_quality_score),
                business_context = VALUES(business_context)
            """
            
            values = (
                event['event_id'],
                event['event_type'],
                event.get('user_id'),
                event['company_id'],
                event_timestamp,
                processed_timestamp,
                event.get('ip_address'),
                event.get('user_agent'),
                event.get('session_id'),
                event.get('data_quality_score'),
                event.get('partition_key'),
                event.get('business_context'),
                json.dumps(event.get('metadata', {}))
            )
            
            self.cursor.execute(insert_query, values)
            logger.info(f"📝 Event {event['event_id']} written to events table")
            
        except Exception as e:
            logger.error(f"❌ Error writing to events table: {str(e)}")
            raise
    
    def _update_summary_table(self, event: Dict[str, Any]):
        """Update events summary table"""
        try:
            # Get current hour for aggregation
            current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
            
            # Check if summary exists
            check_query = """
            SELECT id, event_count, avg_data_quality_score 
            FROM events_summary 
            WHERE company_id = %s AND event_type = %s AND business_context = %s 
            AND date_hour = %s
            """
            
            self.cursor.execute(check_query, (
                event['company_id'],
                event['event_type'],
                event.get('business_context', 'other'),
                current_hour
            ))
            
            result = self.cursor.fetchone()
            
            if result:
                # Update existing summary
                summary_id, current_count, current_avg = result
                new_count = current_count + 1
                new_avg = ((current_avg * current_count) + event.get('data_quality_score', 0)) / new_count
                
                update_query = """
                UPDATE events_summary 
                SET event_count = %s, avg_data_quality_score = %s, 
                    last_event_timestamp = %s, updated_at = NOW()
                WHERE id = %s
                """
                
                self.cursor.execute(update_query, (
                    new_count, new_avg, event['timestamp'], summary_id
                ))
                
            else:
                # Insert new summary
                insert_summary_query = """
                INSERT INTO events_summary (
                    company_id, event_type, business_context, event_count,
                    first_event_timestamp, last_event_timestamp, 
                    avg_data_quality_score, date_hour
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_summary_query, (
                    event['company_id'],
                    event['event_type'],
                    event.get('business_context', 'other'),
                    1,
                    event['timestamp'],
                    event['timestamp'],
                    event.get('data_quality_score', 0),
                    current_hour
                ))
            
            logger.info(f"📊 Summary updated for {event['company_id']} - {event['event_type']}")
            
        except Exception as e:
            logger.error(f"❌ Error updating summary table: {str(e)}")
            # Don't raise here, summary is not critical
    
    def get_stats(self):
        """Get processing statistics"""
        try:
            # Get total events count
            self.cursor.execute("SELECT COUNT(*) FROM events")
            total_events = self.cursor.fetchone()[0]
            
            # Get events by business context
            self.cursor.execute("""
                SELECT business_context, COUNT(*) as count 
                FROM events 
                GROUP BY business_context
            """)
            context_stats = dict(self.cursor.fetchall())
            
            return {
                "total_events": total_events,
                "processed_by_sink": self.processed_count,
                "context_distribution": context_stats
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting stats: {str(e)}")
            return {"error": str(e)}

def main():
    """Main function to run the gold layer sink"""
    logger.info("🥇 Starting Holded Gold Layer Sink")
    logger.info("=" * 50)
    
    sink = GoldLayerSink()
    
    try:
        sink.process_events()
    except Exception as e:
        logger.error(f"❌ Gold layer sink failed: {str(e)}")
        raise

if __name__ == '__main__':
    main()
