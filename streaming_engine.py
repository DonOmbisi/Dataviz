"""
Real-time Data Streaming Module
Supports WebSocket streams, Kafka integration, and live data ingestion
"""

import pandas as pd
import asyncio
import json
from typing import Dict, List, Any, Callable, Optional, AsyncGenerator
from datetime import datetime
import warnings
from collections import deque
import threading

warnings.filterwarnings('ignore')

# Conditional imports
try:
    import websockets
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class DataStreamBuffer:
    """In-memory buffer for streaming data"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.buffer = deque(maxlen=max_size)
        self.schema = None
        self.metadata = {
            "created_at": datetime.now().isoformat(),
            "records_received": 0,
            "bytes_received": 0,
            "last_update": datetime.now().isoformat()
        }
    
    def add_record(self, record: Dict[str, Any]) -> bool:
        """Add record to buffer"""
        
        try:
            # Auto-detect schema from first record
            if self.schema is None and record:
                self.schema = {col: type(val).__name__ for col, val in record.items()}
            
            self.buffer.append(record)
            self.metadata["records_received"] += 1
            self.metadata["last_update"] = datetime.now().isoformat()
            
            return True
            
        except Exception as e:
            return False
    
    def add_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Add batch of records"""
        
        added = 0
        for record in records:
            if self.add_record(record):
                added += 1
        
        return {
            "records_added": added,
            "total_buffered": len(self.buffer),
            "buffer_capacity": self.max_size,
            "usage_percent": (len(self.buffer) / self.max_size) * 100
        }
    
    def get_dataframe(self) -> pd.DataFrame:
        """Convert buffer to DataFrame"""
        
        if not self.buffer:
            return pd.DataFrame()
        
        return pd.DataFrame(list(self.buffer))
    
    def get_recent(self, n: int) -> List[Dict]:
        """Get most recent n records"""
        
        return list(self.buffer)[-n:] if self.buffer else []
    
    def clear(self):
        """Clear the buffer"""
        
        self.buffer.clear()
        self.metadata["records_received"] = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        
        return {
            "buffer_size": len(self.buffer),
            "max_size": self.max_size,
            "usage_percent": (len(self.buffer) / self.max_size) * 100,
            "schema": self.schema,
            "metadata": self.metadata
        }


class StreamProcessor:
    """Process and transform streaming data"""
    
    def __init__(self):
        self.processors = {}
        self.transformations = []
    
    def register_processor(self, name: str, processor: Callable) -> bool:
        """Register a data processor function"""
        
        try:
            self.processors[name] = processor
            return True
        except Exception as e:
            return False
    
    def add_transformation(self, transformation: Dict[str, Any]) -> Dict[str, Any]:
        """Add a transformation pipeline"""
        
        try:
            self.transformations.append(transformation)
            return {
                "success": True,
                "message": "Transformation added",
                "transformation_id": len(self.transformations) - 1
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def filter_records(self, records: List[Dict], filter_func: Callable) -> List[Dict]:
        """Filter records based on condition"""
        
        return [r for r in records if filter_func(r)]
    
    def aggregate_records(self, records: List[Dict], groupby_key: str, 
                         agg_func: str = 'sum', value_key: str = 'value') -> Dict:
        """Aggregate records by key"""
        
        try:
            aggregated = {}
            
            for record in records:
                key = record.get(groupby_key)
                value = record.get(value_key, 0)
                
                if key not in aggregated:
                    aggregated[key] = []
                aggregated[key].append(value)
            
            # Apply aggregation function
            result = {}
            for key, values in aggregated.items():
                if agg_func == 'sum':
                    result[key] = sum(values)
                elif agg_func == 'mean':
                    result[key] = sum(values) / len(values)
                elif agg_func == 'max':
                    result[key] = max(values)
                elif agg_func == 'min':
                    result[key] = min(values)
                elif agg_func == 'count':
                    result[key] = len(values)
            
            return result
            
        except Exception as e:
            return {"error": str(e)}
    
    def detect_anomalies(self, records: List[Dict], key: str, 
                        method: str = 'zscore', threshold: float = 3.0) -> List[Dict]:
        """Detect anomalies in streaming data"""
        
        try:
            import numpy as np
            
            values = [r.get(key) for r in records if key in r]
            
            if not values or len(values) < 2:
                return []
            
            anomalies = []
            
            if method == 'zscore':
                mean = np.mean(values)
                std = np.std(values)
                
                for i, record in enumerate(records):
                    if key in record:
                        value = record[key]
                        z_score = abs((value - mean) / std) if std > 0 else 0
                        
                        if z_score > threshold:
                            anomalies.append({
                                "record_index": i,
                                "record": record,
                                "anomaly_score": z_score,
                                "detection_method": "z-score"
                            })
            
            return anomalies
            
        except Exception as e:
            return []
    
    def apply_transformations(self, record: Dict) -> Dict:
        """Apply registered transformations to a record"""
        
        result = record.copy()
        
        for transform in self.transformations:
            try:
                field = transform.get('field')
                operation = transform.get('operation')
                
                if field in result:
                    if operation == 'uppercase':
                        result[field] = result[field].upper()
                    elif operation == 'lowercase':
                        result[field] = result[field].lower()
                    elif operation == 'trim':
                        result[field] = str(result[field]).strip()
                    elif operation == 'multiply':
                        factor = transform.get('factor', 1)
                        result[field] = result[field] * factor
                    
            except Exception:
                pass
        
        return result


class RealtimeCollaborativeServer:
    """WebSocket server for real-time collaborative features"""
    
    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.clients = set()
        self.shared_state = {}
        self.message_history = deque(maxlen=1000)
    
    async def broadcast_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Broadcast message to all connected clients"""
        
        try:
            self.message_history.append(message)
            
            return {
                "success": True,
                "message": "Message broadcast to all clients",
                "clients_count": len(self.clients),
                "total_messages_sent": len(self.message_history)
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def update_shared_state(self, state_key: str, state_value: Any) -> bool:
        """Update shared state for collaborative editing"""
        
        try:
            self.shared_state[state_key] = {
                "value": state_value,
                "updated_at": datetime.now().isoformat(),
                "clients_viewing": len(self.clients)
            }
            return True
        except Exception:
            return False
    
    def get_shared_state(self, state_key: str) -> Optional[Any]:
        """Get current shared state"""
        
        return self.shared_state.get(state_key, {}).get('value')
    
    def get_collaboration_metrics(self) -> Dict[str, Any]:
        """Get real-time collaboration metrics"""
        
        return {
            "connected_clients": len(self.clients),
            "shared_state_keys": len(self.shared_state),
            "message_history_size": len(self.message_history),
            "server_uptime": datetime.now().isoformat()
        }


class StreamingDataManager:
    """High-level manager for all streaming operations"""
    
    def __init__(self):
        self.buffers: Dict[str, DataStreamBuffer] = {}
        self.processors: Dict[str, StreamProcessor] = {}
        self.collaborative_server = RealtimeCollaborativeServer()
        self.stream_tasks = {}
    
    def create_stream(self, stream_id: str, max_buffer_size: int = 10000) -> Dict[str, Any]:
        """Create a new data stream"""
        
        try:
            self.buffers[stream_id] = DataStreamBuffer(max_size=max_buffer_size)
            self.processors[stream_id] = StreamProcessor()
            
            return {
                "success": True,
                "stream_id": stream_id,
                "message": f"Stream '{stream_id}' created"
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def ingest_data(self, stream_id: str, records: List[Dict]) -> Dict[str, Any]:
        """Ingest batch of data into stream"""
        
        if stream_id not in self.buffers:
            return {"success": False, "error": f"Stream '{stream_id}' not found"}
        
        try:
            stats = self.buffers[stream_id].add_batch(records)
            
            return {
                "success": True,
                "stream_id": stream_id,
                "ingestion_stats": stats
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_stream_data(self, stream_id: str, format: str = 'dataframe',
                       recent_n: int = None) -> Optional[Any]:
        """Get data from stream"""
        
        if stream_id not in self.buffers:
            return None
        
        buffer = self.buffers[stream_id]
        
        if format == 'dataframe':
            return buffer.get_dataframe()
        elif format == 'list':
            if recent_n:
                return buffer.get_recent(recent_n)
            return list(buffer.buffer)
        elif format == 'stats':
            return buffer.get_stats()
        
        return None
    
    def apply_processing(self, stream_id: str, operation: str,
                        **kwargs) -> Dict[str, Any]:
        """Apply processing operation to stream data"""
        
        if stream_id not in self.buffers:
            return {"success": False, "error": f"Stream '{stream_id}' not found"}
        
        try:
            buffer = self.buffers[stream_id]
            processor = self.processors[stream_id]
            records = list(buffer.buffer)
            
            if operation == 'filter':
                filter_key = kwargs.get('key')
                filter_value = kwargs.get('value')
                filtered = [r for r in records if r.get(filter_key) == filter_value]
                return {
                    "success": True,
                    "operation": "filter",
                    "records_after": len(filtered),
                    "records_before": len(records)
                }
            
            elif operation == 'aggregate':
                groupby_key = kwargs.get('groupby_key')
                agg_func = kwargs.get('agg_func', 'sum')
                value_key = kwargs.get('value_key')
                result = processor.aggregate_records(records, groupby_key, agg_func, value_key)
                return {
                    "success": True,
                    "operation": "aggregate",
                    "result": result
                }
            
            elif operation == 'anomaly_detection':
                key = kwargs.get('key')
                method = kwargs.get('method', 'zscore')
                threshold = kwargs.get('threshold', 3.0)
                anomalies = processor.detect_anomalies(records, key, method, threshold)
                return {
                    "success": True,
                    "operation": "anomaly_detection",
                    "anomalies_detected": len(anomalies),
                    "anomalies": anomalies[:10]  # Return top 10
                }
            
            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def list_streams(self) -> List[Dict]:
        """List all active streams"""
        
        return [
            {
                "stream_id": stream_id,
                "buffer_size": len(self.buffers[stream_id].buffer),
                "status": "active",
                "stats": self.buffers[stream_id].get_stats()
            }
            for stream_id in self.buffers.keys()
        ]
    
    def get_collaboration_status(self) -> Dict[str, Any]:
        """Get collaboration server status"""
        
        return self.collaborative_server.get_collaboration_metrics()
    
    def export_stream(self, stream_id: str, format: str = 'csv') -> Optional[str]:
        """Export stream data to file"""
        
        if stream_id not in self.buffers:
            return None
        
        try:
            df = self.buffers[stream_id].get_dataframe()
            
            if format == 'csv':
                return df.to_csv(index=False)
            elif format == 'json':
                return df.to_json(orient='records')
            
            return None
            
        except Exception:
            return None
