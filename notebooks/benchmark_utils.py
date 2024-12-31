import json
from datetime import datetime
import psutil
import pandas as pd
import numpy as np
from decimal import Decimal

class BenchmarkResult:
    def __init__(self, engine_name, engine_version):
        self.engine_name = engine_name
        self.engine_version = engine_version
        self.timestamp = datetime.now().isoformat()
        self.queries = {}
        self.resources = {}
        
    def _serialize_value(self, obj):
        """Helper method to make values JSON serializable"""
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        elif isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32, Decimal)):
            return float(obj)
        elif pd.isna(obj):
            return None
        elif isinstance(obj, pd.Series):
            return obj.to_dict()
        return obj

    def _make_serializable(self, data):
        """Convert all values to JSON serializable format"""
        if isinstance(data, dict):
            return {k: self._make_serializable(v) for k, v in data.items()}
        elif isinstance(data, (list, tuple)):
            return [self._make_serializable(v) for v in data]
        elif isinstance(data, pd.DataFrame):
            return self._make_serializable(data.to_dict(orient='records'))
        else:
            return self._serialize_value(data)
        
    def add_query_result(self, query_name, duration, total_records, sample_results=None):
        """Add a query benchmark result
        
        Args:
            query_name (str): Name of the query
            duration (float): Query execution time
            total_records (int): Total number of records processed (not result rows)
            sample_results: Sample of the results (optional)
        """
        # Convert sample results to a simple dictionary
        if isinstance(sample_results, pd.DataFrame):
            sample_dict = sample_results.head(1).to_dict(orient='records')[0] if not sample_results.empty else {}
        else:
            sample_dict = sample_results
            
        self.queries[query_name] = {
            "duration": float(duration),
            "row_count": int(total_records),  # Total records processed
            "sample_results": self._make_serializable(sample_dict)
        }
        
    def add_resource_usage(self):
        """Add current CPU and memory usage metrics"""
        self.resources = {
            "cpu_percent": float(psutil.cpu_percent()),
            "memory_mb": float(psutil.Process().memory_info().rss / (1024 * 1024))
        }
    
    def to_dict(self):
        """Convert result to dictionary for JSON serialization"""
        return self._make_serializable({
            "engine": self.engine_name,
            "version": self.engine_version,
            "timestamp": self.timestamp,
            "queries": self.queries,
            "resources": self.resources
        })
    
    def save(self, filename):
        """Save results to JSON file"""
        # Add resource usage before saving
        self.add_resource_usage()
        
        # Save to file
        with open(filename, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        print(f"Benchmark results saved to {filename}")