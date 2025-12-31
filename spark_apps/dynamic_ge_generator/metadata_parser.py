# spark_apps/dynamic_ge_generator/metadata_parser.py
from typing import Dict, List, Any
import pandas as pd
import numpy as np
import re

class MetadataParser:
    @staticmethod
    def parse_spark_schema(schema) -> Dict:
        metadata = {
            "columns": {},
            "data_types": {},
            "nullable_info": {}
        }
        
        for field in schema.fields:
            col_name = field.name
            col_type = str(field.dataType)
            nullable = field.nullable
            
            metadata["columns"][col_name] = {
                "name": col_name,
                "spark_type": col_type,
                "nullable": nullable,
                "inferred_type": MetadataParser._infer_type_from_spark_type(col_type)
            }
            metadata["data_types"][col_name] = col_type
            metadata["nullable_info"][col_name] = nullable
        
        return metadata
    
    @staticmethod
    def _infer_type_from_spark_type(spark_type: str) -> str:
        spark_type_lower = spark_type.lower()
        
        if "string" in spark_type_lower:
            return "string"
        elif "int" in spark_type_lower or "long" in spark_type_lower:
            return "integer"
        elif "double" in spark_type_lower or "float" in spark_type_lower or "decimal" in spark_type_lower:
            return "numeric"
        elif "date" in spark_type_lower or "timestamp" in spark_type_lower:
            return "date"
        elif "boolean" in spark_type_lower:
            return "boolean"
        elif "array" in spark_type_lower:
            return "array"
        elif "struct" in spark_type_lower:
            return "struct"
        else:
            return "generic"
    
    @staticmethod
    def parse_json_schema(json_schema: Dict) -> Dict:
        metadata = {
            "columns": {},
            "properties": {}
        }
        
        if "properties" in json_schema:
            for prop_name, prop_details in json_schema["properties"].items():
                metadata["columns"][prop_name] = {
                    "name": prop_name,
                    "json_type": prop_details.get("type", "string"),
                    "description": prop_details.get("description", ""),
                    "format": prop_details.get("format", ""),
                    "required": prop_name in json_schema.get("required", [])
                }
                metadata["properties"][prop_name] = prop_details
        
        return metadata