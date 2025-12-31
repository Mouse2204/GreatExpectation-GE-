import great_expectations as gx
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from datetime import datetime
import re

class DynamicGESuiteGenerator:
    
    def __init__(self, config_base_path: str = "/opt/spark/apps/config/"):
        self.context = gx.get_context()
        self.config_base_path = config_base_path
        self.global_config = {
            "cardinality_threshold": 0.05, 
            "max_category_values": 20, 
            "numeric_std_devs": 3 
        }
        self.rule_templates = self._initialize_rule_templates()
    
    def _initialize_rule_templates(self) -> Dict:
        return {
            "not_null": {"expectation": "ExpectColumnValuesToNotBeNull", "params": {"column": None, "mostly": 1.0}},
            "unique": {"expectation": "ExpectColumnValuesToBeUnique", "params": {"column": None, "mostly": 1.0}},
            "value_set": {"expectation": "ExpectColumnValuesToBeInSet", "params": {"column": None, "value_set": []}},
            "range_statistical": {"expectation": "ExpectColumnValuesToBeBetween", "params": {"column": None, "min_value": None, "max_value": None}},
            "type_check": {"expectation": "ExpectColumnValuesToBeInTypeList", "params": {"column": None, "type_list": []}},
            "email_format": {"expectation": "ExpectColumnValuesToMatchRegex", "params": {"column": None, "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}}
        }

    def generate_suite(self, metadata: Dict, strictness: float = None, domain: str = "auto") -> gx.ExpectationSuite:
        suite_name = f"auto_suite_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"Generating DATA-DRIVEN suite {suite_name}")
        
        suite = self.context.suites.add(gx.ExpectationSuite(name=suite_name))
        
        for col_name, col_stats in metadata["columns"].items():
            self._generate_automatic_rules_for_column(suite, col_stats)
            
        return suite

    def _generate_automatic_rules_for_column(self, suite: gx.ExpectationSuite, stats: Dict):
        col_name = stats["name"]
        row_count = stats.get("row_count", 100)
        
        if stats["null_count"] == 0:
            self._add_expectation(suite, "not_null", col_name)
        elif stats.get("null_ratio", 0) < 0.05:
            self._add_expectation(suite, "not_null", col_name, mostly=0.95)

        if stats["unique_count"] == row_count and row_count > 1:
             self._add_expectation(suite, "unique", col_name)

        if (stats.get("cardinality_ratio", 1) < self.global_config["cardinality_threshold"] 
            and stats["unique_count"] < self.global_config["max_category_values"]):
            value_set = stats.get("sample_values", [])
            value_set = list(set(value_set))
            if value_set:
                self._add_expectation(suite, "value_set", col_name, value_set=value_set)

        if stats.get("type_category") == "numeric" and stats.get("std", 0) > 0:
            mean_val = stats["mean"]
            std_val = stats["std"]
            min_val = mean_val - (3 * std_val)
            max_val = mean_val + (3 * std_val)
            if stats.get("min", 0) >= 0 and min_val < 0:
                min_val = 0
            self._add_expectation(suite, "range_statistical", col_name, min_value=min_val, max_value=max_val)

        if stats.get("type_category") == "numeric":
             self._add_expectation(suite, "type_check", col_name, type_list=["IntegerType", "LongType", "DoubleType", "FloatType", "int", "float", "int64", "float64"])
        elif stats.get("type_category") == "string":
             self._add_expectation(suite, "type_check", col_name, type_list=["StringType", "str", "object"])

        if stats.get("semantic_type") == "email":
            self._add_expectation(suite, "email_format", col_name)

    def _add_expectation(self, suite, template_name, col_name, **kwargs):
        template = self.rule_templates.get(template_name)
        if not template: return
        exp_type = template["expectation"]
        params = template["params"].copy()
        params["column"] = col_name
        for k, v in kwargs.items(): params[k] = v
        
        expectation_func = getattr(gx.expectations, exp_type, None)
        if expectation_func:
            suite.add_expectation(expectation_func(**params))