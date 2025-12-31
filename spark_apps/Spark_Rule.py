import os
import sys
import json
import logging
import time
import yaml
import ast
import re
from datetime import datetime
from typing import Dict, List, Any

from pyspark.sql import SparkSession, DataFrame, Column, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, FloatType, LongType
)

import great_expectations as gx
try:
    from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
except ImportError:
    from great_expectations.core.expectation_configuration import ExpectationConfiguration

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FullHybridDQ")

class YamlRuleConfigLoader:
    def __init__(self, yaml_path: str):
        self.yaml_path = yaml_path

    def load_rules(self) -> Dict[str, Any]:
        if not os.path.exists(self.yaml_path):
            logger.error(f"Config file not found: {self.yaml_path}")
            return {"row_rules": [], "batch_rules": []}

        with open(self.yaml_path, 'r') as f:
            config_data = yaml.safe_load(f)

        all_rules = config_data.get("expectations", [])
        
        batch_prefixes = [
            "expect_table_", "expect_column_max_", "expect_column_min_", 
            "expect_column_mean_", "expect_column_median_", "expect_column_sum_", 
            "expect_column_stdev_", "expect_column_unique_value_count_", 
            "expect_column_proportion_", "expect_column_distinct_values_", 
            "expect_column_most_common_", "expect_column_quantile_", 
            "expect_column_kl_", "expect_compound_columns_to_be_unique",
            "expect_column_values_to_be_increasing", "expect_column_values_to_be_decreasing"
        ]

        row_rules = []
        batch_rules = []

        for rule in all_rules:
            exp_type = rule.get("type")
            if "value_set" in rule.get("kwargs", {}):
                pass 
            
            if any(exp_type.startswith(p) for p in batch_prefixes):
                batch_rules.append(rule)
            else:
                row_rules.append(rule)
        
        logger.info(f"Loaded {len(row_rules)} Row Rules and {len(batch_rules)} Batch Rules.")
        return {"row_rules": row_rules, "batch_rules": batch_rules}

class RowLevelTranslator:
    @staticmethod
    def translate(rule: Dict) -> Column:
        exp_type = rule.get("type")
        kwargs = rule.get("kwargs", {})
        col_name = rule.get("column") or kwargs.get("column")
        c = F.col(col_name) if col_name else None

        if exp_type == "expect_column_to_exist": return F.lit(True)
        if exp_type == "expect_column_values_to_not_be_null": return c.isNotNull()
        if exp_type == "expect_column_values_to_be_null": return c.isNull()

        if exp_type == "expect_column_values_to_be_of_type":
            target_type = kwargs.get("type")
            if target_type == "IntegerType": return c.cast("int").isNotNull()
            if target_type == "FloatType" or target_type == "DoubleType": return c.cast("double").isNotNull()
            return F.lit(True)

        if exp_type == "expect_column_values_to_be_in_type_list":
            return c.isNotNull() 

        if exp_type == "expect_column_values_to_be_json_parseable":
            return F.get_json_object(c, '$').isNotNull() | (c == "{}") | (c == "[]")

        if exp_type == "expect_column_values_to_be_between":
            return (c >= kwargs.get("min_value", -float('inf'))) & (c <= kwargs.get("max_value", float('inf')))

        if exp_type == "expect_column_values_to_be_in_set":
            return c.isin(kwargs.get("value_set", []))
        if exp_type == "expect_column_values_to_not_be_in_set":
            return ~c.isin(kwargs.get("value_set", []))

        if exp_type == "expect_column_values_to_match_regex":
            return c.rlike(kwargs.get("regex"))
        if exp_type == "expect_column_values_to_not_match_regex":
            return ~c.rlike(kwargs.get("regex"))
        
        if exp_type == "expect_column_values_to_match_regex_list":
            patterns = kwargs.get("regex_list", [])
            cond = F.lit(False)
            for p in patterns: cond = cond | c.rlike(p)
            return cond
            
        if exp_type == "expect_column_values_to_not_match_regex_list":
            patterns = kwargs.get("regex_list", [])
            cond = F.lit(True)
            for p in patterns: cond = cond & (~c.rlike(p))
            return cond

        if "like_pattern" in exp_type:
            pattern = kwargs.get("like_pattern")
            if "not" in exp_type: return ~c.like(pattern)
            return c.like(pattern)

        if "like_pattern_list" in exp_type:
            patterns = kwargs.get("like_pattern_list", [])
            if "not" in exp_type:
                cond = F.lit(True)
                for p in patterns: cond = cond & (~c.like(p))
            else:
                cond = F.lit(False)
                for p in patterns: cond = cond | c.like(p)
            return cond

        if exp_type == "expect_column_value_lengths_to_be_between":
            return (F.length(c) >= kwargs.get("min_value", 0)) & (F.length(c) <= kwargs.get("max_value", 99999))
        if exp_type == "expect_column_value_lengths_to_equal":
            return F.length(c) == kwargs.get("value")

        if exp_type == "expect_column_values_to_match_strftime_format":
            fmt = kwargs.get("strftime_format")
            spark_fmt = fmt.replace("%Y", "yyyy").replace("%m", "MM").replace("%d", "dd")
            return F.to_date(c, spark_fmt).isNotNull()
            
        if exp_type == "expect_column_values_to_be_dateutil_parseable":
            return c.cast("timestamp").isNotNull()

        if exp_type == "expect_column_pair_values_a_to_be_greater_than_b":
            col_b = kwargs.get("column_B")
            return c > F.col(col_b)
        
        if exp_type == "expect_column_pair_values_to_be_equal":
            col_b = kwargs.get("column_B")
            return c == F.col(col_b)

        if exp_type == "expect_column_pair_values_to_be_in_set":
            col_b = kwargs.get("column_B")
            pairs = kwargs.get("value_set", [])
            cond = F.lit(False)
            for p in pairs:
                cond = cond | ((c == p[0]) & (F.col(col_b) == p[1]))
            return cond

        if exp_type == "expect_select_column_values_to_be_unique_within_record":
            cols = kwargs.get("column_list", [])
            arr = F.array([F.col(x) for x in cols])
            return F.size(F.array_distinct(arr)) == F.size(arr)

        return F.lit(True)

class BatchStatisticValidator:
    @staticmethod
    def validate(df: DataFrame, rules: List[Dict]) -> List[Dict]:
        results = []
        if not rules: return results
        
        df.cache()
        count = df.count()
        if count == 0: return results
        numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, FloatType, LongType))]
        str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (StringType))]
        
        stats = {}
        if numeric_cols:
            aggs = []
            for c in numeric_cols:
                aggs += [F.min(c).alias(f"{c}_min"), F.max(c).alias(f"{c}_max"), 
                         F.mean(c).alias(f"{c}_mean"), F.stddev(c).alias(f"{c}_std"), 
                         F.sum(c).alias(f"{c}_sum")]
            try:
                row = df.agg(*aggs).collect()[0]
                stats.update(row.asDict())
            except: pass

        aggs_dist = []
        for c in df.columns:
            aggs_dist.append(F.countDistinct(c).alias(f"{c}_distinct"))
            aggs_dist.append(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls"))
        try:
            row_dist = df.agg(*aggs_dist).collect()[0]
            stats.update(row_dist.asDict())
        except: pass

        for rule in rules:
            etype = rule.get("type")
            kwargs = rule.get("kwargs", {})
            col = rule.get("column") or kwargs.get("column")
            
            success = False
            observed = None
            
            try:
                if "expect_table_row_count" in etype:
                    observed = count
                    min_v = kwargs.get("min_value", 0)
                    max_v = kwargs.get("max_value", float('inf'))
                    val = kwargs.get("value")
                    if val: success = (count == val)
                    else: success = (min_v <= count <= max_v)

                elif "expect_table_column_count" in etype:
                    observed = len(df.columns)
                    val = kwargs.get("value")
                    if val: success = (observed == val)
                    else: success = (kwargs.get("min_value", 0) <= observed <= kwargs.get("max_value", 999))
                
                elif "expect_table_columns_to_match" in etype:
                    expected = kwargs.get("column_list") or kwargs.get("column_set")
                    if "ordered" in etype: success = (df.columns == expected)
                    else: success = (set(df.columns) == set(expected))
                    observed = str(df.columns)

                elif col and col in numeric_cols and any(x in etype for x in ["min", "max", "mean", "sum", "stdev", "median"]):
                    stat_key = None
                    if "min" in etype: stat_key = f"{col}_min"
                    elif "max" in etype: stat_key = f"{col}_max"
                    elif "mean" in etype: stat_key = f"{col}_mean"
                    elif "sum" in etype: stat_key = f"{col}_sum"
                    elif "stdev" in etype: stat_key = f"{col}_std"
                    
                    if stat_key and stat_key in stats:
                        observed = stats[stat_key]
                        if observed is not None:
                            success = (kwargs.get("min_value", -float('inf')) <= observed <= kwargs.get("max_value", float('inf')))
                
                elif "unique_value_count" in etype:
                    observed = stats.get(f"{col}_distinct", 0)
                    success = (kwargs.get("min_value", 0) <= observed <= kwargs.get("max_value", 999999))

                elif "proportion_of_unique_values" in etype:
                    distinct = stats.get(f"{col}_distinct", 0)
                    observed = distinct / count if count > 0 else 0
                    success = (kwargs.get("min_value", 0) <= observed <= kwargs.get("max_value", 1))

                elif "proportion_of_non_null_values" in etype:
                    nulls = stats.get(f"{col}_nulls", 0)
                    observed = (count - nulls) / count if count > 0 else 0
                    success = (kwargs.get("min_value", 0) <= observed <= kwargs.get("max_value", 1))

                elif "distinct_values_to_be_in_set" in etype or "equal_set" in etype:
                    actual_set = set(row[col] for row in df.select(col).distinct().collect())
                    target = set(kwargs.get("value_set", []))
                    if "equal" in etype: success = (actual_set == target)
                    elif "contain" in etype: success = target.issubset(actual_set)
                    else: success = actual_set.issubset(target)
                    observed = str(list(actual_set)[:5])

                elif "most_common_value" in etype:
                    top = df.groupBy(col).count().orderBy(F.desc("count")).first()
                    if top:
                        observed = top[col]
                        success = observed in kwargs.get("value_set", [])

                elif "quantile" in etype:
                    q_list = kwargs.get("quantile_ranges", {}).get("quantiles", [0.5])
                    ranges = kwargs.get("quantile_ranges", {}).get("value_ranges", [])
                    approx = df.stat.approxQuantile(col, q_list, 0.05)
                    success = True
                    for i, val in enumerate(approx):
                        r = ranges[i]
                        if not (r[0] <= val <= r[1]): success = False
                    observed = str(approx)

                elif "compound_columns_to_be_unique" in etype:
                    cols = kwargs.get("column_list")
                    distinct_count = df.select(cols).distinct().count()
                    observed = distinct_count
                    success = (distinct_count == count)

                elif "increasing" in etype or "decreasing" in etype:
                    vals = [r[col] for r in df.select(col).sort("transaction_date").collect() if r[col] is not None]
                    if "increasing" in etype: success = all(x <= y for x, y in zip(vals, vals[1:]))
                    else: success = all(x >= y for x, y in zip(vals, vals[1:]))
                    observed = "Checked Monotonicity"

            except Exception as e:
                observed = f"Error: {str(e)}"
                success = False

            results.append({"rule": etype, "column": col, "success": success, "observed": observed})

        return results

class HybridDataQualityPipeline:
    def __init__(self):
        self.config_loader = YamlRuleConfigLoader("config/rules/user_profile.yaml")
        self.rules = self.config_loader.load_rules()
        
        self.spark = SparkSession.builder \
            .appName("FullHybridDQ_V2") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9010") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        self.input_schema = StructType([
            StructField("id", StringType()), StructField("name", StringType()),
            StructField("age", IntegerType()), StructField("role", StringType()),
            StructField("salary", DoubleType()), StructField("email", StringType()),
            StructField("transaction_date", StringType()),
            StructField("extra_metadata", StringType()), # Added for JSON rule
            StructField("null_col_placeholder", StringType()) # Added for Null rule
        ])

    def process_batch(self, df: DataFrame, batch_id: int):
        if df.count() == 0: return
        
        process_date = datetime.now().strftime("%Y-%m-%d")
        
        parsed_df = df.select(F.from_json(F.col("value").cast("string"), self.input_schema).alias("data")).select("data.*")
        
        val_df = parsed_df
        exprs = []
        rule_cols = []
        
        for i, rule in enumerate(self.rules["row_rules"]):
            cond = RowLevelTranslator.translate(rule)
            col_name = f"rule_{i}"
            val_df = val_df.withColumn(col_name, cond)
            exprs.append(F.col(col_name).cast("int"))
            rule_cols.append(col_name)

        if exprs:
            score_col = sum(exprs) / F.lit(len(exprs))
            val_df = val_df.withColumn("score", score_col)
            val_df = val_df.withColumn("status", F.when(F.col("score") == 1.0, "VALID").otherwise("INVALID"))
        else:
            val_df = val_df.withColumn("status", F.lit("VALID"))

        val_df = val_df.withColumn("process_date", F.lit(process_date))
        
        val_df.cache()
        total_count = val_df.count()

        print(f"\n{'='*100}")
        print(f"BATCH {batch_id} REPORT (Total: {total_count} records)")
        print(f"{'='*100}")
        print(f"| {'Column':<20} | {'Row Rule Type':<45} | {'Status':<10} | {'Fail Rate':<10} |")
        print(f"{'-'*100}")

        agg_exprs = [F.sum(F.col(c).cast("int")).alias(c) for c in rule_cols]
        stats_row = val_df.agg(*agg_exprs).collect()[0]

        for i, rule in enumerate(self.rules["row_rules"]):
            pass_count = stats_row[f"rule_{i}"] or 0
            fail_count = total_count - pass_count
            fail_rate = (fail_count / total_count) * 100 if total_count > 0 else 0
            
            rtype = rule.get("type").replace("expect_column_values_", "").replace("expect_column_", "")[:42]
            col_name = rule.get("column") or "global"
            
            status_str = "\033[92mPASSED\033[0m" if fail_count == 0 else "\033[91mFAILED\033[0m"
            print(f"| {col_name:<20} | {rtype:<45} | {status_str:<19} | {fail_rate:>8.1f}% |")
        print(f"{'-'*100}")

        print("\n--- Batch Statistics (Checks on Distribution/Aggregations) ---")
        if total_count > 0:
            clean_df_for_stats = val_df.select([c for c in val_df.columns if not c.startswith("rule_") and c not in ["score", "status"]])
            batch_results = BatchStatisticValidator.validate(clean_df_for_stats, self.rules["batch_rules"])
            
            for res in batch_results:
                status = "\033[92mPASS\033[0m" if res['success'] else "\033[91mFAIL\033[0m"
                obs_text = str(res['observed'])
                if len(obs_text) > 40: obs_text = obs_text[:37] + "..."
                print(f"{status} | {res['rule']:<50} | Obs: {obs_text}")
        else:
            print("No data to calculate statistics.")

        valid_df = val_df.filter(F.col("status") == "VALID").drop(*rule_cols)
        valid_count = valid_df.count()
        
        if valid_count > 0:
            print(f"\n[WRITE] Writing {valid_count} VALID records to SILVER layer...")
            valid_df.write \
                .mode("append") \
                .partitionBy("process_date") \
                .parquet("s3a://datalake/silver/user_profile")
        
        invalid_df = val_df.filter(F.col("status") == "INVALID")
        invalid_count = invalid_df.count()
        
        if invalid_count > 0:
            print(f"[WRITE] Writing {invalid_count} INVALID records to QUARANTINE layer...")
            invalid_df.write \
                .mode("append") \
                .partitionBy("process_date") \
                .parquet("s3a://datalake/quarantine/user_profile")

        print(f"Batch Processing Completed. Valid: {valid_count} | Invalid: {invalid_count}")
        val_df.unpersist()

    def run(self):
        self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:19092") \
            .option("subscribe", "user_data_topic") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load() \
            .writeStream \
            .foreachBatch(self.process_batch) \
            .start().awaitTermination()

if __name__ == "__main__":
    HybridDataQualityPipeline().run()