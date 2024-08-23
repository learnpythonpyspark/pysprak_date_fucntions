from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit, date_format, col, avg, round, datediff, max, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Customizable schema and table names
SCHEMA_NAME = "metrics"
INPUT_TABLE_NAME = "custom_input_table"
TARGET_TABLE_NAME = "custom_target_table"

def table_exists(spark, schema, table):
    try:
        spark.table(f"{schema}.{table}")
        return True
    except:
        return False

def create_input_table_if_not_exists(spark, schema, table):
    if not table_exists(spark, schema, table):
        empty_df = spark.createDataFrame([], schema=StructType([
            StructField("schema_name", StringType(), nullable=False),
            StructField("table_name", StringType(), nullable=False),
            StructField("condition_column", StringType(), nullable=False),
            StructField("last_updated", DateType(), nullable=False)
        ]))
        empty_df.write \
            .mode("overwrite") \
            .partitionBy("last_updated") \
            .format("parquet") \
            .saveAsTable(f"{schema}.{table}")
        print(f"Created empty input table {schema}.{table}")

def create_target_table_if_not_exists(spark, schema, table):
    if not table_exists(spark, schema, table):
        empty_df = spark.createDataFrame([], schema=StructType([
            StructField("schema_name", StringType(), nullable=False),
            StructField("table_name", StringType(), nullable=False),
            StructField("record_count", IntegerType(), nullable=False),
            StructField("count_date", DateType(), nullable=False),
            StructField("condition_date", DateType(), nullable=False),
            StructField("month", StringType(), nullable=False)
        ]))
        empty_df.write \
            .mode("overwrite") \
            .partitionBy("month") \
            .format("parquet") \
            .saveAsTable(f"{schema}.{table}")
        print(f"Created empty target table {schema}.{table}")

def generate_table_counts_graph(pandas_df):
    sns.set_style("whitegrid")
    pandas_df = pandas_df.sort_values("percent_of_average", ascending=True)
    pandas_df['full_table_name'] = pandas_df['schema_name'] + '.' + pandas_df['table_name']
    fig, ax = plt.subplots(figsize=(14, len(pandas_df) * 0.4 + 2))
    sns.barplot(x="record_count", y="full_table_name", data=pandas_df, 
                label="Today's Count", color="skyblue", alpha=0.8, ax=ax)
    sns.barplot(x="latest_7_day_avg", y="full_table_name", data=pandas_df, 
                label="7-day Average", color="navy", alpha=0.5, ax=ax)
    ax.set_xlabel("Record Count", fontsize=12)
    ax.set_ylabel("Table Name", fontsize=12)
    ax.set_title("Today's Count vs 7-day Average", fontsize=16, fontweight='bold')
    ax.legend(fontsize=10)
    for i, row in pandas_df.iterrows():
        ax.text(max(row["record_count"], row["latest_7_day_avg"]), i, 
                f"{row['percent_of_average']}%", va='center', ha='left', 
                fontweight='bold', fontsize=10, color='red')
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    ax.axvline(x=0, color='gray', linestyle='--', linewidth=0.8)
    plt.tight_layout()
    return fig

spark = SparkSession.builder \
    .appName("DailyTableCounts") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# List of tables and their condition columns
table_list = [
    ("schema1", "table1", "date_column1"),
    ("schema2", "table2", "timestamp_column2"),
    # ... add all 40 tables here
]

# Ensure input table exists
create_input_table_if_not_exists(spark, SCHEMA_NAME, INPUT_TABLE_NAME)

# Create or update input table
input_df = spark.createDataFrame(table_list, ["schema_name", "table_name", "condition_column"])
input_df = input_df.withColumn("last_updated", current_date())

# Update input table with new data
input_df.write \
    .mode("overwrite") \
    .partitionBy("last_updated") \
    .format("parquet") \
    .saveAsTable(f"{SCHEMA_NAME}.{INPUT_TABLE_NAME}")
print(f"Input table {SCHEMA_NAME}.{INPUT_TABLE_NAME} updated.")

# Read the updated input table
input_tables = spark.table(f"{SCHEMA_NAME}.{INPUT_TABLE_NAME}")

today = current_date()

# Ensure target table exists
create_target_table_if_not_exists(spark, SCHEMA_NAME, TARGET_TABLE_NAME)

# Check for existing entries in the target table for today
existing_counts = spark.table(f"{SCHEMA_NAME}.{TARGET_TABLE_NAME}") \
    .filter(col("count_date") == today) \
    .select("schema_name", "table_name") \
    .collect()
existing_tables = set((row["schema_name"], row["table_name"]) for row in existing_counts)

count_dfs = []

for row in input_tables.collect():
    schema_name = row['schema_name']
    table_name = row['table_name']
    condition_col = row['condition_column']
    
    if (schema_name, table_name) in existing_tables:
        print(f"Skipping {schema_name}.{table_name} as entry already exists for today.")
        continue
    
    query = f"""
SELECT 
    '{schema_name}' AS schema_name,
    '{table_name}' AS table_name,
    COUNT(*) AS record_count,
    DATE_SUB(CURRENT_DATE(), 1) AS count_date,
    DATE({condition_col}) AS condition_date
FROM {schema_name}.{table_name}
WHERE DATE({condition_col}) = DATE_SUB(CURRENT_DATE(), 1)
"""
    
    count_df = spark.sql(query)
    count_dfs.append(count_df)

if count_dfs:
    final_df = count_dfs[0]
    for df in count_dfs[1:]:
        final_df = final_df.unionAll(df)

    final_df = final_df.withColumn("month", date_format("count_date", "yyyy-MM"))

    # Append new data to the target table
    final_df.write \
        .mode("append") \
        .partitionBy("month") \
        .format("parquet") \
        .saveAsTable(f"{SCHEMA_NAME}.{TARGET_TABLE_NAME}")

    spark.sql(f"ANALYZE TABLE {SCHEMA_NAME}.{TARGET_TABLE_NAME} COMPUTE STATISTICS")

# Calculate trends
window_spec = Window.partitionBy("schema_name", "table_name").orderBy("count_date").rowsBetween(-7, -1)

trend_df = spark.table(f"{SCHEMA_NAME}.{TARGET_TABLE_NAME}") \
    .withColumn("7_day_avg", round(avg("record_count").over(window_spec), 2)) \
    .withColumn("days_diff", datediff(today, col("count_date")))

latest_avg_df = trend_df.groupBy("schema_name", "table_name") \
    .agg(
        max("7_day_avg").alias("latest_7_day_avg"),
        max("days_diff").alias("days_since_last_count")
    )

result_df = trend_df.filter(col("count_date") == today) \
    .join(latest_avg_df, ["schema_name", "table_name"]) \
    .withColumn("percent_of_average", round((col("record_count") / col("latest_7_day_avg")) * 100, 2))

pandas_df = result_df.select(
    "schema_name",
    "table_name", 
    "record_count", 
    "latest_7_day_avg", 
    "percent_of_average"
).toPandas()

fig = generate_table_counts_graph(pandas_df)
plt.show()

result_df.select(
    "schema_name",
    "table_name", 
    "record_count", 
    "latest_7_day_avg", 
    "percent_of_average",
    "days_since_last_count",
    "condition_date"
).orderBy("schema_name", "table_name").show(100, truncate=False)

spark.stop()
