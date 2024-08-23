from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit, date_format, col, avg, round, datediff, max, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def generate_table_counts_graph(pandas_df):
    sns.set_style("whitegrid")
    
    pandas_df = pandas_df.sort_values("percent_of_average", ascending=True)
    
    fig, ax = plt.subplots(figsize=(14, len(pandas_df) * 0.4 + 2))
    
    sns.barplot(x="record_count", y="table_full_name", data=pandas_df, 
                label="Today's Count", color="skyblue", alpha=0.8, ax=ax)
    sns.barplot(x="latest_7_day_avg", y="table_full_name", data=pandas_df, 
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

table_conditions = {
    "schema1.table1": "WHERE COALESCE(status, 'unknown') = 'active'",
    "schema2.table2": "WHERE created_date >= DATE_SUB(CURRENT_DATE(), 30)",
    "schema1.table3": "WHERE COALESCE(amount, 0) > 1000",
    "schema3.table4": "WHERE COALESCE(is_verified, false) = true",
    "schema2.table5": "WHERE COALESCE(category, 'unknown') IN ('A', 'B', 'C')",
    "schema1.table6": "WHERE COALESCE(updated_date, CURRENT_DATE()) = CURRENT_DATE()",
    "schema3.table7": "WHERE COALESCE(age, 0) BETWEEN 18 AND 65"
}

today = current_date()

existing_counts = spark.table("metrics.daily_table_counts") \
    .filter(col("count_date") == today) \
    .select("schema_name", "table_name") \
    .collect()

existing_tables = set((row["schema_name"], row["table_name"]) for row in existing_counts)

count_dfs = []

for table, condition in table_conditions.items():
    schema, table_name = table.split('.')
    
    if (schema, table_name) in existing_tables:
        print(f"Skipping {schema}.{table_name} as entry already exists for today.")
        continue
    
    query = f"""
    SELECT 
        '{schema}' AS schema_name,
        '{table_name}' AS table_name,
        COUNT(*) AS record_count,
        CURRENT_DATE() AS count_date,
        COALESCE(AVG(CASE WHEN {condition.split('WHERE ')[1]} THEN 1 ELSE 0 END), 0) AS condition_ratio
    FROM {table}
    """
    count_df = spark.sql(query)
    count_dfs.append(count_df)

if count_dfs:
    final_df = count_dfs[0]
    for df in count_dfs[1:]:
        final_df = final_df.unionAll(df)

    final_df = final_df.withColumn("month", date_format("count_date", "yyyy-MM"))

    final_df.write \
        .mode("append") \
        .partitionBy("month") \
        .format("parquet") \
        .saveAsTable("metrics.daily_table_counts")

    spark.sql("ANALYZE TABLE metrics.daily_table_counts COMPUTE STATISTICS")

window_spec = Window.partitionBy("schema_name", "table_name").orderBy("count_date").rowsBetween(-7, -1)

trend_df = spark.table("metrics.daily_table_counts") \
    .withColumn("7_day_avg", round(avg("record_count").over(window_spec), 2)) \
    .withColumn("days_diff", datediff(today, col("count_date")))

latest_avg_df = trend_df.groupBy("schema_name", "table_name") \
    .agg(
        max("7_day_avg").alias("latest_7_day_avg"),
        max("days_diff").alias("days_since_last_count")
    )

result_df = trend_df.filter(col("count_date") == today) \
    .join(latest_avg_df, ["schema_name", "table_name"]) \
    .withColumn("percent_of_average", round((col("record_count") / col("latest_7_day_avg")) * 100, 2)) \
    .withColumn("table_full_name", concat(col("schema_name"), lit("."), col("table_name")))

pandas_df = result_df.select(
    "table_full_name", 
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
    "days_since_last_count"
).orderBy("schema_name", "table_name").show(100, truncate=False)

spark.stop()
