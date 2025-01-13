from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

def write_to_parquet_table(new_df, table_name, num_files=1):
   try:
       new_df = new_df.withColumn("as_of_date", current_date())
       
       new_df.createOrReplaceTempView("new_data")
       
       if not spark.catalog.tableExists(table_name):
           print(f"Creating new table {table_name}")
           new_df.coalesce(num_files) \
               .write \
               .format("parquet") \
               .mode("overwrite") \
               .saveAsTable(table_name)
       else:
           duplicate_check_sql = f"""
           SELECT 
               n.*,
               t.*
           FROM new_data n
           INNER JOIN {table_name} t
           ON n.event_date = t.event_date
           """
           
           duplicates = spark.sql(duplicate_check_sql)
           if duplicates.count() > 0:
               print("\nFollowing records already exist:")
               duplicates.show(truncate=False)
           
           insert_sql = f"""
           SELECT n.*
           FROM new_data n
           LEFT JOIN {table_name} t
           ON n.event_date = t.event_date
           WHERE t.event_date IS NULL
           """
           
           new_records = spark.sql(insert_sql)
           new_count = new_records.count()
           
           if new_count > 0:
               print(f"\nInserting {new_count} new records:")
               new_records.show(truncate=False)
               
               new_records.coalesce(num_files) \
                   .write \
                   .format("parquet") \
                   .mode("append") \
                   .saveAsTable(table_name)
               
               print(f"Added new records and coalesced into {num_files} files")
           else:
               print("No new records to add - all records already exist")
           
           print("\nCurrent table statistics:")
           spark.sql(f"""
               SELECT 
                   COUNT(*) as total_records,
                   MIN(event_date) as earliest_date,
                   MAX(event_date) as latest_date,
                   COUNT(DISTINCT event_date) as unique_dates
               FROM {table_name}
           """).show()
           
   except Exception as e:
       print(f"Error: {str(e)}")
       raise

spark = SparkSession.builder \
   .appName("rr") \
   .enableHiveSupport() \
   .getOrCreate()

table_name = ""

write_to_parquet_table(df, table_name, num_files=1)
