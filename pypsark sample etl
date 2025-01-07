from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_date, date_sub, current_timestamp, 
    date_trunc, concat, lit, to_timestamp
)
from datetime import datetime, timedelta
import os

def get_or_create_spark():
    """Create new SparkSession or get existing one"""
    return SparkSession.builder \
        .appName("ETL Process") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "spark-warehouse")) \
        .getOrCreate()

def get_filtered_table1(spark, source_table_name, days_lookback=180):
    """
    Creates table1 with filtered data from source table based on date range using BETWEEN operator
    
    Args:
        spark: SparkSession object
        source_table_name: Name of the source table
        days_lookback: Number of days to look back (default 180)
    """
    # Calculate the date range
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days_lookback)).strftime('%Y-%m-%d')
    
    # Create table1 with date filtering using BETWEEN
    table1_query = f"""
        SELECT 
            cast(event_date as date) as event_date,
            region,
            sales,
            quantity,
            current_timestamp() as processing_time,
            cast(date_trunc('month', cast(event_date as date)) as timestamp) as month_partition
        FROM {source_table_name}
        WHERE cast(event_date as date) BETWEEN cast('{start_date}' as date) AND cast('{end_date}' as date)
    """
    
    table1_df = spark.sql(table1_query)
    
    # Save table1
    warehouse_dir = os.path.join(os.getcwd(), "spark-warehouse")
    table1_location = os.path.join(warehouse_dir, "table1")
    
    # Clean up existing table
    spark.sql("DROP TABLE IF EXISTS table1")
    
    table1_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", table1_location) \
        .saveAsTable("table1")
        
    return table1_df

def process_data(spark, source_table_name, days_lookback=180):
    """
    Main function to process data from source table
    """
    try:
        print(f"Processing data from {source_table_name}...")
        
        # Get filtered table1 data
        table1_df = get_filtered_table1(spark, source_table_name, days_lookback)
        
        # Show statistics
        print("\nDate range statistics:")
        spark.sql("""
            SELECT 
                MIN(event_date) as earliest_date,
                MAX(event_date) as latest_date,
                COUNT(*) as total_records
            FROM table1
        """).show()
        
        return table1_df
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise
    finally:
        print("Processing completed")

def main(source_table_name):
    """
    Main execution function
    Args:
        source_table_name: Name of the source table to process
    """
    spark = None
    try:
        spark = get_or_create_spark()
        spark.sparkContext.setLogLevel("ERROR")
        process_data(spark, source_table_name)
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    # Replace 'your_source_table' with actual source table name when running
    main("your_source_table")
