from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, current_timestamp, explode, split, lit, when, col, lower
from pyspark.sql.types import StringType, IntegerType, ArrayType
import re

def read_from_table(spark, database, table):
    df = spark.read.csv('/content/testdata1.csv', header=True, inferSchema=True)

    @udf(StringType())
    def extract_table_names(query):
      matches = re.findall(r'(?:FROM|JOIN)\s+([^\s,]+)', query, re.IGNORECASE)
      return ",".join(matches)

    @udf(StringType())
    def find_date_operators(string):
          
        patterns = [
            r"(?i)\w+\.\w+\s+(BETWEEN)\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+'(\d{4}-\d{2}-\d{2})'",
            r"(?i)\w+\.\w+\s+(>=|>|<=|<|=|!=|=>)\s+'(\d{4}-\d{2}-\d{2})'",
            r"(?i)\w+\.\w+\s+(>=|>|<=|<|=|!=|=>)\s+(CURRENT_DATE|NOW|DATE|CURRENT_TIMESTAMP|TO_DATE|UNIX_TIMESTAMP|FROM_UNIXTIME|now|from_unixtime|unix_timestamp|to_date|year|quarter|month|day|dayofmonth|hour|minute|second|weekofyear|extract|c  ur  rent_date|date_add|date_sub|add_months|trunc)",
            r"(?i)\w+\.\w+\s+(BETWEEN)\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+(CURRENT_DATE|NOW|DATE|CURRENT_TIMESTAMP|TO_DATE|UNIX_TIMESTAMP|FROM_UNIXTIME|now|from_unixtime|unix_timestamp|to_date|year|quarter|month|day|dayofmonth|hour|minute|second|  we  ekofyear|extract|current_date|date_add|date_sub|add_months|trunc)"
        ]
    
        operators = []
    
        for pattern in patterns:
            matches = re.findall(pattern, string)
            if matches:
                for match in matches:
                    operators.append(match[0])
    
        if not operators:
            return "0"
        else:
            return ", ".join(operators)
    
    
    @udf(StringType())
    def find_dates_and_date_range(string):
    
        patterns = [
        (r"(?i)BETWEEN\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+(CURRENT_DATE\(\)|NOW\(\)|DATE\(\)|CURRENT_TIMESTAMP\(\)|TO_DATE\(\)|UNIX_TIMESTAMP\(\)|FROM_UNIXTIME\(\)|'\d{4}-\d{2}-\d{2}')", "between"),
        (r"(?i)\w+\.\w+\s+(>=|>|<=|<)\s+'(\d{4}-\d{2}-\d{2})'.*?(>=|>|<=|<)\s+(CURRENT_DATE\(\)|NOW\(\)|DATE\(\)|CURRENT_TIMESTAMP\(\)|TO_DATE\(\)|UNIX_TIMESTAMP\(\)|FROM_UNIXTIME\(\)|'\d{4}-\d{2}-\d{2}')", "range"),
        (r"(?i)\w+\.\w+\s+(>=|>|<=|<)\s+(CURRENT_DATE\(\)|NOW\(\)|DATE\(\)|CURRENT_TIMESTAMP\(\)|TO_DATE\(\)|UNIX_TIMESTAMP\(\)|FROM_UNIXTIME\(\)).*?(>=|>|<=|<)\s+(CURRENT_DATE\(\)|NOW\(\)|DATE\(\)|CURRENT_TIMESTAMP\(\)|TO_DATE\(\)|UNIX_TIMEST  AM  P\(\)|FROM_UNIXTIME\(\))", "range_function"),
    ]
    
        date_found = False
        result = ""
    
        for pattern, pattern_type in patterns:
            matches = re.findall(pattern, string)
            if matches:
                date_found = True
                for match in matches:
                    if pattern_type == "between":
                        start_date = match[0].strip("'")
                        end_date = match[1].strip("';")
                    else:
                        start_date = match[1].strip("'")
                        end_date = match[3].strip("';")
                    result += f"{start_date} - {end_date}"
                    break  # This will stop after the first match
    
        if not date_found:
            pattern_single_date = r"(?i)(>|>=|<=|<|=|!=)\s+'(\d{4}-\d{2}-\d{2})'"
            pattern_date_function = r"(?i)(DATE\(\)|CURDATE\(\)|NOW\(\)|SYSDATE\(\)|GETDATE\(\))"
            matches_single_date = re.findall(pattern_single_date, string)
            matches_date_function = re.findall(pattern_date_function, string)
    
            for match in matches_single_date:
                date = match[1].strip("'")
                result += f"{date}, "
                date_found = True
    
            for match in matches_date_function:
                result += f"{match}, "
                date_found = True
    
        if not date_found:
            result = "0"
    
        return result
    
    @udf(StringType())
    def find_columns(string):
        import re
    
        patterns = [
            r"(?i)(\w+\.\w+)\s+BETWEEN\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+'(\d{4}-\d{2}-\d{2})'",
            r"(?i)(\w+\.\w+)\s+(>=|>|<=|<|=|!=|=>)\s+'(\d{4}-\d{2}-\d{2})'",
            r"(?i)(\w+\.\w+)\s+(>=|>|<=|<|=|!=|=>)\s+(CURRENT_DATE|NOW|DATE|CURRENT_TIMESTAMP|TO_DATE|UNIX_TIMESTAMP|FROM_UNIXTIME|now|from_unixtime|unix_timestamp|to_date|year|quarter|month|day|dayofmonth|hour|minute|second|weekofyear|extract  |c  urrent_date|date_add|date_sub|add_months|trunc)",
            r"(?i)(\w+\.\w+)\s+BETWEEN\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+(CURRENT_DATE|NOW|DATE|CURRENT_TIMESTAMP|TO_DATE|UNIX_TIMESTAMP|FROM_UNIXTIME|now|from_unixtime|unix_timestamp|to_date|year|quarter|month|day|dayofmonth|hour|minute|second|  we  ekofyear|extract|current_date|date_add|date_sub|add_months|trunc)"
        ]
    
        columns = []
    
        for pattern in patterns:
            matches = re.findall(pattern, string)
            if matches:
                for match in matches:
                    columns.append(match[0])
    
        if not columns:
            return "0"
        else:
            return ", ".join(columns)
  
    @udf(StringType())
    def process_schema_name(schema_name):
      lower_case_schema = schema_name.lower()

      if re.match(r'fba.*', lower_case_schema):
        return 'real_time_GOLD_layer'
      elif re.match(r'fbz.*', lower_case_schema):
        return 'real_time_bronze_layer'
      elif re.match(r'f1d.*', lower_case_schema):
        return 'batch_bronze_layer'
      elif re.match(r'frc.*', lower_case_schema):
        return 'batch_gold_layer'
      else:
        return 'unknown_layer'
    def add_layer_column(df):
      return df.withColumn('type_of_table', when(col('schema').isNotNull(), process_schema_name(col('schema'))).otherwise('unknown_layer'))

    sample_df = df.withColumn('tables_name', extract_table_names('statement'))
    #sample_df = sample_df.withColumn('date_flag', extract_dates('statement'))

    exploded_df = sample_df.select('user', explode(split('tables_name', ',')).alias('tables_name1'),  'pool', 'querystate', 'starttime', 'endtime', 'xtrct_date', 'statement', current_timestamp().alias('asofdate'))

    final_df_selected = exploded_df.withColumnRenamed('tables_name1', 'tables_name')
    final_df_selected = final_df_selected.withColumn('schema', split('tables_name', '\.').getItem(0)).withColumn('table', split('tables_name', '\.').getItem(1)).withColumn("date_operators", find_date_operators("statement")) \
                        .withColumn("date_columns", find_columns("statement")) \
                        .withColumn("date_range", find_dates_and_date_range("statement"))


    final_df1 = final_df_selected.select('user', 'tables_name', 'schema', 'table', 'date_operators','date_columns','date_range',  'pool', 'querystate')
    final_df1 = add_layer_column(final_df1)
    df_pandas = final_df1.toPandas()
    display(df_pandas)
    #final_df = add_layer_column(final_df_selected)
    

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReadFromTable").getOrCreate()
    impala_database = "your_impala_database"
    impala_table = "your_impala_table"
    read_from_table(spark, impala_database, impala_table)
    spark.stop()



#######
Version 2.7

patterns = [
        r"\w+\.\w+\s+(BETWEEN)\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+'(\d{4}-\d{2}-\d{2})'",
        r"\w+\.\w+\s+(>=|>|<=|<|=|!=|=>)\s+'(\d{4}-\d{2}-\d{2})'",
        r"\w+\.\w+\s+(>=|>|<=|<|=|!=|=>)\s+(CURRENT_DATE|NOW|DATE|CURRENT_TIMESTAMP|TO_DATE|UNIX_TIMESTAMP|FROM_UNIXTIME|now|from_unixtime|unix_timestamp|to_date|year|quarter|month|day|dayofmonth|hour|minute|second|weekofyear|extract|current_date|date_add|date_sub|add_months|trunc)",
        r"\w+\.\w+\s+(BETWEEN)\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+(CURRENT_DATE|NOW|DATE|CURRENT_TIMESTAMP|TO_DATE|UNIX_TIMESTAMP|FROM_UNIXTIME|now|from_unixtime|unix_timestamp|to_date|year|quarter|month|day|dayofmonth|hour|minute|second|weekofyear|extract|current_date|date_add|date_sub|add_months|trunc)"
    ]