from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F, types as T
import re

def create_dataframe(spark: SparkSession, data: list, columns: list) -> DataFrame: 
    adidas_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema=schema)
    return adidas_df

def transform_columns(input_df: DataFrame):
    for column in input_df.columns:
        rename_column = re.sub('(\s+)', '_', column.strip())
        input_df = input_df.withColumnRenamed(column, rename_column)
    return input_df

def transform_datetime(input_df: DataFrame) -> DataFrame: 
    output_df = input_df.withColumn('Invoice_Date', F.to_date(F.col('Invoice_Date'), 'M/d/yy'))
    return output_df

# find the min and max report_invoices date by store and retail id
def min_max_and_datediff(input_df: DataFrame) -> DataFrame:
    output_df = input_df.groupBy(['Retailer', 'Retailer_ID', 'Region']).agg(
                                         F.min(F.col('Invoice_Date')).alias('min_date'), 
                                         F.max(F.col('Invoice_Date')).alias('max_date'),
                                         F.datediff(F.max(F.col('Invoice_Date')), F.min(F.col('Invoice_Date'))).alias('day_difference')
                                         ).orderBy(F.col('day_difference').desc())

    return output_df    