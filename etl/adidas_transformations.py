from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F, types as T
import re

schema = T.StructType([
    T.StructField('Retailer', T.StringType(), True),
    T.StructField('Retailer_ID', T.StringType(), True),
    T.StructField('Invoice_Date', T.StringType(), True),
    T.StructField('Region', T.StringType(), True),
    T.StructField('State', T.StringType(), True),
    T.StructField('City', T.StringType(), True),
    T.StructField('Product', T.StringType(), True),
    T.StructField('Price_per_Unit', T.StringType(), True),
    T.StructField('Units_Sold', T.StringType(), True),
    T.StructField('Total_Sales', T.StringType(), True),
    T.StructField('Operating_Profit', T.StringType(), True),
    T.StructField('Operating_Margin', T.StringType(), True),
    T.StructField('Sales_Method', T.StringType(), True),
])

def create_dataframe(spark: SparkSession, data: list, columns: list) -> DataFrame: 
    adidas_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema=schema)
    return adidas_df

def transform_datetime(input_df: DataFrame) -> DataFrame: 
    output_df = input_df.withColumn('Invoice_Date', F.to_date(F.col('Invoice_Date'), 'M/d/yy'))
    return output_df

# # find the min and max report_invoices date by store and retail id
# # what are the daydiffs between the two
# def min_max_and_datediff(input_df: DataFrame) -> DataFrame:
#     output_df = input_df.groupBy(['Retailer', 'Retailer_ID']).agg(F.min(F.col('Invoice_Date')), 
#                                          F.max(F.col('Invoice_Date'))
#                                          )

#     return output_df    