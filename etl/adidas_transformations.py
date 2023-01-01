from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date

def create_dataframe(spark: SparkSession, data: list, columns: list) -> DataFrame: 
    adidas_df = spark.createDataFrame(data, columns)
    return adidas_df

def transform_datetime(input_df: DataFrame) -> DataFrame: 
    output_df = input_df.withColumn('Invoice_Date', to_date(col('Invoice_Date'), 'M/d/yy'))
    return output_df