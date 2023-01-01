from pyspark.sql import DataFrame, SparkSession

def create_dataframe(spark: SparkSession, data: list, schema: list) -> DataFrame: 
    adidas_df = spark.createDataFrame(data, schema=schema)
    return adidas_df

