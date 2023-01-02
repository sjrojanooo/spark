from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from etl import adidas_transformations, file_transformations
import os

def main():
    spark = SparkSession.builder.master('local[*]').appName('AdidasSales')\
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                        .getOrCreate()

    # read file in data directory 
    project_dir = os.getcwd()
    adidas_zip_file = file_transformations.extract_zipfile(project_dir)
    target_file = file_transformations.extract_and_return_target_file(adidas_zip_file)
    file_transformations.copy_file_to_processed(target_file)
    # adidas_df = adidas_transformations.create_dataframe(spark, data, columns)
    # adidas_df = adidas_transformations.transform_datetime(adidas_df)
    # adidas_df.cache()
    # adidas_df.printSchema()


if __name__ == '__main__':
    main()