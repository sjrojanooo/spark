from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from etl import adidas_transformations, file_transformations
import re
import os

def main():
    spark = SparkSession.builder.master('local[*]').appName('AdidasSales')\
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                        .getOrCreate()

    # read file in data directory 
    project_dir = os.getcwd()
    adidas_zip_file = file_transformations.extract_zipfile(project_dir)
    target_file = file_transformations.extract_and_return_target_file(adidas_zip_file)
    file_transformations.move_and_rename_file(target_file)
    # read csv and generate dataframe
    adidas_sales = spark.read.csv('./data/sales/adidas_us_retail_sales_data.csv', sep=',', header=True)
    adidas_sales = adidas_transformations.transform_columns(adidas_sales)
    adidas_sales = adidas_transformations.transform_datetime(adidas_sales)
    adidas_sales = adidas_transformations.min_max_and_datediff(adidas_sales)
    adidas_sales.show(truncate=False)
    


if __name__ == '__main__':
    main()