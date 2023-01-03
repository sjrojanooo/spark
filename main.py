import os
from pyspark.sql import SparkSession
from etl import adidas_transformations, file_transformations

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
    # we cache here so that the spark query plan can make sure to save this dataframe for later use. 
    # spark is lazy and makes sure to execute items from the top down. If we want it to store 
    # an important item for later use, then we can use cache, only one of memory caption options available to us 
    # in the spark sesssion. I plan to use this dataframe throughout, so why not help spark out.
    adidas_sales.cache()
    # where has adidas been selling at the longest? 
    adidas_day_diff = adidas_transformations.min_max_and_datediff(adidas_sales)
    adidas_day_diff.show(truncate=False)
    
if __name__ == '__main__':
    main()