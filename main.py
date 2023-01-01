from pyspark.sql import SparkSession, DataFrame
from etl import adidas_transformations

def main():
    spark = SparkSession.builder.master('local[*]').appName('AdidasSales')\
                        .getOrCreate()

    # read file in data directory 
    adidas_file = adidas_transformations.return_zipfile('./data')
    in_memory_data = adidas_transformations.unzip_in_memory(adidas_file)
    data, columns = adidas_transformations.clean_data_and_columns(in_memory_data)
    adidas_df = adidas_transformations.create_dataframe(spark, data, columns)
    adidas_df.show(truncate=False)
if __name__ == '__main__':
    main()