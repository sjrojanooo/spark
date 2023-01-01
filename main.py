from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from etl import adidas_transformations, file_transformations

def main():
    spark = SparkSession.builder.master('local[*]').appName('AdidasSales')\
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                        .getOrCreate()

    # read file in data directory 
    adidas_file = file_transformations.return_zipfile('./data')
    in_memory_data = file_transformations.unzip_in_memory(adidas_file)
    data, columns = file_transformations.clean_data_and_columns(in_memory_data)
    adidas_df = adidas_transformations.create_dataframe(spark, data, columns)
    adidas_df = adidas_transformations.transform_datetime(adidas_df)
    adidas_df.show()
    
if __name__ == '__main__':
    main()