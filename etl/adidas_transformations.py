from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
import re

# rename all columns and substitue and an underscore in between words an white space
def transform_columns(adidas_df: DataFrame):
    for column in adidas_df.columns:
        rename_column = re.sub('(\\s+)', '_', column.lower())
        adidas_df = adidas_df.withColumnRenamed(column, rename_column.strip())
    return adidas_df

# replace all literal type characters and replace them with an empty string
def transform_literal_types(adidas_df: DataFrame, list_o_columns: list) -> DataFrame:
    for column in list_o_columns: 
        adidas_df = adidas_df.withColumn(column, F.regexp_replace(column, '([$,%])', '').cast('double'))
    return adidas_df   

# transform invoice date to to a datetime column type 
def transform_datetime(input_df: DataFrame) -> DataFrame: 
    output_df = input_df.withColumn('invoice_date', F.to_date(F.col('invoice_date'), 'M/d/yy'))
    return output_df

# aggregate metric for adidas retails 
def min_max_and_datediff(input_df: DataFrame) -> DataFrame:
    output_df = input_df.groupBy(['retailer', 'retailer_id', 'region']).agg(
                                         F.min(F.col('invoice_date')).alias('min_date'), 
                                         F.max(F.col('invoice_date')).alias('max_date'),
                                         F.datediff(F.max(F.col('invoice_date')), F.min(F.col('invoice_date'))).alias('day_in_sales'),
                                         F.sum(F.col('units_sold')).alias('total_units_sold'),
                                         F.sum(F.col('total_sales')).alias('total_sales'), 
                                         F.sum(F.col('operating_profit')).alias('total_operating_profit')
                                         ).orderBy(F.col('day_in_sales').desc())

    return output_df 

# window function to sum aggregated columns by region
def sum_by_region(column: str) -> DataFrame: 
    return F.sum(column).over(Window.partitionBy('region')).alias(f'{column}_by_region')

# loop through producd aggregated columns and call the window function 
# get the percentage of each that each retailer takes up. 
def sum_values_by_region(retail_summary: DataFrame) -> DataFrame: 
    for column in ['total_units_sold', 'total_sales', 'total_operating_profit']: 
        retail_summary = retail_summary.select('*', sum_by_region(column))\
                                        .withColumn(f'percentage_of_{column}', F.expr(f"Round({column}/{column}_by_region, 4)"))
    return retail_summary