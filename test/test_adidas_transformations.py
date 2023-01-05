import pytest 
from etl import adidas_transformations


@pytest.mark.usefixtures("spark_session")
def test_transform_columns(spark_session):
    df = spark_session.createDataFrame([('1','2','3','4','4')],
                                        ['col name', 'col name 2', 'col name 3', 'Col Name 4', 'Col name 5'])
    out_df = adidas_transformations.transform_columns(df)
    assert out_df.columns == ['col_name', 'col_name_2', 'col_name_3', 'col_name_4', 'col_name_5']

def test_transform_literal_types(spark_session): 
    df = spark_session.createDataFrame([('1,000', '$1.00', '50%')],
                                        ['comma', 'dollar', 'percentage'])
    out_df = adidas_transformations.transform_literal_types(df, ['comma', 'dollar', 'percentage'])
    vals = [r for r in out_df.collect()[0]]
    assert vals == [1000.0, 1.0, 50.0]
    

def test_transform_datetime(spark_session):
    df = spark_session.createDataFrame([('1/2/20',)], ['invoice_date'])
    out_df = adidas_transformations.transform_datetime(df)
    date = out_df.collect()[0][0].strftime('%Y-%m-%d')
    assert date == '2020-01-02'


# def min_max_and_datediff(input_df: DataFrame) -> DataFrame:
#     output_df = input_df.groupBy(['retailer', 'retailer_id', 'region']).agg(
#                                          F.min(F.col('invoice_date')).alias('min_date'), 
#                                          F.max(F.col('invoice_date')).alias('max_date'),
#                                          F.datediff(F.max(F.col('invoice_date')), F.min(F.col('invoice_date'))).alias('day_in_sales'),
#                                          F.sum(F.col('units_sold')).alias('total_units_sold'),
#                                          F.sum(F.col('total_sales')).alias('total_sales'), 
#                                          F.sum(F.col('operating_profit')).alias('total_operating_profit')
#                                          ).orderBy(F.col('day_in_sales').desc())

#     return output_df 

# def sum_by_region(column: str) -> DataFrame: 
#     return F.sum(column).over(Window.partitionBy('region')).alias(f'{column}_by_region')

# def sum_values_by_region(retail_summary: DataFrame) -> DataFrame: 
#     for column in ['total_units_sold', 'total_sales', 'total_operating_profit']: 
#         retail_summary = retail_summary.select('*', sum_by_region(column))\
#                                         .withColumn(f'percentage_of_{column}', F.expr(f"Round({column}/{column}_by_region, 4)"))
#     return retail_summary