import pytest 
from etl import adidas_transformations
from datetime import datetime


@pytest.mark.usefixtures("spark_session")
def test_transform_columns(spark_session):
    df = spark_session.createDataFrame([('1','2','3','4','5')],
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

def test_min_max_and_datediff(spark_session):
    df = spark_session.createDataFrame([('retailer1', '12345678', 'Arizona', datetime(2022,10,1), 1000, 345, 13450),
                                        ('retailer1', '12345678', 'Arizona', datetime(2022,9,1), 1000, 345, 13450),
                                        ('retailer1', '12345678', 'Arizona', datetime(2022,6,1), 1000, 345, 13450)],
                                        ['retailer', 'retailer_id', 'region', 'invoice_date', 'units_sold', 'total_sales','operating_profit'])
    out_df = adidas_transformations.min_max_and_datediff(df)
    values = [r for r in out_df.collect()[0]]
    expected_columns = ['retailer', 'retailer_id', 'region', 'min_date', 'max_date', 'day_in_sales', 'total_units_sold','total_sales', 'total_operating_profit']
    assert out_df.columns == expected_columns
    assert values == ['retailer1', '12345678', 'Arizona', datetime(2022, 6, 1, 0, 0), datetime(2022, 10, 1, 0, 0), 122, 3000, 1035, 40350]

def test_sum_by_region(spark_session): 
    df = spark_session.createDataFrame([('region', 1234589,987654)],
                                        ['region', 'total_sales', 'total_operating_profit'])
    for column in ['total_sales','total_operating_profit']:
        df = df.select('*', adidas_transformations.sum_by_region(column))
    values = [x for x in df.collect()[0]]
    assert values == ['region', 1234589, 987654, 1234589, 987654]
    assert df.columns == ['region','total_sales','total_operating_profit','total_sales_by_region','total_operating_profit_by_region']