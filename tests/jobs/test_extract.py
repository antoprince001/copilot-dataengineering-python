from src.jobs.extract import read_csv_file


def test_read_csv_file(spark_session):
    input_path = 'data_source/raw/ecommerce_data.csv'

    df = read_csv_file(spark_session, input_path)

    assert df.count() == 51290
    assert len(df.columns) == 16
    assert df.columns == ['Order_Date', 'Time', 'Aging', 'Customer_Id',
                          'Gender', 'Device_Type', 'Customer_Login_type',
                          'Product_Category', 'Product', 'Sales', 'Quantity',
                          'Discount', 'Profit', 'Shipping_Cost',
                          'Order_Priority', 'Payment_method']
