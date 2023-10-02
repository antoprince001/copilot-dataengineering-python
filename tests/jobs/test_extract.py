from src.jobs.extract import read_csv_file


def test_read_csv_file(spark_session):
    input_path = 'data/raw/ecommerce_data.csv'

    df = read_csv_file(spark_session, input_path)

    assert df.count() == 51290
    assert len(df.columns) == 8
    assert df.columns == ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
