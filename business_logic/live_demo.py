import awswrangler as wr
import pandas as pd


def demo():
    wr.s3.to_parquet(
        df=pd.DataFrame({'col': [1, 2, 100]}),
        path='s3://poc-bucket-oremeta/prefix/my_file.parquet')
    return "Data written to s3."
