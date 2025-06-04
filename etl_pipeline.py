from sqlalchemy.engine.base import Engine
from pandas.core.frame import DataFrame
import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime
import pytz

def extract(
    db_engine: Engine, 
    table_name: str
) -> DataFrame:
    # Connect to the database
    conn = db_engine.connect()

    # Read data from a table into a DataFrame
    df = pd.read_sql_table(table_name=table_name, con=conn)

    # close connection
    conn.close()

    # return data as DataFrame
    return df

def load(
    df: DataFrame,
    table_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    s3_path: dict
) -> bool:
    
    # create s3 client using boto3 + credentials
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # upload result to s3 as parquet file
    today = datetime.now(tz=pytz.timezone('Australia/Adelaide'))
    year, month, day = today.strftime('%Y'), today.strftime('%m'), today.strftime('%d')

    # Upload file to folder
    file_path = f"{s3_path}/year={year}/month={month}/day={day}/{table_name}.parquet"

    wr.s3.to_parquet(
        df=df,
        path=file_path,
        boto3_session=session
    )
    print(f'Data successfully loaded into: {file_path}\n')