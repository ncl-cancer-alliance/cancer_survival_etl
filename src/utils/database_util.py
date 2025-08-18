#Functions for interacting with the database

#Import packages
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, text, insert

from snowflake.connector.pandas_tools import write_pandas

def upload_df(ctx, df, destination, replace=True):

    """
    Function to upload a dataframe to Snowflake.

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - df: Dataframe object
    - destination: Full table name of the destination
    (e.g. DATABASE_NAME.SCHEMA_NAME.TABLE_NAME)
    - replace: If True, the destination is TRUNCATED before uploading new data
    (If the upload fails, the truncation is rollbacked)

    output:
    Returns Boolean value if the upload was successful
    """

    df = df.copy()
    df.reset_index(drop=True, inplace=True)
    #Needed to prevent "null" strings in the destination
    df = df.where(pd.notnull(df), None)

    cur = ctx.cursor()
    destination_segs = destination.split(".")
    success = False

    try:
        if replace:
            cur.execute(f"TRUNCATE TABLE {destination}")

        # Upload DataFrame
        success, nchunks, nrows, _ = write_pandas(
            conn=ctx,
            df=df,
            table_name=destination_segs[2],
            schema=destination_segs[1],
            database=destination_segs[0],
            overwrite=False
        )

        if not success:
            raise Exception("Failed to write DataFrame to Snowflake.")

        print(f"Uploaded {nrows} rows to {destination}")
    except Exception as e:
        print("Data ingestion failed with error:", e)
        cur.execute("ROLLBACK") #Undoes truncation on upload error

    finally:
        cur.close()
    
    return success