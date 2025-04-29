#Functions for interacting with the database

#Import packages
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, text, insert

#Establish a connection to the database
def db_connect(dsn, database):
    
    #Create Connection String
    conn_str = (f"mssql+pyodbc:///"
                f"?odbc_connect=DSN={dsn};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;")
    
    #Create SQL Alchemy Engine object
    engine = create_engine(conn_str, use_setinputsizes=False)
    return engine

def load_data_from_query(engine, query):
    with engine.connect() as con:
        res = pd.read_sql_query(query, con)

    return res

#Function to load data
def load_data(settings, src, src_type="sql", engine=None):
    #Handle SQL data sources
    if src_type == "sql":
        #Load .sql file
        with open(settings[src]) as file:
            query_sql = text(file.read())
        
        #Execute query
        return load_data_from_query(engine, query_sql)
    
    #Execute a SFW string
    elif src_type == "query":
        return load_data_from_query(engine, text(src))
    
    #Handle csv data sources
    elif src_type == "csv":
        #Return file contents
        return pd.read_csv(settings[src])

#Function to upload data for a given dataset
def upload_data(df, engine, settings, batch_size=200):

    #Convert float columns to objects (Needed for dealing with NaNs)
    df = df.astype(object).where(pd.notnull(df), None)

    #Load destination table name
    sql_database = settings["sql_database"]
    sql_schema = settings["sql_destination_schema"]
    sql_table = settings["sql_destination_table"]
    
    rows = df.to_dict(orient="records")

    #Check if the destination table already exists - now using EXISTS
    query_exists = text(f"""
        SELECT CASE WHEN EXISTS (
            SELECT 1 FROM sys.schemas AS s 
            INNER JOIN sys.tables AS t ON s.schema_id = t.schema_id 
            WHERE s.name = '{sql_schema}' AND t.name = '{sql_table}'
        ) THEN 1 ELSE 0 END
    """)

    with engine.connect() as con:
        #Set the database
        con.execute(text(f"USE {sql_database};"))

        #Check if the table already exists
        table_exists = con.execute(query_exists).scalar()  # More direct boolean result

        #Is the table does not exist, create it
        if not table_exists:
            with open(settings["sql_createtable"]) as file:
                create_query = text(file.read())
            con.execute(create_query)
        
            #Commit changes
            con.commit()

    #Bind the metadata to the destination table
    metadata = MetaData()
    sqlalc_table = Table(sql_table, metadata, 
                            schema=sql_schema, autoload_with=engine)

    # Single transaction for truncate and insert to prevent empty table if inserts fail
    with engine.begin() as con:  # Using begin() instead of connect() for transaction
        #If the destination table existed previously truncate it
        if table_exists:  # Using the boolean from earlier
            #Create delete query to remove existing data for this data point
            del_query = (f"TRUNCATE TABLE "
                        f"[{sql_database}].[{sql_schema}].[{sql_table}] ")
    
            #Delete existing data from the destination
            con.execute(text(del_query))

        #Group the data into batches and upload
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            result = con.execute(
                insert(sqlalc_table),
                batch
            )

def execute_query(engine, query):
    with engine.connect() as con:
        con.execute(text(query))
        con.commit()