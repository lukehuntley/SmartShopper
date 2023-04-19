# from sqlalchemy import String, DateTime, Boolean, BigInteger, Numeric
# from sqlalchemy import Table, Column, Integer, String, MetaData
# from sqlalchemy.dialects import postgresql
# from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema
from sqlalchemy import text
import pandas as pd
import numpy as np
import logging 

class Load():
   
    def __init__(self, df:pd.DataFrame, engine:str, schema_name:str, table_name:str, load_method:str, chunksize:int=1000):
        self.df=df
        self.engine=engine
        self.schema_name=schema_name
        self.table_name=table_name
        self.chunksize = chunksize
        self.load_method = load_method

    def _create_schema(self, engine, schema_name:str)->None:
        """
        Creates a database schema to group tables together
        - `engine`: connection engine to database 
        - `schema_name` : database schema  
        Returns None
        """

        with engine.connect() as conn:            
            if not conn.dialect.has_schema(conn, schema_name):
                conn.execute(CreateSchema(schema_name))
                conn.commit()
                logging.info(f'Schema [{schema_name}] created')            
            else:
                logging.info(f'Schema [{schema_name}] already exists')
        
        return None
    
    def _insert_in_chunks(self, df:pd.DataFrame, engine, schema_name:str, table_name:str, chunksize:int=1000)->None:
        """
        Performs the insert with several rows at a time (i.e. a chunk of rows)
        - `df`: pandas dataframe 
        - `engine`: connection engine to database 
        - `schema_name: database schema
        - `table_name`: target table        
        - `chunksize`: if chunksize greater than 0 is specified, then the rows will be inserted in the specified chunksize. e.g. 1000 rows at a time
        Returns None
        """

        max_length = len(df)

        for i in range(0, max_length, chunksize):
            if i + chunksize >= max_length: 
                lower_bound = i
                upper_bound = max_length 
            else: 
                lower_bound = i 
                upper_bound = i + chunksize

            chunk_df = df.iloc[lower_bound:upper_bound]

            if i == 0:
                chunk_df.to_sql(name=table_name, con=engine, if_exists="replace", index=False, schema=schema_name)
            else:
                chunk_df.to_sql(name=table_name, con=engine, if_exists="append", index=False, schema=schema_name)
            
            logging.info(f"Inserted chunk: {len(chunk_df)} [{lower_bound}:{upper_bound}] out of index {max_length}")

        return None 

    def _overwrite_to_database(self, df:pd.DataFrame, engine, schema_name:str, table_name:str, chunksize:int=1000)->None:
        """
        Insert dataframe to a database table 
        - `df`: pandas dataframe 
        - `engine`: connection engine to database 
        - `schema_name: database schema
        - `table_name`: target table        
        - `chunksize`: if chunksize greater than 0 is specified, then the rows will be inserted in the specified chunksize. e.g. 1000 rows at a time
        Returns None
        """

        if chunksize > 0:
            self._insert_in_chunks(df=df, engine=engine, schema_name=schema_name, table_name=table_name, chunksize=chunksize)
        else: 
            df.to_sql(df=df, con=engine, schema=schema_name, name=table_name, if_exists="replace", index=False)
        
        logging.info(f"Successful write to table: {table_name}, rows inserted/updated: {len(df)}")

        return None

    def run(self):
        """
        Run load
        """
        # Create schema if not exists
        self._create_schema(engine=self.engine, schema_name=self.schema_name)

        if self.load_method == 'overwrite':
            self._overwrite_to_database(df=self.df, engine=self.engine, schema_name=self.schema_name, table_name=self.table_name, chunksize=self.chunksize)
    