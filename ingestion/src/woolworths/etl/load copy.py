from sqlalchemy import String, DateTime, Boolean, BigInteger, Numeric
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.dialects import postgresql
import pandas as pd
import numpy as np
import logging 
import time

class Load():
   
    def __init__(self, df:pd.DataFrame, engine:str, table_name:str, load_method:str, chunksize:int=1000, key_columns:list=None):
        self.df=df
        self.key_columns=key_columns
        self.engine=engine
        self.table_name=table_name
        self.chunksize = chunksize
        self.load_method = load_method

    def _get_sqlalchemy_column(self, column_name:str , source_datatype:str, primary_key:bool=False)->Column:
        """
        A helper function that returns a SQLAlchemy column by mapping a pandas dataframe datatypes to sqlalchemy datatypes 
        """
        dtype_map = {
            "int64": BigInteger, 
            "object": String, 
            "datetime64[ns]": DateTime, 
            "float64": Numeric,
            "bool": Boolean
        }
        column = Column(column_name, dtype_map[source_datatype], primary_key=primary_key) 
        return column

    def _generate_sqlalchemy_schema(self, df: pd.DataFrame, key_columns:list, table_name, meta): 
        """
        Generates a sqlalchemy table schema that shall be used to create the target table and perform insert/upserts. 
        """
        schema = []
        for column in [{"column_name": col[0], "source_datatype": col[1]} for col in zip(df.columns, [dtype.name for dtype in df.dtypes])]:
            schema.append(self._get_sqlalchemy_column(**column, primary_key=column["column_name"] in key_columns))
        return Table(table_name, meta, *schema)


    def _upsert_in_chunks(self, df:pd.DataFrame, engine, table_schema:Table, key_columns:list, chunksize:int=1000)->bool:
        """
        Performs the upsert with several rows at a time (i.e. a chunk of rows). this is better suited for very large sql statements that need to be broken into several steps. 
        """
        max_length = len(df)
        df = df.replace({np.nan: None})
        for i in range(0, max_length, chunksize):
            if i + chunksize >= max_length: 
                lower_bound = i
                upper_bound = max_length 
            else: 
                lower_bound = i 
                upper_bound = i + chunksize
            insert_statement = postgresql.insert(table_schema).values(df.iloc[lower_bound:upper_bound].to_dict(orient='records'))
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=key_columns,
                set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})            
            # result = engine.execute(upsert_statement)
            # engine.execute(upsert_statement)
            with engine.connect() as conn:
                conn.execute(upsert_statement)
            logging.info(f"Inserted chunk: [{lower_bound}:{upper_bound}] out of index {max_length}")
        return True

    def _insert_in_chunks(self, df:pd.DataFrame, engine, table_schema:Table, key_columns:list, chunksize:int=1000)->bool:
        """
        Performs the insert with several rows at a time (i.e. a chunk of rows). this is better suited for very large sql statements that need to be broken into several steps. 
        """
        max_length = len(df)
        df = df.replace({np.nan: None})
        for i in range(0, max_length, chunksize):
            if i + chunksize >= max_length: 
                lower_bound = i
                upper_bound = max_length 
            else: 
                lower_bound = i 
                upper_bound = i + chunksize
            insert_statement = postgresql.insert(table_schema).values(df.iloc[lower_bound:upper_bound].to_dict(orient='records'))
            # upsert_statement = insert_statement.on_conflict_do_update(
            #     index_elements=key_columns,
            #     set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})            
            # result = engine.execute(upsert_statement)
            # engine.execute(insert_statement)
            with engine.connect() as conn:
                conn.execute(insert_statement)
            logging.info(f"Inserted chunk: [{lower_bound}:{upper_bound}] out of index {max_length}")
        return True  


    def _upsert_all(self, df:pd.DataFrame, engine, table_schema:Table, key_columns:list)->bool:
        """
        Performs the upsert with all rows at once. this may cause timeout issues if the sql statement is very large. 
        """
        insert_statement = postgresql.insert(table_schema).values(df.to_dict(orient='records'))
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
        # result = engine.execute(upsert_statement)

        with self.engine.connect() as conn:
            result = engine.execute(upsert_statement)

        logging.info(f"Insert/updated rows: {result.rowcount}")
        return True 

    def _upsert_to_database(self, df: pd.DataFrame, table_name: str, key_columns:list, engine, chunksize:int=1000)->bool: 
        """
        Upsert dataframe to a database table 
        - `df`: pandas dataframe 
        - `table`: name of the target table 
        - `key_columns`: name of key columns to be used for upserting 
        - `engine`: connection engine to database 
        - `chunksize`: if chunksize greater than 0 is specified, then the rows will be inserted in the specified chunksize. e.g. 1000 rows at a time. 
        """
        meta = MetaData()
        logging.info(f"Generating table schema: {table_name}")
        table_schema = self._generate_sqlalchemy_schema(df=df, key_columns=key_columns,table_name=table_name, meta=meta)
        meta.create_all(engine)
        logging.info(f"Table schema generated: {table_name}")
        logging.info(f"Writing to table: {table_name}")
        if chunksize > 0:
            self._upsert_in_chunks(df=df, engine=engine, table_schema=table_schema, key_columns=key_columns, chunksize=chunksize)
        else: 
            self._upsert_all(df=df, engine=engine, table_schema=table_schema, key_columns=key_columns)
        logging.info(f"Successful write to table: {table_name}")
        return True 

    # def _overwrite_to_database(self, df:pd.DataFrame, table_name:str, engine)->None:
    #     """
    #     Upsert dataframe to a database table 
    #     - `df`: pandas dataframe 
    #     - `table`: name of the target table 
    #     - `engine`: connection engine to database 
    #     """
    #     logging.info(f"Writing to table: {table_name}")
    #     df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    #     logging.info(f"Successful write to table: {table_name}, rows inserted/updated: {len(df)}")

    def _overwrite_to_database(self, df:pd.DataFrame, table_name:str, key_columns:list, engine, chunksize:int=1000)->None:
        """
        Insert dataframe to a database table 
        - `df`: pandas dataframe 
        - `table`: name of the target table 
        - `engine`: connection engine to database 
        - `chunksize`: if chunksize greater than 0 is specified, then the rows will be inserted in the specified chunksize. e.g. 1000 rows at a time.
        """

        meta = MetaData()
        logging.info(f"Generating table schema: {table_name}")
        table_schema = self._generate_sqlalchemy_schema(df=df, key_columns=key_columns,table_name=table_name, meta=meta)

        logging.info(f"Writing to table: {table_name}")
        # df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        if chunksize > 0:
            self._insert_in_chunks(df=df, engine=engine, table_schema=table_schema, key_columns=key_columns, chunksize=chunksize)
        else: 
            df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        logging.info(f"Successful write to table: {table_name}, rows inserted/updated: {len(df)}")

    def run(self):
        if self.load_method == 'overwrite':
            self._overwrite_to_database(df=self.df, table_name=self.table_name, engine=self.engine, key_columns = self.key_columns, chunksize=self.chunksize)
            time.sleep(0.5) # Added to pause before doing next overwrite call
        elif self.load_method == 'upsert':
            self._upsert_to_database(df=self.df, table_name=self.table_name, key_columns=self.key_columns, engine=self.engine, chunksize=self.chunksize)
        
    