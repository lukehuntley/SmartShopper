from sqlalchemy.schema import CreateSchema
from sqlalchemy import Table, Column, Integer, String, MetaData, JSON
from sqlalchemy import insert, select, func
import datetime as dt 
import logging

class MetadataLogging():

    def __init__(self, engine):
        self.engine = engine
    
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
        
        return None
    
    def _create_logging_table(self, schema_name:str, table_name:str)->Table:
        
        self._create_schema(engine=self.engine, schema_name=schema_name)
        
        meta = MetaData()
        target_table = Table(
            table_name, 
            meta,
            Column("run_timestamp", String, primary_key=True),
            Column("run_id", Integer, primary_key=True),
            Column("run_status", String, primary_key=True),
            Column("run_config", JSON),
            Column("run_log", String),
            schema=schema_name
        )
        meta.create_all(self.engine)
        return target_table 
    
    def get_latest_run_id(self, schema_name:str, table_name:str)->int:
        target_table = self._create_logging_table(schema_name=schema_name, table_name=table_name)
        statement = (
            select(func.max(target_table.c.run_id))
        )

        with self.engine.connect() as conn:
            response = conn.execute(statement).first()[0]
        
        if response is None: 
            return 1 
        else: 
            return response + 1 

    def log(
        self,
        run_timestamp: dt.datetime,
        run_id: int,
        run_config: dict,
        schema_name: str,
        table_name: str,
        run_status: str="started",
        run_log:str="",
    )->bool:
        target_table = self._create_logging_table(schema_name=schema_name, table_name=table_name)
        insert_statement = insert(target_table).values(
            run_timestamp=run_timestamp,
            run_id=run_id,
            run_status=run_status,
            run_config=run_config,
            run_log=run_log
        )

        with self.engine.connect() as conn:
            conn.execute(insert_statement)
            conn.commit()

        return True 