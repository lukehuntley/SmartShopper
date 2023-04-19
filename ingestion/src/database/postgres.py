from sqlalchemy import create_engine
from sqlalchemy.engine import URL

class PostgresDB():

    def __init__(self, db_user, db_password, db_server_name, db_database_name):
        self.db_user = db_user
        self.db_password = db_password
        self.db_server_name = db_server_name
        self.db_database_name = db_database_name        

    def create_pg_engine(self):
        """
        create an engine to either `source` or `target`
        """
        # drivername = "postgresql+pg8000", 
        # create connection to database self.
        connection_url = URL.create(
            # drivername = "postgresql+psycopg2", 
            drivername = "postgresql+pg8000", 
            username = self.db_user,
            password = self.db_password,
            host = self.db_server_name, 
            port = 5432,
            database = self.db_database_name, 
        )

        # engine = create_engine(connection_url, echo=True)
        engine = create_engine(connection_url)
        return engine 