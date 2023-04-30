from graphlib import TopologicalSorter
from io import StringIO
from woolworths.etl.extract import Extract
from woolworths.etl.load import Load
from database.postgres import PostgresDB
from utility.metadata_logging import MetadataLogging
import datetime as dt
import logging
import yaml

def run_pipeline():

    # set up logging 
    run_log = StringIO()
    logging.basicConfig(stream=run_log,level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

    logging.info("Reading yaml config file")
    # get config variables
    with open("../config.yaml") as stream:
        config = yaml.safe_load(stream)
    
    logging.info("Getting yaml config variables")
    metadata_log_table = config["meta"]["log_table"]
    category_url=config['extract']['category_url']
    product_url=config['extract']['product_url']    
    schema_name=config['extract']['schema_name']
    load_method=config['load']['load_method']

    # logging.info("Getting env variables")       
    # target_db_user = os.environ.get("target_db_user")
    # target_db_password = os.environ.get("target_db_password")
    # target_db_server_name = os.environ.get("target_db_server_name")
    # target_db_database_name = os.environ.get("target_db_database_name")
    target_db_user = 'postgres'
    target_db_password = 'postgres'
    target_db_server_name = 'localhost'
    target_db_database_name = 'smartshopper'   


    logging.info("Creating target database engine")
    # set up target db engine     
    te = PostgresDB(db_user=target_db_user, db_password=target_db_password, db_server_name=target_db_server_name, db_database_name=target_db_database_name)
    target_engine = te.create_pg_engine()

    logging.info("Setting up metadata logger")
    # set up metadata logger 
    metadata_logger = MetadataLogging(engine=target_engine)    
    metadata_log_run_id = metadata_logger.get_latest_run_id(schema_name=schema_name, table_name=metadata_log_table)
    
    try:

        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Started",
            run_id=metadata_log_run_id, 
            run_config=config,
            schema_name=schema_name,
            table_name=metadata_log_table
        )              

        logging.info("Running extract")
        extract_object = Extract(category_url=category_url, product_url=product_url)
        list_of_product_df = extract_object.run()     

        list_of_load_nodes  = []

        # Loop through list of product df and create load nodes list
        for df in list_of_product_df:
            table_name = f"raw_{df.attrs['name']}Products"
            list_of_load_nodes.append(Load(df=df, engine=target_engine, schema_name=schema_name, table_name=table_name, load_method=load_method, chunksize=2500))

        # Build dag
        dag = TopologicalSorter()

        logging.info("Adding DAG nodes")
        # Adding load nodes 
        for node in list_of_load_nodes:
            dag.add(node)
        
        logging.info("Executing DAG")
        # Run dag 
        dag_rendered = tuple(dag.static_order())

        logging.info("Running load")
        for node in dag_rendered: 
            node.run()

        logging.info("Pipeline run successful")
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Completed",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            schema_name=schema_name,
            table_name=metadata_log_table
        )

    except Exception as e: 
        logging.exception(e)
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Error",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            schema_name=schema_name,
            table_name=metadata_log_table
        )

    print(run_log.getvalue())

if __name__ == "__main__":
    run_pipeline()