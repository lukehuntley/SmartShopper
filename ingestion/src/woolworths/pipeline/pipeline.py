from graphlib import TopologicalSorter
from io import StringIO
from woolworths.etl.extract import Extract
from woolworths.etl.load import Load
from database.postgres import PostgresDB
from utility.metadata_logging import MetadataLogging
import datetime as dt
import pandas as pd
import os 
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
    # path_transform_model = config["transform"]["model_path"]
    category_url=config['extract']['category_url']
    product_url=config['extract']['product_url']    
    schema_name=config['extract']['schema_name']
    load_method=config['load']['load_method']

    logging.info(f'cageogry url: {category_url}')


    # logging.info("Getting env variables")       
    # target_db_user = os.environ.get("target_db_user")
    # target_db_password = os.environ.get("target_db_password")
    # target_db_server_name = os.environ.get("target_db_server_name")
    # target_db_database_name = os.environ.get("target_db_database_name")
    target_db_user = 'postgres'
    target_db_password = 'postgres'
    target_db_server_name = 'localhost'
    target_db_database_name = 'smartshopper'   


    # The below is for confriming the .env file is read correctly
    # print(f'targer_db_user: {target_db_user}')
    # print(f'targer_db_password: {target_db_password}')  
    # print(f'targer_db_server_name: {target_db_server_name}')  
    # print(f'targer_db_database_name: {target_db_database_name}')     

    logging.info("Creating target database engine")
    # set up target db engine     
    te = PostgresDB(db_user=target_db_user, db_password=target_db_password, db_server_name=target_db_server_name, db_database_name=target_db_database_name)
    target_engine = te.create_pg_engine()

    logging.info("Setting up metadata logger")
    # set up metadata logger 
    metadata_logger = MetadataLogging(engine=target_engine)    
    # metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)
    metadata_log_run_id = 1
    
    try:

        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Started",
            run_id=metadata_log_run_id, 
            run_config=config,
            db_table=metadata_log_table
        )              

        logging.info("Running extract to get products")
        products_object = Extract(category_url=category_url, product_url=product_url)
        list_of_product_df = products_object.run()     

        node_load_prodcts_list  = []

        # Loop through list of product df and create load nodes list
        for df in list_of_product_df:
            category_table_name = df['Category'].unique()[0]
            table_name = f'raw_{category_table_name}Products'
            node_load_prodcts_list.append(Load(df=df, engine=target_engine, schema_name=schema_name, table_name=table_name, load_method=load_method))

        # logging.info("Creating transform nodes")
        # # transform nodes  
        # node_staging_forecast = Transform(model="staging_forecast", engine=target_engine, models_path=path_transform_model)
        # node_staging_historic = Transform(model="staging_historic", engine=target_engine, models_path=path_transform_model)
        # node_staging_historic_and_forecast = Transform(model="staging_historic_and_forecast", engine=target_engine, models_path=path_transform_model)
        # node_serving_activities = Transform(model="serving_activities", source_table = 'staging_historic_and_forecast', engine=target_engine, models_path=path_transform_model)
        # node_serving_rank_city_metrics_by_day = Transform(model="serving_rank_city_metrics_by_day", source_table = 'staging_historic_and_forecast', engine=target_engine, models_path=path_transform_model)
        # node_serving_rank_city_metrics_by_month = Transform(model="serving_rank_city_metrics_by_month", source_table = 'staging_historic_and_forecast', engine=target_engine, models_path=path_transform_model)
        # node_serving_weather_condition_count_by_month = Transform(model="serving_weather_condition_count_by_month", source_table = 'staging_historic', engine=target_engine, models_path=path_transform_model)
        # node_serving_weather_condition_count_by_year = Transform(model="serving_weather_condition_count_by_year", source_table = 'staging_historic', engine=target_engine, models_path=path_transform_model)
        
        # Build dag
        dag = TopologicalSorter()

        logging.info("Adding dag nodes")
        # Adding load nodes 
        for item in node_load_prodcts_list:
            dag.add(item)
        
        # # adding trasform staging nodes 
        # dag.add(node_staging_forecast)
        # dag.add(node_staging_historic)        
        # dag.add(node_staging_historic_and_forecast, node_staging_forecast, node_staging_historic)

        # # adding transform serving nodes 
        # dag.add(node_serving_activities, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)
        # dag.add(node_serving_rank_city_metrics_by_day, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)
        # dag.add(node_serving_rank_city_metrics_by_month, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)
        # dag.add(node_serving_weather_condition_count_by_month, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)
        # dag.add(node_serving_weather_condition_count_by_year, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)

        logging.info("Executing DAG")
        # Run dag 
        dag_rendered = tuple(dag.static_order())

        for node in dag_rendered: 
            node.run()

        logging.info("Pipeline run successful")
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Completed",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )

    except Exception as e: 
        logging.exception(e)
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Error",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )

    print(run_log.getvalue())

if __name__ == "__main__":
    run_pipeline()