import cpp_master_v2 as master
import logging
from dotenv import load_dotenv
from database import Database
from database import Analysis
import os

# Load the environment variables from the .env file
load_dotenv()

master.setup_logging(logging.DEBUG)
cpp_config = master.load_cpp_config()
# connection, cursor = master.connect_db(cpp_config)
# sub_analysis = master.get_sub_analysis_from_db(cursor, 9407)
# analyis = master.get_analysis_from_db(cursor, 6851)
# master.prepare_analysis_cellprofiler_dardel(sub_analysis, cursor)
# logging.info(sub_analysis)

master.init_new_db(cpp_config)
analysis = Database.get_instance().get_analysis_from_sub_id(9504)
master.merge_family_jobs_csv_to_parquet(analysis, Database.get_instance())