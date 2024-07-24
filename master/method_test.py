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
master.init_new_db(cpp_config)
# setup old database
connection, cursor = master.connect_db(cpp_config)

# Test 1
# sub_analysis = master.get_sub_analysis_from_db(cursor, 9407)
# analyis = master.get_analysis_from_db(cursor, 6851)
# master.prepare_analysis_cellprofiler_dardel(sub_analysis, cursor)
# logging.info(sub_analysis)

# Test 2
#analysis = Database.get_instance().get_analysis_from_sub_id(9905)
#logging.info(f"analysis: {analysis}")
#master.prepare_analysis_cellprofiler_dardel(analysis.raw_data, cursor, connection)

# Test 3
analysis = Database.get_instance().get_analysis_from_sub_id(10325) # 9809 9877 9492
master.merge_family_jobs_csv_to_parquet(analysis, Database.get_instance())

# Test 4
#print(f"config: {cpp_config}")

# Test 5
#analysis = Database.get_instance().get_analysis_from_sub_id(7180)
#master.build_ssh_cmd_sbatch_dardel(analysis.analysis_id, analysis.analysis_sub_id, 'icf')