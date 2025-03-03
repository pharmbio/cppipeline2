import logging
from dotenv import load_dotenv
from database import Database
from database import Analysis
import hpc_utils
import os
import cpp_server as cpp_server
from config import CONFIG
from file_sync_utils import SyncManager

def test_prepare_all_analyses():
    analyses = Database.get_instance().get_new_analyses()
    for analysis in analyses:
        if analysis.is_cellprofiler_analysis:
            if analysis.is_run_on_hpcdev:
                logging.debug(f'is hpc dev: {analysis.id}')
                # only run analyses that have satisfied dependencies
                if analysis.all_dependencies_satisfied():
                    logging.debug(f'all deps satisfied: {analysis.id}')

                    cmd = cpp_server.prepare_analysis_cellprofiler_hpc(analysis)


def test_input_dir_synch():
    uppmax_cfg = CONFIG.cluster.get('rackham')
    sync_manager = SyncManager(uppmax_cfg.get('user'), uppmax_cfg.get('hostname'), uppmax_cfg.get('work_dir'))
    analysis = Database.get_instance().get_analysis(12893)
    sync_manager.sync_input_dir(analysis.sub_id)

# Load the environment variables from the .env file
load_dotenv()

cpp_server.setup_logging(logging.DEBUG)
cpp_config = cpp_server.load_cpp_config()
cpp_server.init_new_db(cpp_config)
# setup old database
#connection, cursor = master.connect_db(cpp_config)

# Test 1
# sub_analysis = master.get_sub_analysis_from_db(cursor, 9407)
# analyis = master.get_analysis_from_db(cursor, 6851)
# master.prepare_analysis_cellprofiler_dardel(sub_analysis, cursor)
# logging.info(sub_analysis)

## Test 2
#analysis = Database.get_instance().get_analysis_from_sub_id(11436)
#logging.info(f"analysis: {analysis}")
#master.prepare_analysis_cellprofiler_dardel(analysis.raw_data, cursor, connection)

# Test 3
#analysis = Database.get_instance().get_analysis_from_sub_id(10325) # 9809 9877 9492
#master.merge_family_jobs_csv_to_parquet(analysis, Database.get_instance())

# Test 4
#print(f"config: {cpp_config}")

# Test 5
#analysis = Database.get_instance().get_analysis_from_sub_id(7180)
#master.build_ssh_cmd_sbatch_dardel(analysis.analysis_id, analysis.analysis_sub_id, 'icf')

# Test 6
#test_prepare_all_analyses()

# Test 7
#analysis = Database.get_instance().get_analysis(12893)
#cmd = cpp_server.prepare_analysis_cellprofiler_hpc(analysis)
#logging.info(f"cmd: {cmd}")

# Test 8
#test_file_sync()

# Test 9
#z_plane= Database.get_instance().get_middle_z_plane(5643)
#logging.debug(f"zplane: {z_plane}")

# Test 10
analysis = Database.get_instance().get_analysis(12893)
cpp_server.handle_analysis_cellprofiler_hpc(analysis)




