import logging
import os
import threading
import time
from database import Database
from database import Analysis
import hpc_utils
import job_utils
import cpp_server as cpp_server
from config import CONFIG
from file_sync_utils import SyncManager
import error_utils


def test_input_dir_synch():
    uppmax_cfg = CONFIG.cluster.get('rackham')
    assert uppmax_cfg is not None, "uppmax_cfg is None"
    sync_manager = SyncManager(uppmax_cfg.get('user'), uppmax_cfg.get('hostname'), uppmax_cfg.get('work_dir'))
    analysis = Database.get_instance().get_analysis(12893)
    assert analysis is not None, "analysis missing"
    sync_manager.sync_input_dir(analysis.sub_id)

# .env is loaded centrally in config.py via python-dotenv

cpp_server.setup_logging(logging.INFO)
cpp_server.init_new_db()
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
#print(f"config: {config}")

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
#analysis = Database.get_instance().get_analysis()
#cpp_server.prepare_analysis_cellprofiler_hpc(analysis)

# Test 10.5
# def test_prepare_all_analyses():
#     analyses = Database.get_instance().get_new_analyses()
#     for analysis in analyses:
#         if analysis.is_cellprofiler_analysis:
#             if analysis.is_run_on_hpcdev:
#                 logging.debug(f'is hpc dev: {analysis.id}')
#                 # only run analyses that have satisfied  dependencies
#                 if analysis.all_dependencies_satisfied():
#                     logging.debug(f'all deps satisfied: {analysis.id}')
#                     cmd = cpp_server.prepare_analysis_cellprofiler_hpc(analysis)
#                     logging.debug(f'cmd: {cmd}')


# Test 11
#analysis = Database.get_instance().get_analysis(13744)
#cpp_server.handle_analysis_cellprofiler_hpc(analysis)

# smaller analyses: 15118 full: 15164

###Test 12
#analysis = Database.get_instance().get_analysis(15320) # 15307 4.2.8 feat
#if analysis:
#     cpp_server.handle_analysis_cellprofiler_hpc(analysis)

# Test 13
#analysis = Database.get_instance().get_analysis(15530)
#if analysis:
#    run_location="pelle"
#    cmd = hpc_utils.build_ssh_cmd_sbatch_hpc(analysis, run_location=run_location)
#    logging.info(f"cmd: {cmd}")


# Test 14: move_job_results_to_storage for sub_id 15171
#analysis = Database.get_instance().get_analysis(15320)
#if analysis:
#    jobs, n_jobs = job_utils.get_job_list(analysis, finished_only=True, exclude_errors=True)
#   logging.info(f"n_jobs parsed: {n_jobs}, finished jobs: {len(jobs)}")
#    files_created = cpp_server.move_job_results_to_storage(analysis, jobs)
#    logging.info(f"files_created: {files_created}")


# Test 15: pretty-print finished families summary
# result = cpp_server.fetch_finished_job_families_hpc()
# if not result:
#     logging.info("No finished job families found")
# else:
#     logging.info(f"Finished families: {len(result)}")
#     for family_name, jobs in result.items():
#         logging.info(f"- {family_name}: {len(jobs)} jobs")
#         # Show up to first 5 job names for readability
#         for job in jobs:
#             logging.info(f"   • {job['metadata']['name']}")


# Test 16: run_server_processing() single iteration
#cpp_server.run_server_processing()

# Test 17
#hpc_utils.update_hpc_job_status_all()

# Test 18
#analysis = Database.get_instance().get_analysis(15320)
#if analysis:
#    cpp_server.insert_sub_analysis_results_to_db(analysis)

# Test 19
#cpp_server.fetch_finished_subanalyses_hpc()

# Test 20: run_server_processing() single iteration
cpp_server.run_server_loop_continously()


# Optional: send a test Slack error message once when env var is set
def test_send_slack_error_message():
    title = "Test: Collapsed Slack Error"
    msg = (
        """
        Test Error message: 2025-11-07 08:10:04,253,253 ERROR    [cpp_server.py:917] run_server_processing failed
Traceback (most recent call last):
  File "/home/anders/projekt/cppipeline2/server/cpp_server.py", line 910, in run_server_processing
    hpc_utils.update_hpc_job_status(CLUSTER)
  File "/home/anders/projekt/cppipeline2/server/hpc_utils.py", line 480, in update_hpc_job_status
    sacct_output = exec_ssh_cmd(sacct_cmd)
  File "/home/anders/projekt/cppipeline2/server/hpc_utils.py", line 301, in exec_ssh_cmd
    output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
  File "/usr/lib/python3.10/subprocess.py", line 421, in check_output
    return run(*popenargs, stdout=PIPE, timeout=timeout, check=True,
    """)

    ctx = {"sub_id": 123, "analysis_id": 456, "note": "slack-test"}
    # Use log_error to include context and exercise Block Kit formatting
    error_utils.log_error(title=title, err=msg, context=ctx, also_raise=False)


#test_send_slack_error_message()

