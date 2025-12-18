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
analysis = Database.get_instance().get_analysis(16212)
cmd = cpp_server.prepare_analysis_cellprofiler_hpc(analysis)
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


# # Test 14: move_job_results_to_storage
# analysis = Database.get_instance().get_analysis(15926)
# cluster = "pelle"
# if analysis and analysis.sub_id:
    
#     finished_list, total_jobs = cpp_server.get_list_of_finished_jobs(analysis, finished_only=True, exclude_errors=True)
    
#     logging.info(f"finished_list: {finished_list}")
#     logging.info(f"total_jobs: {total_jobs}")

#     files_created = cpp_server.move_job_results_to_storage(analysis, finished_list)
#     logging.info(f"files_created: {files_created}")
    

    # # Scan for finished sub-analyses (for selected cluster) and finalize them
    # finished_subs = cpp_server.fetch_finished_subanalyses_hpc(cluster)
    # for sub_id, job_list in finished_subs.items():
    #     if sub_id == analysis.sub_id:
    #       if not job_list:
    #           logging.warning(f"Finished sub-analysis '{sub_id}' has empty job list; skipping finalize.")
    #           continue

    #       files_created = cpp_server.move_job_results_to_storage(analysis, jobs)
    #       logging.info(f"files_created: {files_created}")
    
    
    #       cpp_server.move_job_results_to_storage(analysis, job_list)


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
#cpp_server.run_server_loop_continously()


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

def move_missing_files_from_output():
    
    subs_to_move = [
      16108, 16107, 16105, 16058, 16056, 16052, 15981, 15980, 15979, 15972,
      15971, 15970, 15966, 15965, 15964, 15963, 15962, 15961, 15960, 15959,
      15958, 15956, 15955, 15954, 15953, 15952, 15951, 15936, 15935, 15934,
      15933, 15932, 15931, 15930, 15929, 15928, 15927, 15926, 15925, 15924,
      15923, 15922, 15921, 15920, 15919, 15918, 15917, 15916, 15892, 15891,
      15890, 15889, 15888, 15887, 15886, 15885, 15884, 15874, 15873, 15872,
      15871, 15870, 15869, 15868, 15867, 15866, 15862, 15861, 15860, 15859,
      15858, 15857, 15829, 15825, 15824, 15823, 15822, 15821, 15820, 15819,
      15818, 15817, 15816, 15812, 15811, 15810, 15809, 15808, 15807, 15806,
      15798, 15797, 15796, 15795, 15791, 15790, 15789, 15788, 15787, 15786,
      15785, 15784, 15783, 15782, 15778, 15777, 15776, 15775, 15774, 15773,
      15772, 15771, 15767, 15766, 15765, 15764, 15763, 15762, 15761, 15760,
      15756, 15755, 15754, 15753, 15752, 15751, 15750, 15749, 15745, 15744,
      15743, 15742, 15741, 15740, 15739, 15738, 15737, 15736, 15732, 15731,
      15730, 15729, 15728, 15727, 15726, 15725, 15721, 15720, 15719, 15718,
      15717, 15716, 15700, 15699, 15698, 15610, 15609, 15605, 15577, 15576,
      15536, 15535, 15534, 15533, 15530, 15529, 15525, 15524, 15523, 15501,
      15498, 15495, 15494, 15480, 15439, 15438, 15437, 15436, 15397, 15396,
      15393, 15392, 15391, 15390, 15387, 15386, 15382, 15381, 15380, 15375,
      15374
    ]

    for sub_id in subs_to_move:
        analysis = Database.get_instance().get_analysis(sub_id)
        if not analysis or not analysis.sub_id:
            continue

        finished_list, total_jobs = cpp_server.get_list_of_finished_jobs(
            analysis, finished_only=True, exclude_errors=True
        )
        logging.info("sub_id=%s finished_list=%s total_jobs=%s", sub_id, len(finished_list), total_jobs)

        files_created = cpp_server.move_job_results_to_storage(analysis, finished_list)
        logging.info("sub_id=%s files_created=%s", sub_id, len(files_created))

#move_missing_files_from_output()

