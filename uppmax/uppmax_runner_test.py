import os
import sys
import logging
import time
import cp_uppmax_runner
from cp_uppmax_runner import RunnerConfig

required_version = (3, 12, 7)
if sys.version_info < required_version:
   print("- Make sure to load python version, e.g. \n"
     "module load python3/3.12.7\n"
     "- And then load venv\n"
     "source venv/bin/activate\n\n")
   sys.exit("Wrong python version")


# Configure logging
logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%y-%m-%d %H:%M:%S',
    level=logging.DEBUG
)

sub_id = 13744
cfg =  RunnerConfig(
        stage_root_dir     = "/proj/cellprofiling/nobackup/stage",
        input_dir          = f"/proj/cellprofiling/nobackup/cpp_work/input/{sub_id}",
        remote_input_dir   = f"/cpp_work/input/{sub_id}",
        output_dir         = f"/proj/cellprofiling/nobackup/cpp_work/output/{sub_id}",
        remote_output_dir  = f"/cpp_work/output/{sub_id}",
        cpp_dir            = "/proj/cellprofiling/nobackup/cpp_work",
        remote_cpp_dir     = "/cpp_work/",
        max_workers        = 1,
        max_errors         = 1,
        omp_threads        = 1,
    )

# cfg =  RunnerConfig(
#         sub_id =           = f"{sub_id}",
#         image_stage_dir    = f"/cpp_work/output/{sub_id}",
#         cpp_dir            = "/proj/cellprofiling/nobackup/cpp_work",
#         remote_cpp_dir     = "/cpp_work",
#         max_workers        = 1,
#         max_errors         = 1,
#         omp_threads        = 1,
#     )


start_time = time.time()


#cp_uppmax_runner.sync_pipelines_dir(f"{local_dir_prefix}{pipelines_dir}", pipelines_dir)
#cp_uppmax_runner.sync_input_dir(f"{local_dir_prefix}{input_dir}", input_dir)
#cp_uppmax_runner.stage_images_from_file_list(f'{local_dir_prefix}{input_dir}/stage_images.txt')
cp_uppmax_runner.testrun1(cfg)
cp_uppmax_runner.run_pipeline(cfg)
