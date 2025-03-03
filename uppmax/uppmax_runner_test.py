import os
import logging
import time

import cp_uppmax_runner

# Configure logging
logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%y-%m-%d %H:%M:%S',
    level=logging.DEBUG
)

start_time = time.time()

output_dir = "/cpp_work/output/12893"
input_dir = "/cpp_work/input/12893"
results_dir = "/cpp_work/not_needed/result"
max_workers = 1
max_errors = 1
pipelines_dir = "/cpp_work/pipelines"
cmd_file = f"{input_dir}/cmds.txt"

cp_uppmax_runner.sync_input_dir(f"/tmp{input_dir}", input_dir)

cp_uppmax_runner.sync_pipelines_dir(f"/tmp{pipelines_dir}", pipelines_dir)