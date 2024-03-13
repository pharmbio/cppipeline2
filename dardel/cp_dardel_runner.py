#!/usr/bin/env python3
import logging
import os
import time
import traceback
import subprocess
import concurrent.futures
import re



import csv

# Global list to track active subprocesses
active_processes = []

def sync_output_dir_to_remote(local_path):
    logging.info(f"inside sync_output_dir_to_remote local_path {local_path}")
    from_local_path = local_path.rstrip('/')
    remote_path = "/share/" #os.path.join("/share/", os.path.dirname(from_local_path))
    to_remote_path = f"guestserver:{remote_path}"
    logging.info(f"to_remote_path {to_remote_path}")
    sync_with_rsync_relative(from_local_path, to_remote_path)

def sync_input_dir(input_dir):
    from_remote_path = f"guestserver:/share{input_dir}/*"
    to_local_path = input_dir
    sync_with_rsync(from_remote_path, to_local_path)

def sync_pipelines_dir(results_dir):
    from_remote_path = f"guestserver:/share{results_dir}/*"
    to_local_path = results_dir
    sync_with_rsync(from_remote_path, to_local_path)

def sync_with_rsync(from_path, to_path):
    # Prepare the rsync command
    rsync_command = ["rsync", "-av", from_path, to_path]

    logging.info(f"rsync_command {rsync_command}")

    # make sure to_path path exists
    if not os.path.exists(to_path):
        os.makedirs(to_path, exist_ok=True)

    # Execute the command
    result = subprocess.run(rsync_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logging.info(result.stdout.decode())

def sync_with_rsync_relative(from_path, to_path):
    # Prepare the rsync command
    rsync_command = ["rsync", "-av","--relative", "--no-perms", "--omit-dir-times", from_path, to_path]

    logging.info(f"rsync_command {rsync_command}")

    # Execute the command
    result = subprocess.run(rsync_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logging.info(result.stdout.decode())


def parse_images_from_datafile(file_path):
    pathname_data = []
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            for key, value in row.items():
                if key.startswith("URL_"):
                    pathname_entry = value
                    pathname_entry = pathname_entry[5:] # remove "file:" prefix
                    pathname_data.append(pathname_entry)
    return pathname_data

def stage_images(image_list):
    logging.info(f"inside stage_images")
    target_directory = "/share/"
    remote_ssh_host_config = "guestserver"
    stage_files_with_scp(image_list, target_directory, remote_ssh_host_config)
    logging.info(f"done stage_images")

def stage_files_with_scp(pathname_list, target_directory, remote_ssh_host_config):
    logging.info(f"inside stage_files_with_scp, target {target_directory} len(pathname_list) {len(pathname_list)}")
    if not os.path.exists(target_directory):
        os.makedirs(target_directory, exist_ok=True)

    for image_path in pathname_list:
        local_path = os.path.join(target_directory, image_path) # save to local stage folder and preserve full filename
        remote_file_path = f"{remote_ssh_host_config}:{image_path}"

        # Ensure the subdirectory structure for the local path exists
        local_subdir = os.path.dirname(local_path)
        #logging.debug(f"local_subdir {local_subdir}")
        if not os.path.exists(local_subdir):
            os.makedirs(local_subdir)

        # check if file is there already
        if not os.path.exists(local_path):
            try:
                cmd_w_args = ["scp", remote_file_path, local_path]
                subprocess.run(cmd_w_args, check=True)
                logging.debug(f"Downloaded '{local_path}' to '{target_directory}'.")
            except subprocess.CalledProcessError as e:
                logging.error(f"Failed run cmd_w_args {cmd_w_args}", e)
                raise(e)
        else:
            logging.debug(f"File '{local_path}' already exists in '{target_directory}'.")

    logging.info("Done stage_files_with_scp")


def set_permissions_recursive(path, permissions=0o777):
    logging.info(f"Inside set_permissions_recursive {path}")

    #set permissions for the input directory itself
    os.chmod(path, permissions)

    for dirpath, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            os.chmod(os.path.join(dirpath, dirname), permissions)
        for filename in filenames:
            os.chmod(os.path.join(dirpath, filename), permissions)
    logging.info(f"Done set_permissions_recursive {path}")


def run_cmd(cmd):
    """
    Execute a command (usually done in a separate thread)

    Args:
        cmd (str): The command line string to be executed by the system.

    """

    logging.info(f"run_cmd {cmd}")

    output_dir = None
    try:
        if cmd is None or cmd.isspace():
            logging.debug(f"return becatce cmd is None or cmd.isspace()")
            return

        # get parameters from cmd
        parts = cmd.split()
        index_data_file = parts.index('--data-file') + 1
        data_file = parts[index_data_file]

        index_output_dir = parts.index('-o') + 1
        output_dir = parts[index_output_dir]

        # set up logging
        log_file_path = os.path.join(output_dir, 'cp.log')

        # get images from this commands datafile
        image_list = parse_images_from_datafile(data_file)
        logging.debug(f'image_list {image_list}')

        # stage images from this command
        stage_images(image_list)

        # check if finished file exists, then skip this command
        if os.path.exists(os.path.join(output_dir, "finished")):
            logging.debug(f"finished file exist, skip this job")
        else:
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            os.chmod(output_dir, 0o0777)

            # Create and track the subprocess
            with open(log_file_path, 'w') as log_file:
                logging.info(f"subprocess.run cmd = {cmd}")
                #cmd = "echo hello"
                proc_result = subprocess.run(cmd, shell=True, stdout=log_file, stderr=subprocess.STDOUT)

            # Check the exit code of the subprocess
            if proc_result.returncode != 0:
                with open(os.path.join(output_dir, 'error'), 'w') as file:
                    file.write(f'cmd: {cmd}')  # Writing command to the error file
                    raise Exception("There was an exception in cellprofiler subprocess")

            # Write a finished flag file
            with open(os.path.join(output_dir, 'finished'), 'w') as file:
                file.write('')  # Writing an empty string to create the file

    except Exception as e:
        logging.error(f"Exception in run_cmd running command: {cmd} ", e)
        raise(e)

    finally:
        # synch output dir
        if output_dir:
            logging.info("finally synch output dir")
            set_permissions_recursive(output_dir, 0o777)
            sync_output_dir_to_remote(output_dir)

# Configure logging for the main script
logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

try:

    rootLogger = logging.getLogger()
    start_time = time.time()

    output_dir = os.environ.get('OUTPUT_DIR')
    logging.info(f"output_dir {output_dir}")

    input_dir = os.environ.get('INPUT_DIR')
    logging.info(f"input_dir {input_dir}")

    results_dir = os.environ.get('RESULT_DIR')
    logging.info(f"results_dir {results_dir}")

    max_workers = int(os.environ.get('MAX_WORKERS', default=16))
    logging.info(f"max_workers {max_workers}")

    max_errors = int(os.environ.get('MAX_ERRORS', default=5))
    logging.info(f"max_errors {max_errors}")

    logging.info(f"OMP_NUM_THREADS {os.environ.get('OMP_NUM_THREADS')}")

    pipelines_dir = "/cpp_work/pipelines"

    # sync input dir
    sync_input_dir(input_dir)

    # sync pipelines dir
    sync_pipelines_dir(pipelines_dir)

    # read all cellprofiler commands to be executed
    with open(f'{input_dir}/cmds.txt', 'r') as file:
        cmds = file.readlines()

    logging.info(f"all cmds: {cmds}")

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_cmd, cmd): cmd for cmd in cmds}

        pending = len(futures)
        finished = 0
        errors = 0

        for future in concurrent.futures.as_completed(futures):
            cmd = futures[future]
            try:
                result = future.result()  # You can capture the result if needed
                finished += 1
            except Exception as e:
                logging.error(f"Error running command {cmd}", e)
                errors += 1

            pending -= 1
            logging.info(f"Pending: {pending}, Finished: {finished}, Errors: {errors}")

            if errors > max_errors:
                raise Exception(f"There are more errors than max_errors, errors {errors}")

    # Need to set rw to files since also modified by cp_master process
    set_permissions_recursive(output_dir, 0o777)

    logging.info(f"elapsed: {time.time() - start_time} sek")

except Exception as e:
    logging.error(f"Exception out of script ", e)

    # Cleanup: terminate all active subprocesses
    for p in active_processes:
        p.terminate()
