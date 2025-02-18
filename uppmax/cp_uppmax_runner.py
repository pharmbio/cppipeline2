#!/usr/bin/env python3
import logging
import os
import time
import subprocess
import concurrent.futures
import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import pathlib

# Global list to track active subprocesses
active_processes = []

class CSVToParquetConverter:
    def __init__(self, input_path, output_path, chunk_size=10000):
        self.input_path = input_path
        self.output_path = output_path
        self.chunk_size = chunk_size

    @staticmethod
    def to32bit(df):
        return df.astype({c: str(df[c].dtype).replace('64', '32') for c in df.columns})

    def csv_to_parquet_chunked(self, csv_file_path, parquet_file_path):
        # Create a Parquet writer
        parquet_writer = None
        schema = None

        pyarrow.set_cpu_count(5)

        # Read the CSV file in chunks
        for chunk in pd.read_csv(csv_file_path, chunksize=self.chunk_size):
            print("processing a new chunk")
            chunk = self.to32bit(chunk)
            # Convert the chunk to a PyArrow Table
            table = pyarrow.Table.from_pandas(chunk)
            print("done df to table")
            if parquet_writer is None:
                # Initialize the Parquet writer with the schema of the first chunk
                schema = table.schema
                parquet_writer = pq.ParquetWriter(parquet_file_path, schema=schema, compression='snappy')

            # Write the table chunk to the Parquet file
            print("before write table")
            parquet_writer.write_table(table)
            print("done write table")

        # Close the Parquet writer if it was initialized
        if parquet_writer:
            parquet_writer.close()

    def merge_csv_to_parquet(self):
        logging.info("Inside merge_csv_to_parquet")

        all_csv_files = pathlib.Path(self.input_path).rglob("*.csv")
        filename_dict = {}
        for file in all_csv_files:
            filename = os.path.basename(file)
            file_list = filename_dict.setdefault(filename, [])
            file_list.append(file)

        excludes = ["_experiment_", "_experiment.csv", 'Experiment.csv']
        filename_excluded = {key: filename_dict[key] for key in list(filename_dict) if any(ex in key for ex in excludes)}
        for key in filename_excluded:
            del filename_dict[key]

        # concat all csv-files (per filename), loop filename(key)
        for filename, files in filename_dict.items():
            start = time.time()
            tmp_csvfile = os.path.join('/tmp/', filename + '.merged.csv.tmp')
            try:
                with open(tmp_csvfile, 'w') as csvout:
                    is_header_already_included = False
                    for fileCount, file in enumerate(files):
                        with open(file, "r") as f:
                            # only include header once
                            if is_header_already_included:
                                next(f)
                            for row in f:
                                csvout.write(row)
                                is_header_already_included = True

                    if fileCount % 500 == 0:
                        logging.info(f'{fileCount}/{len(files)} {filename}')


                logging.info(f'done concat csv {filename}')
                parquetfilename = os.path.splitext(filename)[0] + '.parquet'
                parquetfile = os.path.join(self.output_path, parquetfilename)

                self.csv_to_parquet_chunked(tmp_csvfile, parquetfile)
                logging.info(f"Elapsed time for {filename}: {(time.time() - start):.3f} seconds")

            except Exception as e:
                logging.error(f"Failed during concat csv files, error: {e}")

            finally:
                logging.info("Temporary file kept for inspection")
                # os.remove(tmp_csvfile)  # Uncomment this line if you wish to remove temporary files

        logging.info("Done merging CSV to Parquet")

def sync_output_dir_to_remote(local_path):
    logging.debug(f"inside sync_output_dir_to_remote local_path {local_path}")
    from_local_path = local_path.rstrip('/')
    remote_path = "/share/" #os.path.join("/share/", os.path.dirname(from_local_path))
    to_remote_path = f"guestserver:{remote_path}"
    logging.debug(f"to_remote_path {to_remote_path}")
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

    logging.debug(f"rsync_command {rsync_command}")

    # make sure to_path path exists
    if not os.path.exists(to_path):
        os.makedirs(to_path, exist_ok=True)

    # Execute the command
    result = subprocess.run(rsync_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logging.debug(result.stdout.decode())

def sync_with_rsync_relative(from_path, to_path):
    # Prepare the rsync command
    rsync_command = ["rsync", "-av","--relative", "--no-perms", "--omit-dir-times", from_path, to_path]

    logging.debug(f"rsync_command {rsync_command}")

    # Execute the command
    result = subprocess.run(rsync_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logging.debug(result.stdout.decode())

def stage_images_from_file_list(stage_images_file):
    logging.info("Inside stage_images_from_file_list")
    # Define the source and target directories
    remote_ssh_host_config = "guestserver"
    local_stage_directory = "/"  # Adjust the path as necessary

    # Ensure the local directory exists
    if not os.path.exists(local_stage_directory):
        os.makedirs(local_stage_directory, exist_ok=True)

    # Build the rsync command with additional options to ignore ownership and permissions
    rsync_command = [
        "rsync", "-av",
        "--no-owner", "--no-group", "--no-perms", "--omit-dir-times",
        "--files-from=" + stage_images_file,
        f"{remote_ssh_host_config}:/",  # Assuming the paths in stage_images.txt are absolute paths
        local_stage_directory
    ]

    # Log the rsync command
    logging.info(f"Executing rsync command: {' '.join(rsync_command)}")

    # Execute the rsync command
    try:
        result = subprocess.run(rsync_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Check for errors
        if result.returncode != 0:
            logging.error(f"Rsync error: {result.stderr.decode()}")
        else:
            logging.debug(f"Rsync output: {result.stdout.decode()}")
    except Exception as e:
        logging.error(f"Failed to execute rsync command: {e}")
        raise e

    logging.info(f"Staging completed successfully to {local_stage_directory}")


def set_permissions_recursive(path, permissions=0o777):
    logging.debug(f"inside set_permissions_recursive {path}")

    #set permissions for the input directory itself
    os.chmod(path, permissions)

    for dirpath, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            os.chmod(os.path.join(dirpath, dirname), permissions)
        for filename in filenames:
            os.chmod(os.path.join(dirpath, filename), permissions)
    logging.debug(f"done set_permissions_recursive {path}")


def run_cmd(cmd):
    """
    Execute a command (usually done in a separate thread)

    Args:
        cmd (str): The command line string to be executed by the system.

    """

    logging.info(f"run_cmd {cmd}")

    output_dir = None
    proc = None  # Initialize proc outside the try block
    try:
        if cmd is None or cmd.isspace():
            logging.error(f"return becatce cmd is None or cmd.isspace()")
            return

        # get parameters from cmd
        parts = cmd.split()
        index_data_file = parts.index('--data-file') + 1
        data_file = parts[index_data_file]

        index_output_dir = parts.index('-o') + 1
        output_dir = parts[index_output_dir]

        # set up logging
        log_file_path = os.path.join(output_dir, 'cp.log')

        # Check if a 'finished' file exists in the output directory, skip execution if it does
        finished_file_path = os.path.join(output_dir, "finished")
        if os.path.exists(finished_file_path):
            logging.info(f"Finished file exists, skipping this cmd")
            return

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)
        os.chmod(output_dir, 0o0777)

        # TODO touch image miles manually because uppmax is mounted with noatime and
        # we want to keep track of when files where last accessed
        ## get images from this commands datafile
        # image_list = parse_images_from_datafile(data_file)

        # Execute the command and redirect stdout and stderr to a log file
        with open(log_file_path, 'w') as log_file:
            proc = subprocess.Popen(cmd, shell=True, stdout=log_file, stderr=subprocess.STDOUT)
            active_processes.append(proc)
            stdout, stderr = proc.communicate()  # Wait for the subprocess to complete

        # Check the exit status of the subprocess
        if proc.returncode != 0:
            with open(os.path.join(output_dir, 'error'), 'w') as error_file:
                error_file.write(f'cmd: {cmd}\n')  # Log the command that failed
                error_file.flush()  # Flushes the internal buffer
                os.fsync(error_file.fileno())  # Ensures that all changes are written to disk
            logging.error("Subprocess exited with non-zero exit status")
            raise Exception("Subprocess failed with non-zero exit status.")

        # Create a 'finished' flag file if the command completes successfully
        with open(finished_file_path, 'w') as file:
            logging.info("inside write finished file")
            file.write('Done')  # Writing an empty string to create the file
            file.flush()  # Flushes the internal buffer
            os.fsync(file.fileno())  # Ensures that all changes are written to disk
            logging.info("done write finished file")

    except Exception as e:
        logging.error(f"Exception in subprocess running command: {cmd}, Error: {e}")
        raise

    finally:
        # Sync output dir back to fileserver
        if output_dir:
            logging.debug("finally sync output dir")
            set_permissions_recursive(output_dir, 0o777)
            sync_output_dir_to_remote(output_dir)

        # Remove the process from the active list and check if it needs to be terminated
        if proc is not None:  # Check if proc is defined
            if proc in active_processes:
                active_processes.remove(proc)
            if proc.returncode is None:  # Check if the process is still running
                proc.terminate()  # Terminate if still running

        logging.info(f"Done with job cmd: {cmd}")

def run_all_commands_from_threadpool(cmd_file, max_workers, max_errors):
    # read all cellprofiler commands to be executed
    with open(cmd_file, "r") as file:
        cmds = file.readlines()

    logging.debug(f"all cmds: {cmds}")

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

def main():

    # Configure logging
    logging.basicConfig(
        format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
        datefmt='%y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    try:
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

        cmd_file = f"{input_dir}/cmds.txt"

        # sync input dir
        sync_input_dir(input_dir)

        # sync pipelines dir
        sync_pipelines_dir(pipelines_dir)

        # stage images
        stage_images_from_file_list(f'{input_dir}/stage_images.txt')

        # execute all commands with a threadpool
        run_all_commands_from_threadpool(cmd_file, max_workers, max_errors)

        # merge all csv into parquet files
        converter = CSVToParquetConverter(input_path=output_dir,
                                        output_path=output_dir,
                                        chunk_size=10000)
        converter.merge_csv_to_parquet()

        # Sync output dir back to fileserver
        if output_dir:
            logging.info("sync output dir including parquet files")
            sync_output_dir_to_remote(output_dir)

        logging.info(f"elapsed: {time.time() - start_time} sek")

    except Exception as e:
        logging.error(f"Exception out of script ", e)

        # Cleanup: terminate all active subprocesses
        for p in active_processes:
            p.terminate()


if __name__ == "__main__":
    main()
