#!/usr/bin/env python3
import logging
import os
import sys
import time
import subprocess
import platform
import csv
import concurrent.futures
import shlex
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError, EndpointConnectionError
from boto3.s3.transfer import TransferConfig

import pandas as pd
import pyarrow
import pyarrow.parquet as pq

from s3_client_wrapper import S3ClientWrapper

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
@dataclass
class RunnerConfig:
    stage_root_dir: str = os.environ.get("STAGE_ROOT_DIR", "/")
    input_dir: str = os.environ.get("INPUT_DIR", "/cpp_work/input")
    output_dir: str = os.environ.get("OUTPUT_DIR", "/cpp_work/output")
    results_dir: str = os.environ.get("RESULT_DIR", "/cpp_work/result")
    pipelines_dir: str = "/cpp_work/pipelines"
    max_workers: int = int(os.environ.get("MAX_WORKERS", "16"))
    max_errors: int = int(os.environ.get("MAX_ERRORS", "5"))
    omp_threads: int = int(os.environ.get("OMP_NUM_THREADS", "1"))
    cmd_ix_to_run: int = int(os.environ.get("CMD_IX_TO_RUN", "-1"))
    s3_bucket: str = os.environ.get("S3_BUCKET", "mikro")
    s3_endpoint_url: str = os.environ.get("S3_ENDPOINT_URL", "https://s3.spirula.uppmax.uu.se:8443" )
    s3_region: Optional[str] = os.environ.get("AWS_DEFAULT_REGION", None) # default not needed


class CSVToParquetConverter:
    def __init__(self, input_path, output_path, chunk_size=10000):
        self.input_path = input_path
        self.output_path = output_path
        self.chunk_size = chunk_size

    @staticmethod
    def _to32bit(df):
        return df.astype({c: str(df[c].dtype).replace('64', '32') for c in df.columns})

    def _csv_to_parquet_chunked(self, csv_file_path, parquet_file_path):
        # Create a Parquet writer
        parquet_writer = None
        schema = None

        pyarrow.set_cpu_count(5)

        # Read the CSV file in chunks
        for chunk in pd.read_csv(csv_file_path, chunksize=self.chunk_size):
            logging.debug("processing a new chunk")
            chunk = self._to32bit(chunk)
            # Convert the chunk to a PyArrow Table
            table = pyarrow.Table.from_pandas(chunk)
            logging.debug("done df to table")
            if parquet_writer is None:
                # Initialize the Parquet writer with the schema of the first chunk
                schema = table.schema
                parquet_writer = pq.ParquetWriter(parquet_file_path, schema=schema, compression='snappy')

            # Write the table chunk to the Parquet file
            logging.debug("before write table")
            parquet_writer.write_table(table)
            logging.debug("done write table")

        # Close the Parquet writer if it was initialized
        if parquet_writer:
            parquet_writer.close()

    def merge_csv_and_convert_to_parquet(self):
        logging.info("Inside merge_csv_and_convert_to_parquet")

        all_csv_files = Path(self.input_path).rglob("*.csv")
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

                self._csv_to_parquet_chunked(tmp_csvfile, parquetfile)
                logging.info(f"Elapsed time for {filename}: {(time.time() - start):.3f} seconds")


            except Exception as e:
                logging.error(f"Failed during concat csv files, error: {e}")

            finally:
                logging.info("Temporary file kept for inspection")
                # os.remove(tmp_csvfile)  # Uncomment this line if you wish to remove temporary files

        logging.info("Done merging CSV to Parquet")


# ----------------------------------------------------------------------------
# Sync Utilities
# ----------------------------------------------------------------------------
def _run_rsync(src: str, dst: str, relative: bool = False, excludes: Optional[List[str]] = None) -> None:
    cmd = [
        "rsync", "-av",
        "--no-owner", "--no-group", "--no-perms",
        "--omit-dir-times",
    ]
    if relative:
        cmd += ["--relative"]
    if excludes:
        for pat in excludes:
            cmd.append(f"--exclude={pat}")
    cmd += [src, dst]
    logging.debug("Rsync command: %s", cmd)
    subprocess.run(cmd, check=True)

def sync_input_dir(local: str, remote: str) -> None:
    _run_rsync(f"guestserver:{remote}/*", local)

def sync_pipelines_dir(local: str, remote: str) -> None:
    _run_rsync(f"guestserver:{remote}/*", local)

def sync_output_dir_to_remote(local: str) -> None:
    _run_rsync(local.rstrip('/'), f"guestserver:/share")

# def download_single_file_via_rsync(remote: str, key: str, dest: str) -> None:
#     _run_rsync(f"{remote}:/{key}", dest)

def stage_images_via_rsync_files_list(cfg: RunnerConfig, stage_images_file: str) -> None:

    os.makedirs(cfg.stage_root_dir, exist_ok=True)

    cmd = [
        "rsync", "-av",
        "--no-owner", "--no-group", "--no-perms", "--omit-dir-times",
        f"--files-from={stage_images_file}",
        "guestserver:/",             # remote host fixed here
        cfg.stage_root_dir,
    ]

    logging.info("[stage][rsync] starting fetch using file list: %s", stage_images_file)
    logging.debug("Rsync command: %s", " ".join(shlex.quote(c) for c in cmd))

    try:
        subprocess.run(cmd, check=True)
        logging.info("[stage][rsync] fetch completed")
    except subprocess.CalledProcessError as e:
        logging.error("[stage][rsync] fetch failed: %s", e)
        raise

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


def _make_s3_wrapper(cfg: RunnerConfig) -> S3ClientWrapper:
    return S3ClientWrapper(endpoint_url=cfg.s3_endpoint_url, region=cfg.s3_region)

# def download_from_s3(
#     client_wrapper: S3ClientWrapper,
#     bucket: str,
#     key: str,
#     dest: str) -> bool:
#     """
#     Try to copy s3://{bucket}/{key} → dest via boto3.
#     Returns True if the object was fetched successfully, False otherwise.
#     """
#     os.makedirs(os.path.dirname(dest), exist_ok=True)
#     try:
#         s3 = client_wrapper.get_fresh_s3_client()
#         # download_file will raise if 404, etc
#         s3.download_file(bucket, key, dest)
#         logging.info(f"[stage][S3] fetched {key} → {dest}")
#         return True
#     except client_wrapper.s3_client.exceptions.NoSuchKey:
#         logging.debug(f"[stage][S3] {key} not found in bucket {bucket}")
#         return False
#     except Exception as e:
#         logging.warning(f"[stage][S3] error fetching {key}: {e}")
#         return False

def stage_images_via_s3_files_list(cfg: RunnerConfig, stage_images_file: str) -> None:
    """
    Fetch listed files via S3 into cfg.stage_root_dir.
    Abort on the FIRST error of any kind.
    """
    if not cfg.s3_bucket:
        raise Exception("Missing s3_bucket in config")

    os.makedirs(cfg.stage_root_dir, exist_ok=True)

    # Build the wrapper here (safer for multiprocessing)
    client_wrapper = _make_s3_wrapper(cfg)
    s3 = client_wrapper.get_fresh_s3_client()

    xfer_cfg = TransferConfig(num_download_attempts=1, max_concurrency=1)

    with open(stage_images_file, "r") as f:
        for line in f:
            src = line.strip()
            if not src:
                continue

            key = src.lstrip("/")
            dest = os.path.join(cfg.stage_root_dir, key)

            if os.path.exists(dest):
                logging.debug("[stage][S3] exists, skipping: %s", dest)
                continue

            os.makedirs(os.path.dirname(dest), exist_ok=True)

            try:
                s3.download_file(cfg.s3_bucket, key, dest, Config=xfer_cfg)
                logging.info("[stage][S3] fetched %s → %s", key, dest)
            except Exception as e:
                logging.error("[stage][S3] failed for %s: %s", key, e)
                raise

def stage_images_from_file_list(cfg: RunnerConfig, stage_images_file: str) -> None:
    """
    Stage all files listed in stage_images_file into cfg.stage_root_dir.
    Job-wide sync method (S3 or rsync) is controlled by a flag file.
    """
    os.makedirs(cfg.stage_root_dir, exist_ok=True)

    method = get_image_sync_method()

    logging.info("[stage] method=%s, staging %s", method, stage_images_file)

    if method == "s3":
        try:
            stage_images_via_s3_files_list(cfg, stage_images_file)
            logging.info("[stage] S3 succeeded for %s", stage_images_file)
            return
        except Exception as e:
            logging.warning("[stage] S3 failed for %s: %s. Switching to rsync.", stage_images_file, e)
            set_image_sync_method("rsync")

    # Fallback / chosen path: rsync for the whole list into cfg.stage_root_dir
    stage_images_via_rsync_files_list(cfg, stage_images_file)


def get_sync_method_flag_file_name() -> str:
    """Return path to a job-specific flag file in /tmp."""
    job_id = os.environ.get("SLURM_JOB_ID") or "default"
    return f"/tmp/stage_sync_method_{job_id}.flag"

def get_image_sync_method() -> str:
    """Return current sync method ('s3' or 'rsync'), defaulting to 's3'."""
    flag = get_sync_method_flag_file_name()
    if os.path.exists(flag):
        with open(flag) as f:
            val = f.read().strip().lower()
            return val if val else "s3"
    return "s3"

def set_image_sync_method(method: str) -> None:
    """Set the sync method ('s3' or 'rsync')."""
    with open(get_sync_method_flag_file_name(), "w") as f:
        f.write(method.strip().lower())

def set_permissions_recursive(path, permissions=0o777):
    logging.debug(f"inside set_permissions_recursive {path}")

    #set permissions for the root
    os.chmod(path, permissions)

    for dirpath, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            os.chmod(os.path.join(dirpath, dirname), permissions)
        for filename in filenames:
            os.chmod(os.path.join(dirpath, filename), permissions)
    logging.debug(f"done set_permissions_recursive {path}")


# ----------------------------------------------------------------------------
# Command Runner
# ----------------------------------------------------------------------------
active_processes: List[subprocess.Popen] = []
def run_cmd(cmd: str, cfg: RunnerConfig):
    """
    Execute a command (usually done in a separate thread)

    Args:
        cmd (str): The command line string to be executed by the system.

    """

    logging.info(f"run_cmd {cmd}")
    # use cfg inside here
    logging.info("run_cmd %s (stage_root=%s)", cmd, cfg.stage_root_dir)

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

        # TODO touch image files manually because uppmax is mounted with noatime and
        # we want to keep track of when files where last accessed
        ## get images from this commands datafile
        # image_list = parse_images_from_datafile(data_file)

        # Execute the command and redirect stdout and stderr to a log file
        logging.info(f"cp logfile: {log_file_path}")
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

        # Remove the process from the active list and check if it needs to be terminated
        if proc is not None:  # Check if proc is defined
            if proc in active_processes:
                active_processes.remove(proc)
            if proc.returncode is None:  # Check if the process is still running
                proc.terminate()  # Terminate if still running

        logging.info(f"Done with job cmd: {cmd}")


# ----------------------------------------------------------------------------
# Thread Pool Executor
# ----------------------------------------------------------------------------
def run_all_commands_via_threadpool(
    cfg: RunnerConfig,
    cmd_file: str) -> None:

    # read & clean commands
    with open(cmd_file, "r") as f:
        cmds = [line.strip() for line in f if line.strip()]

    if cfg.cmd_ix_to_run >= 0:
        if cfg.cmd_ix_to_run >= len(cmds):
            raise IndexError(f"cmd_ix_to_run {cfg.cmd_ix_to_run} out of range (0–{len(cmds)-1})")
        cmds = [cmds[cfg.cmd_ix_to_run]]

    with concurrent.futures.ProcessPoolExecutor(max_workers=cfg.max_workers) as executor:
        futures = {executor.submit(run_cmd, cmd, cfg): cmd for cmd in cmds}

        pending = len(futures)
        finished = errors = 0

        for future in concurrent.futures.as_completed(futures):
            cmd = futures[future]
            try:
                future.result()
                finished += 1
            except Exception:
                logging.exception("Error running command %s", cmd)
                errors += 1
            pending -= 1
            logging.info("Pending: %d, Finished: %d, Errors: %d", pending, finished, errors)
            if errors > cfg.max_errors:
                raise Exception(f"More errors than max_errors: {errors} > {cfg.max_errors}")

def setup_logging(level=logging.INFO):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s [%(module)s:%(lineno)d] %(message)s',
        datefmt='%y-%m-%d %H:%M:%S',
        level=logging.INFO,
    )

def testrun1(cfg: Optional[RunnerConfig] = None):

    setup_logging(level=logging.DEBUG)
    if cfg is None:
      cfg = RunnerConfig()

    sync_input_dir(f"{cfg.stage_root_dir}{cfg.input_dir}", f"/share{cfg.input_dir}")
    stage_images_from_file_list(cfg, f'{cfg.input_dir}/stage_images_all.txt')

def test_run_all(cfg: Optional[RunnerConfig] = None):

    setup_logging(level=logging.DEBUG)
    if cfg is None:
        cfg = RunnerConfig()

    run_pipeline(cfg)

def test_print_debug():

    setup_logging(level=logging.DEBUG)

    # --- log interpreter details ---
    logging.info("Python %s  (executable: %s)",
    platform.python_version(), sys.executable)

    try:
        result = subprocess.run(
            ["ssh", "-G", "guestserver"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        logging.info("Effective SSH config for guestserver")
        logging.info(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error running ssh -G:", e.stderr)


def testrun_stage(cfg: Optional[RunnerConfig] = None):

    setup_logging(level=logging.DEBUG)
    if cfg is None:
      cfg = RunnerConfig()

    stage_images_from_file_list(cfg, f'{cfg.input_dir}/stage_images.txt')

def run_pipeline(cfg: RunnerConfig) -> None:

    try:
        start_time = time.time()

        cmd_file = f"{cfg.input_dir}/cmds.txt"

        # sync input dir
        sync_input_dir(f"{cfg.input_dir}", f"/share{cfg.input_dir}")

        # sync pipelines dir
        sync_pipelines_dir(f"{cfg.pipelines_dir}", f"/share{cfg.pipelines_dir}")

        # stage images
        stage_images_from_file_list(cfg, f'{cfg.input_dir}/stage_images.txt',)

        # execute all commands with a threadpool
        run_all_commands_via_threadpool(cfg, cmd_file)

        # merge all csv into parquet files
        converter = CSVToParquetConverter(input_path=cfg.output_dir,
                                        output_path=cfg.output_dir,
                                        chunk_size=10000)
        converter.merge_csv_and_convert_to_parquet()

        # Sync output dir back to fileserver (or S3)
        if cfg.output_dir:
            logging.info("sync output dir including parquet files")
            sync_output_dir_to_remote(cfg.output_dir)

        logging.info(f"elapsed: {time.time() - start_time} sek")

    except Exception as e:
        logging.error(f"Exception out of script ", e)

        # Cleanup: terminate all active subprocesses
        for p in active_processes:
            p.terminate()


def main() -> None:
    setup_logging()
    cfg = RunnerConfig()
    run_pipeline(cfg)

if __name__ == "__main__":
    main()
