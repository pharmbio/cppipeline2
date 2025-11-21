#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
import sys
import logging
import re
import yaml
import random
import base64
import os
import pathlib
import pdb
import json
import string
import itertools
import math
import pathlib
import csv
import shutil
import datetime
import time
import pandas as pd
import pyarrow
import subprocess
from typing import List, Dict, Any, Optional, Tuple

from database import Database
from database import Analysis
from job_utils import JobName, get_list_of_finished_jobs
from file_sync_utils import SyncManager
import hpc_utils
from config import CONFIG
import error_utils

## JobName helpers are now provided by job_utils.JobName; no wrappers needed here.

def _list_subdirs(path: str) -> List[str]:
    try:
        return [d.name for d in os.scandir(path) if d.is_dir()]
    except FileNotFoundError:
        return []

def _get_sub_analysis_start(sub_id: int) -> Optional[datetime.datetime]:
    db = Database.get_instance()
    conn = None
    try:
        conn = db.get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT start FROM image_sub_analyses WHERE sub_id=%s", (sub_id,))
            row = cur.fetchone()
            if not row:
                return None
            start = row.get("start")
            # Return DB value as-is (tz-aware if timestamptz); do not strip tz
            return start
    except Exception as e:
        logging.error(f"Error fetching sub analysis start for {sub_id}: {e}")
        return None
    finally:
        if conn:
            db.release_connection(conn)

def _update_progress(analysis_id: int, sub_id: int, done: int, total: int) -> None:
    if total <= 0:
        return
    # Estimate remaining time if we have at least one finished job
    progress_str = f"{done} / {total} jobs finished"
    if done > 0:
        start_time = _get_sub_analysis_start(sub_id)
        if start_time:
            # Use tz-aware now if start_time is tz-aware to avoid naive/aware mismatch
            try:
                if getattr(start_time, 'tzinfo', None):
                    now_ts = datetime.datetime.now(start_time.tzinfo)
                else:
                    now_ts = datetime.datetime.now()
                elapsed = now_ts - start_time
            except Exception:
                elapsed = datetime.datetime.now() - start_time
            try:
                avg = elapsed.total_seconds() / max(done, 1)
                remain = max(total - done, 0) * avg
                hours_remain = remain / 3600.0
                progress_str = f"{progress_str}, time remaining: {hours_remain:.2f} hours"
            except Exception:
                pass
    Database.get_instance().set_status(analysis_id, sub_id, "progress", progress_str)


## get_job_list moved to job_utils.py

def make_imgset_csv(imgsets: Any,
                    channel_map: Dict[Any, str],
                    sub_analysis_input_dir: str,
                    use_icf: bool) -> str:
    """
    Create a CellProfiler formatted CSV string from image sets.

    Args:
        imgsets: Either a dictionary mapping keys to ImageSet objects or a list of ImageSet objects.
        channel_map: A dict mapping channel numbers to channel names.
        sub_analysis_input_dir: Directory where illumination correction files reside.
        use_icf: If True, include illumination correction columns.

    Returns:
        A CSV-formatted string.
    """
    # Ensure we have an iterable of (key, ImageSet) pairs.
    if isinstance(imgsets, dict):
        imgset_items = list(imgsets.items())
    elif isinstance(imgsets, list):
        # Create dummy keys (using the index as a string)
        imgset_items = [(str(i), imgset) for i, imgset in enumerate(imgsets)]
    else:
        raise ValueError("imgsets must be a dict or list.")

    # Build header row.
    header_parts = []
    for ch_nr, ch_name in sorted(channel_map.items()):
        header_parts.append(f"FileName_{ch_name}")
    header_parts.extend([
        "Group_Index",
        "Group_Number",
        "ImageNumber",
        "Metadata_Barcode",
        "Metadata_Site",
        "Metadata_Well",
        "Metadata_AcqID",
        "Metadata_z"
    ])
    for ch_nr, ch_name in sorted(channel_map.items()):
        header_parts.append(f"PathName_{ch_name}")
    for ch_nr, ch_name in sorted(channel_map.items()):
        header_parts.append(f"URL_{ch_name}")
    if use_icf:
        for ch_nr, ch_name in sorted(channel_map.items()):
            header_parts.append(f"URL_ICF_{ch_name}")
        for ch_nr, ch_name in sorted(channel_map.items()):
            header_parts.append(f"PathName_ICF_{ch_name}")
        for ch_nr, ch_name in sorted(channel_map.items()):
            header_parts.append(f"FileName_ICF_{ch_name}")
    header = ",".join(header_parts) + "\n"

    # Build content rows.
    content_lines = []
    for imgset_key, imgset in imgset_items:
        row_parts = []
        # Sort the images in the ImageSet by their 'channel' key.
        sorted_imgset = sorted(imgset.all_images, key=lambda img: img.get_data().get('channel'))

        # Add filenames (basename of the image path) for each image.
        filenames = [f'"{os.path.basename(img.path)}"' for img in sorted_imgset]
        row_parts.extend(filenames)

        # Use the first image's metadata for common fields.
        first_img = sorted_imgset[0].get_data()
        row_parts.append(str(imgset_key))           # Group_Index
        row_parts.append("1")                         # Group_Number (fixed value)
        row_parts.append(str(imgset_key))             # ImageNumber
        row_parts.append(f'"{first_img.get("plate_barcode")}"')
        row_parts.append(str(first_img.get("site")))
        row_parts.append(f'"{first_img.get("well")}"')
        row_parts.append(str(first_img.get("plate_acquisition_id")))
        row_parts.append(str(first_img.get("z")))

        # Add file paths (directory names).
        filepaths = [f'"{os.path.dirname(img.path)}"' for img in sorted_imgset]
        row_parts.extend(filepaths)

        # Add file URLs (with "file:" prefix).
        file_urls = [f'"file:{img.path}"' for img in sorted_imgset]
        row_parts.extend(file_urls)

        if use_icf:
            icf_path = sub_analysis_input_dir
            # For each channel, add ICF URL, PathName, and FileName.
            url_icf = [f'"file:{icf_path}/ICF_{ch_name}.npy"' for _, ch_name in sorted(channel_map.items())]
            row_parts.extend(url_icf)
            pathname_icf = [f'"{icf_path}"' for _ in sorted(channel_map.items())]
            row_parts.extend(pathname_icf)
            filename_icf = [f'"ICF_{ch_name}.npy"' for _, ch_name in sorted(channel_map.items())]
            row_parts.extend(filename_icf)

        # Join row parts and add to content.
        content_lines.append(",".join(row_parts))

    content = "\n".join(content_lines) + "\n"
    return header + content

def is_debug() -> bool:
    """
    Determine debug mode from the DEBUG env var.

    Truthy values: 1, true, yes, on (case-insensitive)
    Falsy values: 0, false, no, off, empty/missing
    """
    val = os.environ.get('DEBUG')
    if val is None:
        return False
    v = str(val).strip().lower()
    if v in ("1", "true", "yes", "on"):  # enable
        return True
    if v in ("0", "false", "no", "off", ""):
        return False
    # Fallback: any other non-empty value counts as true
    return True


def get_hpc_cluster() -> str:
    """
    Resolve which HPC cluster/run_location to use.

    Controlled via the HPC_CLUSTER env var, falling back to "pelle".
    Accepted values include: pelle, hpc_dev, rackham, uppmax, farmbio.
    """
    default = "pelle"
    env_val = os.environ.get("HPC_CLUSTER")
    if not env_val:
        return default

    cluster = env_val.strip().lower()
    allowed = {"pelle", "hpc_dev", "rackham", "uppmax", "farmbio"}
    if cluster not in allowed:
        logging.warning("Unknown HPC_CLUSTER '%s', falling back to %s", cluster, default)
        return default
    return cluster


def generate_random_identifier(length):
    return ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(length))

def copy_dependant_results_to_input(to_dir, from_dir):
    logging.info(f"copy_dependant_results_to_input, to_dir {to_dir} from_dir{from_dir}")
    # List of file suffixes to include, in lower case for case-insensitive comparison
    include_suffixes = ['.npy']

    # Check if the source (input) directory exists
    if not os.path.exists(from_dir):
        logging.debug(f"The source directory {from_dir} does not exist. return")
        return  # Exit the function

    # Check if target exist
    if not os.path.exists(to_dir):
        logging.debug(f"The target directory {to_dir} does not exist. return")
        return

    # List all the files and directories in the source directory.
    for item in os.listdir(from_dir):
        source = os.path.join(from_dir, item)
        destination = os.path.join(to_dir, item)

        # Check if the current item is a file and if it ends with one of the included suffixes.
        if os.path.isfile(source) and any(source.lower().endswith(suffix) for suffix in include_suffixes):
            # Copy the file to the destination directory.
            shutil.copy2(source, destination)  # copy2 preserves metadata
            logging.debug(f"File {source} copied to {destination}")
        elif os.path.isfile(source):
            # Log skipping of the file
            logging.debug(f"File {source} skipped due to not matching include suffixes.")


def fetch_finished_subanalyses_hpc(cluster: str) -> Dict[int, List[Dict[str, Any]]]:
    """
    Find sub-analyses (HPC) where all jobs have finished.

    - Queries unfinished, non-error sub_analyses for the selected run_location.
    - Scans /cpp_work/output/<sub_id>/ for job directories created by this server.
    - Considers a job finished if a file named "finished" exists in the job folder.
    - Updates progress via Database.set_status(..., "progress", ...).
    - Returns dict keyed by sub_id: { sub_id: [ {"metadata": {"name", "sub_id", "analysis_id"}}, ... ] }
    """
    logging.info("Inside fetch_finished_subanalyses_hpc")

    # get unfinished subs from db
    db = Database.get_instance()
    conn = None
    unfinished_subs = []
    try:
        conn = db.get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = (
                "SELECT sub_id, analysis_id, meta "
                "FROM image_sub_analyses "
                "WHERE finish IS NULL "
                "  AND error IS NULL "
                "  AND (meta->>'run_location') = %s "
                "ORDER BY sub_id"
            )
            logging.debug(f"fetch_finished_subanalyses_hpc query: {query} cluster: {cluster}")
            cur.execute(query, [cluster])
            unfinished_subs = cur.fetchall()
            sub_ids_for_logging = [row["sub_id"] for row in unfinished_subs]
            logging.info(f"unfinished_subs in database (sub_ids): {sub_ids_for_logging}")
    except Exception as e:
        logging.error("fetch_finished_subanalyses_hpc: DB query failed; cluster=%s", cluster, exc_info=True)
    finally:
        if conn:
            db.release_connection(conn)

    # Per-sub_id processing: compute finished lists and totals via helper, update progress, and emit finished per sub_id
    finished_by_sub: dict[int, list[dict]] = {}
    for sub in unfinished_subs:
        sub_id = sub["sub_id"]
        analysis = Database.get_instance().get_analysis(sub_id)
        if not analysis:
            logging.debug(f"No analysis object for sub_id={sub_id}; skipping")
            continue

        # First: If a top-level error marker exists in the sub output directory, mark sub-analysis as error
        try:
            if os.path.exists(os.path.join(analysis.sub_output_dir, "error")):
                logging.error(
                    "Top-level error marker found; marking sub-analysis error; sub_id=%s analysis_id=%s",
                    analysis.sub_id,
                    analysis.id,
                )
                # Read original error message (if available) from the 'error' file
                err_path = os.path.join(analysis.sub_output_dir, "error")
                original_err = None
                try:
                    with open(err_path, "r", encoding="utf-8", errors="replace") as f:
                        original_err = f.read().strip()
                except Exception:
                    # Non-fatal: fall back to generic message if we cannot read file
                    logging.error("Could not read original error file at %s", err_path, exc_info=True)

                # Compose message to persist in DB, including original error if present
                msg = (
                    f"error marker found in sub output dir"
                    + (f": {original_err}" if original_err else "")
                )
                Database.get_instance().set_sub_analysis_error(analysis, msg)
                # Skip further processing for this sub-analysis in this cycle
                continue
        except Exception:
            # If checking fails, proceed without crashing the loop
            logging.debug("Failed checking top-level error marker for sub_id=%s", analysis.sub_id)

        # Second Get all finished jobs
        finished_list, total_jobs = get_list_of_finished_jobs(analysis, finished_only=True, exclude_errors=True)
        done = len(finished_list)

        if total_jobs is not None:
            _update_progress(analysis.id, analysis.sub_id, done, total_jobs)

        # If all done, emit as finished for this sub_id
        if total_jobs is not None and done == total_jobs and total_jobs > 0:
            finished_by_sub[analysis.sub_id] = finished_list

    logging.info("Finished sub-analyses: %d", len(finished_by_sub))
    return finished_by_sub


def handle_new_analyses(cluster: str):
    logging.info(f'Inside handle_new_analyses for cluster={cluster}')

    analyses = Database.get_instance().get_new_analyses()

    # first run through all and check for unstarted on hpc (dardel)
    for analysis in analyses:
        if analysis.is_cellprofiler_analysis:
            #logging.debug(f"Selected analysis {analysis.id} analysis.run_location {analysis.run_location} for cluster '{cluster}'")
            if analysis.run_location == cluster:
                logging.debug(f"Selected analysis {analysis.id} for cluster '{cluster}'")
                if analysis.all_dependencies_satisfied():
                    try:
                        # Soft validation guard from Analysis
                        if not analysis.is_valid:
                            logging.error(
                                "Invalid analysis; sub_id=%s analysis_id=%s reason=%s",
                                analysis.sub_id,
                                analysis.id,
                                getattr(analysis, 'invalid_reason', None),
                            )
                            Database.get_instance().set_sub_analysis_error(
                                analysis,
                                str(getattr(analysis, 'invalid_reason', 'Invalid analysis')),
                            )
                            continue
                        handle_analysis_cellprofiler_hpc(analysis, run_location=cluster)
                    except Exception as e:
                        logging.error(
                            "handle_new_analyses: sub-analysis failed; sub_id=%s analysis_id=%s",
                            analysis.sub_id,
                            analysis.id,
                            exc_info=True,
                        )
                        Database.get_instance().set_sub_analysis_error(analysis, str(e))
                        # Continue with other analyses without crashing the server loop


def handle_analysis_cellprofiler_hpc(analysis: Analysis, run_location: str):

        prepare_analysis_cellprofiler_hpc(analysis)

        # Build sbatch command using explicit run location
        cmd = hpc_utils.build_ssh_cmd_sbatch_hpc(analysis, run_location)

        # write sbatch file to input dir (for debug and re-submit functionality)
        sbatch_out_file = f"{analysis.sub_input_dir}/sbatch.sh"
        logging.debug(f"sbatch_out_file: {sbatch_out_file}")
        with open(sbatch_out_file, "w") as file:
            file.write(cmd)

        ## sync input dir to hpc
        #uppmax_cfg = CONFIG.cluster.get('rackham')
        #sync_manager = SyncManager(uppmax_cfg.get('user'), uppmax_cfg.get('hostname'), uppmax_cfg.get('work_dir'))
        #sync_manager.sync_input_dir(analysis.sub_id)

        # submit sbatch job via ssh
        #logging.info("Skip submit sbatch for now, cmd %r", cmd)
        #job_id = "NoRealJobIDJustTest"
        logging.info("Submit sbatch, cmd %r", cmd)
        job_id = hpc_utils.exec_ssh_sbatch_cmd(cmd)
        logging.info("job_id, job_id %r", job_id)

        if job_id:
            analysis.add_status_jobid(job_id)
            analysis.timestamp_started()


def prepare_analysis_cellprofiler_hpc(analysis: Analysis):

        if analysis is None:
            msg = "prepare_analysis_cellprofiler_hpc called with None analysis"
            logging.error(msg)
            raise ValueError(msg)

        logging.info(f"Inside prepare_analysis_cellprofiler_hpc, analysis: {analysis.raw_data}")

        # Validate required fields
        if not analysis.plate_acquisition_id:
            raise ValueError(f"Missing or wrong plate_acquisition_id for sub_id {analysis.sub_id}")

        # Ensure input/output directories exist
        os.makedirs(analysis.sub_input_dir, exist_ok=True)
        os.makedirs(analysis.sub_output_dir, exist_ok=True)

        # get cellprofiler-version
        logging.debug(f"pipeline: {analysis.pipeline_file}")
        logging.debug(f"cp version: {analysis.cellprofiler_version}")

        # group imagesets into batches and create one cellprofiler job/command per batch
        imgset_batches = analysis.get_imgset_batches()
        logging.info(f"imgset_batches: {len(imgset_batches)}")
        if not imgset_batches:
            raise ValueError(f"No image sets found for sub_id {analysis.sub_id}")

        channel_map = analysis.get_channelmap()
        logging.debug(f"channelmap: {channel_map}")

        # Fetch images once; avoid repeated DB queries inside the loop
        all_images_list = analysis.get_images()

        #random_identifier = generate_random_identifier(8)
        all_cmds = []
        n_jobs = len(imgset_batches)

        # Log once per analysis to reduce noise
        job_timeout = analysis.job_timeout
        logging.debug(f"job_timeout={job_timeout}")
        use_icf = analysis.use_icf
        logging.debug(f"use_icf: {use_icf}")
        for i, imgsets in enumerate(imgset_batches):
            # generate names
            job_number = i
            jn = JobName(sub_id=analysis.sub_id, job_number=job_number, n_jobs=n_jobs, analysis_id=analysis.id)
            job_name = jn.to_folder_name()
            imageset_file = f"{analysis.sub_input_dir}/{job_name}.csv"
            job_output_path = f"{analysis.sub_output_dir}/{job_name}/"

            cellprofiler_cmd = hpc_utils.get_cellprofiler_cmd_hpc(analysis.pipeline_file,
                                                                  imageset_file,
                                                                  job_output_path,
                                                                  job_timeout)

            # Check if icf headers should be added to imgset csv file
            # generate cellprofiler imgset file for this imgset
            imageset_content = make_imgset_csv(imgsets=imgsets, channel_map=channel_map,
                                               sub_analysis_input_dir=analysis.sub_input_dir, use_icf=use_icf)

            # create a folder for the file if needed
            os.makedirs(os.path.dirname(imageset_file), exist_ok=True)
            # write csv
            with open(imageset_file, 'w') as file:
                file.write(imageset_content)


            all_cmds.append(cellprofiler_cmd)


        logging.info("done creating jobs")

        # write all cmds into a text file into input folder
        os.makedirs(analysis.sub_input_dir, exist_ok=True)
        with open(f"{analysis.sub_input_dir}/cmds.txt", "w") as file:
            for item in all_cmds:
                file.write(item + "\n")

        # write a stage list for all images (reuse pre-fetched list)
        with open(f"{analysis.sub_input_dir}/stage_images_all.txt", "w") as file:
            for image in all_images_list:
                file.write(image.path + "\n")

        # Make sure analysis output dir exists
        os.makedirs(f"{analysis.results_dir}", exist_ok=True)
        # Write all results from dependant sub analyses into the input dir
        copy_dependant_results_to_input(analysis.sub_input_dir, analysis.results_dir)

        logging.info(f"Done prepare_analysis_cellprofiler_hpc")


def setup_logging(log_level):
        # set up logging to file
        now = datetime.datetime.now()
        now_string = now.strftime("%Y-%m-%d_%H.%M.%S.%f")

        print(f"is_debug {is_debug()}")

        if log_level == logging.DEBUG:
            log_prefix = "debug."
        else:
            log_prefix = ""

        logfile_name = "/cpp_work/logs/cpp_master_v2." + log_prefix + now_string + ".log"

        logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                            datefmt='%Y-%m-%d:%H:%M:%S',
                            level=log_level,
                            filename=logfile_name,
                            filemode='w')

        # define a Handler which writes INFO messages or higher to the sys.stderr

        console = logging.StreamHandler()
        console.setLevel(log_level)

        # set a formater for console
        consol_fmt = logging.Formatter('%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
        # tell the handler to use this format
        console.setFormatter(consol_fmt)

        # add the handler to the root logger
        root_logger = logging.getLogger('')
        root_logger.addHandler(console)
        # Also forward ERROR+ logs to Slack
        try:
            error_utils.install_slack_logging(level=logging.ERROR, logger=root_logger)
        except Exception as e:
            # If Slack handler install fails, keep console logging only
            logging.getLogger(__name__).debug(f"Slack logging install failed: {e}")


def _log_level_from_env(default_level: int = logging.INFO) -> int:
    """
    Resolve log level from environment variables.
    - `LOG_LEVEL` / `LOGLEVEL` can be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL
    - If not set, fall back to DEBUG when `DEBUG` env is present, else `default_level`.
    """
    env_val = os.environ.get("LOG_LEVEL") or os.environ.get("LOGLEVEL")
    if env_val:
        name = env_val.strip().upper()
        mapping = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "WARN": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
            "FATAL": logging.CRITICAL,
        }
        return mapping.get(name, default_level)
    return default_level

def init_new_db(cpp_config=None):
    """
    Initialize the connection pool using unified configuration.

    - Prefer values from the provided `cpp_config['postgres']` for backward compatibility.
    - Otherwise, use `CONFIG.db` loaded via config.py, which takes DB_PASS from env.
    """
    if cpp_config and isinstance(cpp_config, dict) and 'postgres' in cpp_config:
        db_cfg = cpp_config['postgres']
    else:
        from config import CONFIG as _CFG
        db_cfg = _CFG.db
    Database.get_instance().initialize_connection_pool(
        dbname=db_cfg['db'],
        user=db_cfg['user'],
        host=db_cfg['host'],
        port=db_cfg['port'],
        password=db_cfg.get('password'),
    )

def move_job_results_to_storage(analysis: Analysis, job_list: List[Dict[str, Any]]) -> List[str]:
    """
    Move non-CSV/log job outputs from each finished job directory into the analysis storage path.
    Also move any merged parquet files found in the sub-analysis output root.

    Returns a list of file paths (relative to analysis storage) that were created.
    """
    logging.info("Inside move_job_results_to_storage")

    skip_suffixes = {'.csv', '.log'}
    skip_files = {'finished', 'error'}

    files_created: List[str] = []

    storage_root = analysis.results_dir.rstrip('/')
    os.makedirs(storage_root, exist_ok=True)

    # For each finished job, move result files except skipped ones
    for job in job_list:
        job_name = job['metadata']['name']
        job_path = f"/cpp_work/output/{analysis.sub_id}/{job_name}"

        if not os.path.exists(job_path):
            logging.warning(f"Job path does not exist, skipping: {job_path}")
            continue

        for result_file in pathlib.Path(job_path).rglob("*"):
            # Skip dirs and certain file suffixes and marker files
            if result_file.is_dir():
                continue
            if result_file.suffix in skip_suffixes or result_file.name in skip_files:
                continue

            # Relative path inside the job directory
            rel_path = str(result_file.relative_to(job_path))
            dest_path = os.path.join(storage_root, rel_path)

            # Ensure destination directory exists
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            # Move the file
            shutil.move(str(result_file), dest_path)

            files_created.append(rel_path)

    # Also move merged parquet files produced at sub-analysis root
    sub_analysis_root = analysis.sub_output_dir.rstrip('/')
    if os.path.exists(sub_analysis_root):
        for parquet_file in pathlib.Path(sub_analysis_root).glob("*.parquet"):
            filename = parquet_file.name
            dest_path = os.path.join(storage_root, filename)
            # Ensure destination exists
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.move(str(parquet_file), dest_path)
            files_created.append(filename)

    logging.info("Done move_job_results_to_storage: %d files", len(files_created))

    return files_created


def _filter_list_remove_files_suffix(input_list: List[str], suffix: tuple[str, ...]) -> List[str]:
    filtered_list: List[str] = []
    was_filtered = False
    for file in input_list:
        lf = file.lower()
        if lf.endswith(suffix):
            filtered_list.append(os.path.dirname(file) + '/')
            was_filtered = True
        else:
            filtered_list.append(file)
    # deduplicate
    unique_filtered_list = list(set(filtered_list))
    if was_filtered:
        logging.debug("Filtered image file list to directories: %s", unique_filtered_list)
    return unique_filtered_list


def _filter_list_remove_imagefiles(files: List[str]) -> List[str]:
    suffix = ('.png', '.jpg', '.jpeg', '.tiff', '.tif')
    return _filter_list_remove_files_suffix(files, suffix)


def _list_results_files_relative(analysis: Analysis) -> List[str]:
    """Return all file paths relative to `analysis.results_dir`.

    Only files are returned (directories are skipped). If the results directory
    does not exist, an empty list is returned.
    """
    root = analysis.results_dir.rstrip('/')
    p = pathlib.Path(root)
    if not p.exists():
        return []
    rel_files: List[str] = []
    for f in p.rglob("*"):
        if f.is_file():
            rel_files.append(str(f.relative_to(p)))
    return rel_files


def insert_sub_analysis_results_to_db(analysis: Analysis, files_created: Optional[List[str]] = None) -> None:
    """
    Persist result metadata and finish timestamp for a sub-analysis.

    - If `files_created` is None, scan `analysis.results_dir` to discover files.
    - Paths are expected to be relative to `analysis.results_dir`.
    - We store `job_folder` (storage-relative prefix) and a filtered `file_list`
      with each path prefixed by `job_folder`.
    """
    # Discover files if caller didn't pass them explicitly
    if files_created is None:
        files_created = _list_results_files_relative(analysis)

    logging.info(
        "insert_sub_analysis_results_to_db: analysis=%s sub=%s files=%d",
        analysis.id, analysis.sub_id, len(files_created)
    )

    storage_paths = analysis.storage_paths
    job_folder_prefix = storage_paths.get('job_specific', '')

    # Prefix returned files with job_folder, then filter images to directories
    file_list_with_prefix = [job_folder_prefix + f for f in files_created]
    filtered_file_list = _filter_list_remove_imagefiles(file_list_with_prefix)

    result_payload = {
        'job_folder': job_folder_prefix,
        'file_list': filtered_file_list,
    }

    db = Database.get_instance()
    conn = None
    try:
        conn = db.get_connection()
        with conn.cursor() as cursor:
            query = (
                "UPDATE image_sub_analyses "
                "   SET result = %s, "
                "       finish = NOW() "
                " WHERE sub_id = %s"
            )
            cursor.execute(query, [json.dumps(result_payload), analysis.sub_id])
            conn.commit()
            logging.info("Updated image_sub_analyses for sub_id=%s", analysis.sub_id)
    except Exception as e:
        logging.error(
            "insert_sub_analysis_results_to_db failed; sub_id=%s analysis_id=%s",
            analysis.sub_id,
            analysis.id,
            exc_info=True,
        )
        if conn:
            conn.rollback()
    finally:
        if conn:
            db.release_connection(conn)


def handle_finished_analyses(cluster: str) -> None:
    """
    Walk unfinished parent analyses and mark them finished if all sub-analyses
    completed successfully, or mark them as errored if any sub-analysis failed.

    Logic adapted from cpp_old_server.handle_finished_analyses but implemented
    using the new Database singleton and Analysis/result schema.
    """
    logging.info(f"handle_finished_analyses: scanning unfinished parent analyses for cluster={cluster}")

    db = Database.get_instance()
    conn = None
    try:
        conn = db.get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            # Fetch all parent analyses that are neither finished nor errored for this run_location
            cursor.execute(
                "SELECT id FROM image_analyses "
                " WHERE finish IS NULL AND error IS NULL "
                "   AND (meta->>'run_location') = %s",
                [cluster],
            )
            parents = cursor.fetchall()
            if not parents:
                logging.debug("handle_finished_analyses: no unfinished parent analyses")
                return

            for parent in parents:
                parent_id = parent["id"]
                try:
                    # Load all sub-analyses for this parent
                    cursor.execute(
                        "SELECT sub_id, finish, error, result "
                        "  FROM image_sub_analyses "
                        " WHERE analysis_id = %s AND (meta->>'run_location') = %s",
                        [parent_id, cluster],
                    )
                    subs = cursor.fetchall()
                    if not subs:
                        logging.debug(
                            "Parent %s: no sub-analyses with run_location=%s; skipping finish check",
                            parent_id,
                            cluster,
                        )
                        continue

                    only_finished_subs = True
                    no_failed_subs = True
                    aggregated_files: List[str] = []
                    job_folder: Optional[str] = None

                    for sub in subs:
                        if not sub.get("finish"):
                            only_finished_subs = False
                        if sub.get("error"):
                            no_failed_subs = False

                    if only_finished_subs and no_failed_subs:
                        # Aggregate results across sub-analyses
                        for sub in subs:
                            res = sub.get("result")
                            if isinstance(res, dict):
                                files = res.get("file_list") or []
                                if files:
                                    aggregated_files.extend(files)
                                jf = res.get("job_folder")
                                if jf and not job_folder:
                                    job_folder = jf

                        # Deduplicate while preserving rough order
                        seen = set()
                        dedup_files = []
                        for f in aggregated_files:
                            if f not in seen:
                                seen.add(f)
                                dedup_files.append(f)

                        result_payload = {"file_list": dedup_files, "job_folder": job_folder or ""}

                        cursor.execute(
                            "UPDATE image_analyses "
                            "   SET finish = NOW(), result = %s "
                            " WHERE id = %s",
                            [json.dumps(result_payload), parent_id],
                        )
                        conn.commit()
                        logging.info(
                            "Marked analysis %s finished with %d files",
                            parent_id,
                            len(dedup_files),
                        )
                    elif not no_failed_subs:
                        # Any sub failed -> mark parent error using database helper
                        Database.get_instance().set_analysis_error(parent_id, "sub-analysis failure")
                        logging.info(
                            "Marked analysis %s as errored due to sub-analysis failure (cluster=%s)",
                            parent_id,
                            cluster,
                        )
                    else:
                        # Some sub still running; nothing to do now
                        logging.debug(
                            "Analysis %s not ready: only_finished=%s no_failed=%s",
                            parent_id,
                            only_finished_subs,
                            no_failed_subs,
                        )
                except Exception as e:
                    Database.get_instance().set_analysis_error(parent_id, "handle_finished_analyses: parent processing failed")
                    logging.error(
                        "handle_finished_analyses: parent processing failed; parent_id=%s cluster=%s",
                        parent_id,
                        cluster,
                        exc_info=True,
                    )
                    if conn:
                        conn.rollback()
    except Exception as e:
        logging.error("handle_finished_analyses failed", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            db.release_connection(conn)


def run_server_processing() -> None:
    """
    Execute a single server iteration: submit new analyses, pick up finished
    sub-analyses, finalize them, and optionally attempt to finish parent analyses.
    """
    try:
        cluster = get_hpc_cluster()
        logging.info("run_server_processing: using cluster=%s", cluster)

        # Submit or prepare new analyses for selected cluster
        handle_new_analyses(cluster)

        # Scan for finished sub-analyses (for selected cluster) and finalize them
        finished_subs = fetch_finished_subanalyses_hpc(cluster)
        for sub_id, job_list in finished_subs.items():
            if not job_list:
                logging.warning(f"Finished sub-analysis '{sub_id}' has empty job list; skipping finalize.")
                continue

            analysis = Database.get_instance().get_analysis(sub_id)
            if not analysis:
                logging.warning(f"Analysis for sub_id {sub_id} not found; skipping finalize.")
                continue

            try:
                move_job_results_to_storage(analysis, job_list)
                # Discover files directly from results dir to persist in DB
                insert_sub_analysis_results_to_db(analysis)
            except Exception as e:
                # Log with context and mark this sub-analysis as errored, then continue
                logging.error(
                    "finalize sub-analysis failed; sub_id=%s analysis_id=%s",
                    sub_id,
                    analysis.id if analysis else None,
                    exc_info=True,
                )
                Database.get_instance().set_sub_analysis_error(analysis, str(e))
                continue

        # Optionally mark full analyses finished for the selected cluster
        handle_finished_analyses(cluster)

        # Update database with hpc job status for the selected cluster
        hpc_utils.update_hpc_job_status(cluster)

    except (psycopg2.Error) as dberr:
        logging.exception(dberr)
        logging.error("run_server_processing: database error", exc_info=True)
    except Exception as e:
        logging.exception(e)
        logging.error("run_server_processing failed", exc_info=True)


def run_server_loop_continously(sleep_seconds: int = 10) -> None:
    """
    Run the continuous server loop, calling run_server_processing() and then sleeping.
    """
    while True:
        run_server_processing()
        logging.info(f"Sleeping for {sleep_seconds} sec")
        time.sleep(sleep_seconds)

def main():
    # Determine log level via env, falling back to DEBUG when DEBUG=1 is set
    # and INFO otherwise.
    log_level = _log_level_from_env(logging.INFO)
    setup_logging(log_level)
    logging.info("cpp_server starting; debug=%s", is_debug())

    init_new_db()

    # Start continuous loop; callers may import and call run_server_processing() directly.
    try:
        run_server_loop_continously(sleep_seconds=10)
    except KeyboardInterrupt:
        logging.info("cpp_server interrupted by user (Ctrl+C)")
    except Exception as e:
        logging.exception(e)
        logging.error("cpp_server crashed in main", exc_info=True)


if __name__ == "__main__":

    main()
