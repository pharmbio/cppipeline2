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
from typing import List, Dict, Any

from database import Database
from database import Analysis
from file_sync_utils import SyncManager
import hpc_utils
from config import CONFIG

def create_job_id(analysis_id, sub_analysis_id, random_identifier, job_number, n_jobs):
    return f"{sub_analysis_id}-{random_identifier}-{job_number}-{n_jobs}-{analysis_id}"

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
        "Group_Index", "Group_Number", "ImageNumber",
        "Metadata_Barcode", "Metadata_Site", "Metadata_Well", "Metadata_AcqID"
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

def is_debug():
    """
    Check if the users has the debug env.var. set
    """
    debug = False
    if os.environ.get('DEBUG'):
        debug = True

    return debug


def load_cpp_config():

    if is_debug():
        with open('debug_configs.yaml', 'r') as configs_debug:
            cpp_config = yaml.load(configs_debug, Loader=yaml.FullLoader)

    else:
        logging.error("Only debug profile for now, exit here")
        exit()

    cpp_config['postgres']['password'] = os.environ.get('DB_PASS')

    # # fetch config
    # cpp_config['dardel_user'] = cpp_config['dardel']['user']
    # cpp_config['dardel_hostname'] = cpp_config['dardel']['hostname']
    # cpp_config['uppmax_user'] = cpp_config['uppmax']['user']
    # cpp_config['uppmax_hostname'] = cpp_config['uppmax']['hostname']


    return cpp_config


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


def handle_new_analyses(cursor, connection):
    logging.info('Inside handle_new_jobs')

    analyses = Database.get_instance().get_new_analyses()

    # first run through all and check for unstarted on hpc (dardel)
    for analysis in analyses:
        if analysis.is_cellprofiler_analysis:
            if analysis.is_run_on_hpcdev:
                logging.debug(f'is hpc dev: {analysis.id}')
                # only run analyses that have satisfied dependencies
                if analysis.all_dependencies_satisfied():
                    try:
                        handle_analysis_cellprofiler_hpc(analysis)
                    except Exception as e:
                        logging.error("Exception", e)
                        logging.warning(f"TODO: try to fail only this analysis {analysis.id}")


def handle_analysis_cellprofiler_hpc(analysis: Analysis):

        cmd = prepare_analysis_cellprofiler_hpc(analysis)

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
        job_id = hpc_utils.exec_ssh_sbatch_cmd(cmd)

        if job_id:
            update_sub_analysis_status_to_db(connection, cursor, analysis.id(), analysis.sub_id(), f"submitted, job_id={job_id}")

            # when all chunks of the sub analysis are sent in, mark the sub analysis as started
            mark_analysis_as_started(cursor, connection, analysis.id())
            mark_sub_analysis_as_started(cursor, connection, analysis.sub_id())


def prepare_analysis_cellprofiler_hpc(analysis: Analysis):

        logging.info(f"Inside prepare_analysis_cellprofiler_hpc, analysis: {analysis.raw_data}")

        # get cellprofiler-version
        logging.debug(f"pipeline: {analysis.pipeline_file}")
        logging.debug(f"cp version: {analysis.cellprofiler_version}")

        # group imagesets into batches and create one cellprofiler job/command per batch
        imgset_batches = analysis.get_imgset_batches()
        logging.debug(f"imgset_batches: {imgset_batches}")

        channel_map = analysis.get_channelmap()
        logging.debug(f"channelmap: {channel_map}")

        random_identifier = generate_random_identifier(8)
        all_cmds = []
        n_jobs = len(analysis.get_all_imgsets())
        for i, imgsets in enumerate(imgset_batches):
            # generate names
            job_number = i
            job_id = create_job_id(analysis.id, analysis.sub_id, random_identifier, job_number, n_jobs)
            job_name = f"cpp-worker-job-{job_id}"
            imageset_file = f"{analysis.sub_input_dir}/{job_name}.csv"
            job_yaml_file = f"{analysis.sub_input_dir}/{job_name}.yaml"
            job_output_path = f"{analysis.sub_output_dir}/{job_name}/"

            logging.debug(f"job_timeout={analysis.job_timeout}")

            job_timeout = analysis.job_timeout
            priority = analysis.priority
            logging.debug(f"priority: {priority}")
            logging.debug(f"job_timeout: {job_timeout}")


            if priority == 1:
                high_priority = True
            else:
                high_priority = False

            cellprofiler_cmd = hpc_utils.get_cellprofiler_cmd_hpc(analysis.pipeline_file,
                                                                  imageset_file,
                                                                  job_output_path,
                                                                  job_timeout)

            # Check if icf headers should be added to imgset csv file, default is False
            use_icf = analysis.use_icf
            logging.debug(f"use_icf: {use_icf}")
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
        with open(f"{analysis.sub_input_dir}/cmds.txt", "w") as file:
            for item in all_cmds:
                file.write(item + "\n")

        # write a stage list for all images in all imgsets
        with open(f"{analysis.sub_input_dir}/stage_images.txt", "w") as file:
            for key, imgset in analysis.get_all_imgsets().items():
                logging.debug(f"imgset_key: {key}")
                for image in imgset:
                    logging.debug(f"path: {image.path}")
                    file.write(image.path + "\n")

        # Make sure analysis output dir exists
        os.makedirs(f"{analysis.results_dir}", exist_ok=True)
        # Write all results from dependant sub analyses into the input dir
        copy_dependant_results_to_input(analysis.sub_input_dir, analysis.results_dir)

        cmd = hpc_utils.build_ssh_cmd_sbatch_hpc(analysis)

        return cmd

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
        logging.getLogger('').addHandler(console)

def init_new_db(cpp_config):
     # Initialize the connection pool
     Database.get_instance().initialize_connection_pool(
                    dbname=cpp_config['postgres']['db'],
                    user=cpp_config['postgres']['user'],
                    host=cpp_config['postgres']['host'],
                    port=cpp_config['postgres']['port'],
                    password=cpp_config['postgres']['password']
        )

def main():

    try:

        log_level = logging.INFO if is_debug() else logging.INFO
        setup_logging(log_level)

        logging.info("isdebug:" + str(is_debug()))

        cpp_config = load_cpp_config()

        init_new_db(cpp_config)

        while True:

            try:

                handle_new_analyses(cursor, connection)

                finished_families = fetch_finished_job_families_dardel(cursor, connection)

                # merge finised jobs for each family (i.e. merge jobs for a sub analysis)
                for family_name, job_list in finished_families.items():

                    sub_analysis_id = get_analysis_sub_id_from_family_name(family_name)
                    #analysis_id = get_analysis_id_from_family_name(family_name)

                    # final results should be stored in an analysis id based folder e.g. all sub ids beling to the same analyiss id sould be stored in the same folder
                    storage_paths = get_storage_paths_from_sub_analysis_id(cursor, sub_analysis_id)

                    # merge all job csvs into family csv
                    #analysis = Database.get_instance().get_analysis_from_sub_id(sub_analysis_id)
                    #files_created = merge_family_jobs_csv_to_parquet(analysis, Database.get_instance())

                    # move all files to storage, e.g. results folder
                    files_created = move_job_results_to_storage(family_name, job_list, storage_paths)

                    # insert csv to db
                    insert_sub_analysis_results_to_db(connection, cursor, sub_analysis_id, storage_paths, files_created)


                # check for finished analyses
                handle_finished_analyses(cursor, connection)


            # catch db errors
            except (psycopg2.Error) as error:
                logging.exception(error)

            finally:
                #closing database connection
                if connection:
                    cursor.close()
                    connection.close()


            #print('Exit because single run debug')
            #exit()

            sleeptime = 20
            logging.info(f"Going to sleep for {sleeptime} sec")
            time.sleep(sleeptime)

    # Catch all errors
    except Exception as e:
        logging.error("Exception", e)


if __name__ == "__main__":

    main()
