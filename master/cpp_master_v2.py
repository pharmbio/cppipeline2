#!/usr/bin/env python3



########### HEJ FRAMTIDEN ################
# todo:
# * get channel_map into the database
# * get plate acqusitions into the database
# * fetch db login info from secret
# * fetch only images that have not been analysed from a plate acqusition?
# * store the imgset file as a configmap for each job?
# * fix the job spec yaml, the command and mount paths (root vs user etc)
# * make sure the worker container image exists and works
# * build csv straight to file to reduce memory problems, need at least 32MB at moment



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
from database import Database
from database import Analysis


# divide a dict into smaller dicts with a set number of items in each
def chunk_dict(data, chunk_size=1):

    # create iterator of the dict
    it = iter(data)

    # for each step
    for i in range(0, len(data), chunk_size):

        # produce a dict with chunk_size items in it
        yield {k:data[k] for k in itertools.islice(it, chunk_size)}


# fetch all dependencies belongin to an analysis and check if they are finished
def all_dependencies_satisfied(analysis, cursor):

    # check if there are any dependencies
    if analysis['depends_on_sub_id']:

        dep_unsatified = False

        # unpack the dependency list
        # turn json list into a comma separated text of values as SQL wants it
        deps = ",".join(map(str, analysis['depends_on_sub_id']))

        # there are dependencies, fetch them from the db
        logging.debug('Fetching analysis dependencies.')
        cursor.execute(f'''
                            SELECT *
                            FROM image_sub_analyses
                            WHERE sub_id IN ({deps})
                           ''')
        dep_analyses = cursor.fetchall()

        # check dependencies and return true if they are all finished
        is_all_analyses_finished = check_analyses_finished(dep_analyses)
        return is_all_analyses_finished

    # there are no dependencies, good to go
    else:
        return True



# check if all analyses in a list are finished
def check_analyses_finished(analyses):

        logging.info("Inside check_analyses_finished")

        # check all dependencies
        for analysis in analyses:

            # if a dependency is not finished, return false
            if not analysis['finish']:
                return False

        # if they were all finished
        return True


# function for making a cellprofiler formatted csv file
def make_imgset_csv(imgsets, channel_map, storage_paths, use_icf):
    logging.info("Inside make_imgset_csv")

    ### create header row
    header = ""

    for ch_nr,ch_name in sorted(channel_map.items()):
        header += f"FileName_{ch_name}," #header += f"FileName_w{ch_nr}_{ch_name},"

    header += "Group_Index,Group_Number,ImageNumber,Metadata_Barcode,Metadata_Site,Metadata_Well,Metadata_AcqID,"

    for ch_nr,ch_name in sorted(channel_map.items()):
        header += f"PathName_{ch_name},"

    for ch_nr,ch_name in sorted(channel_map.items()):
        header += f"URL_{ch_name},"

    # Add Illumination correction headers if needed
    if use_icf:
        # First as URL_
        for ch_nr,ch_name in sorted(channel_map.items()):
            header += f"URL_ICF_{ch_name},"

        # And then as PathName_
        for ch_nr,ch_name in sorted(channel_map.items()):
            header += f"PathName_ICF_{ch_name},"

         # And then as FileName_
        for ch_nr,ch_name in sorted(channel_map.items()):
            header += f"FileName_ICF_{ch_name},"

    # remove last comma and add newline
    header = header[:-1]+"\n"
    ###

    # init counter
    content = ""

    # for each imgset
    for imgset_counter,imgset in enumerate(imgsets.values()):
        #pdb.set_trace()
        # construct the csv row
        row = ""

        # sort the images in the imgset by channel id
        sorted_imgset = sorted(imgset, key=lambda k: k['channel'])

        # add filenames
        for img in sorted_imgset:
            img_filename = os.path.basename(img['path'])
            row += f'"{img_filename}",'

        # add imgset info
        row += (
            f'{imgset_counter},1,{imgset_counter},'
            f'"{img["plate_barcode"]}",{img["site"]},'
            f'"{img["well"]}",{img["plate_acquisition_id"]},'
        )

        # add file paths
        for img in sorted_imgset:
            img_dirname = os.path.dirname(img['path'])
            row += f'"{img_dirname}",'

        # add file urls
        for img in sorted_imgset:
            path = img['path']
            row += f'"file:{path}",'


        # add illumination file names, both as URL_ and PATH_ - these are not uniqe per image,
        # all images with same channel have the same correction image
        if use_icf:

            icf_path = storage_paths['full']

            # First as URL
            for ch_nr,ch_name in sorted(channel_map.items()):
                icf_file_name = f'ICF_{ch_name}.npy'
                path = f'{icf_path}/{icf_file_name}'
                row +=  f'"file:{path}",'

            # Also as PathName_
            for ch_nr,ch_name in sorted(channel_map.items()):
                row +=  f'"{icf_path}",'

            # Also as FileName_
            for ch_nr,ch_name in sorted(channel_map.items()):
                icf_file_name = f'ICF_{ch_name}.npy'
                row +=  f'"{icf_file_name}",'

        # remove last comma and add a newline before adding it to the content
        content += row[:-1] + "\n"

    # return the header and the content
    return f"{header}{content}"



def get_cellprofiler_cmd_hpc(pipeline_file, imageset_file, output_path, job_timeout):

    cmd = (f' timeout {job_timeout}'
           f' cellprofiler'
           f' -r'
           f' -c'
           f' -p {pipeline_file}'
           f' --data-file {imageset_file}'
           f' -o {output_path}'
           f' --plugins-directory /CellProfiler/plugins')

    return cmd


def make_cellprofiler_yaml(cellprofiler_version, pipeline_file, imageset_file, output_path, job_name, analysis_id, sub_analysis_id, job_timeout, high_prioryty):

    if cellprofiler_version is None:
        cellprofiler_version = "v4.2.5-cellpose2.0"

    if high_prioryty:
        priority_class_name = "high-priority-cpp"
    else:
        priority_class_name = "low-priority-cpp"

    if is_debug():
       docker_image="ghcr.io/pharmbio/cpp_worker:" + cellprofiler_version + "-latest"
    else:
       docker_image="ghcr.io/pharmbio/cpp_worker:" + cellprofiler_version + "-stable"

    return yaml.safe_load(f"""

apiVersion: batch/v1
kind: Job
metadata:
  name: {job_name}
  namespace: cpp_debug
  labels:
    pod-type: cpp
    app: cpp-worker
    analysis_id: "{analysis_id}"
    sub_analysis_id: "{sub_analysis_id}"
  annotations:
    pipeline_file: "{pipeline_file}"
    imageset_file: "{imageset_file}"
    output_path: "{output_path}"
    job_timeout: "{job_timeout}"
    docker_image: "{docker_image}"

spec:
  backoffLimit: 1
  template:
    spec:
      nodeSelector:
        pipelineNode: "true"
      priorityClassName: {priority_class_name}
      containers:
      - name: cpp-worker
        image: {docker_image}
        imagePullPolicy: Always
        command: ["/cpp_worker.sh"]
        env:
        - name: PIPELINE_FILE
          value: {pipeline_file}
        - name: IMAGESET_FILE
          value: {imageset_file}
        - name: OUTPUT_PATH
          value: {output_path}
        - name: JOB_TIMEOUT
          value: "{job_timeout}"
        - name: OMP_NUM_THREADS # This is to prevent multithreading of cellprofiler
          value: "1"
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        #
        # I specify default resources in namespace file now
        #
        volumeMounts:
        - mountPath: /share/mikro/
          name: mikroimages
        - mountPath: /share/mikro2/
          name: mikroimages2
        #- mountPath: /root/.kube/
        #  name: kube-config
        - mountPath: /cpp_work
          name: cpp2
        #- mountPath: /cpp2_work
        #  name: cpp2
        - mountPath: /share/data/external-datasets
          name: externalimagefiles
      restartPolicy: Never
      volumes:
      - name: mikroimages
        persistentVolumeClaim:
          claimName: micro-images-pvc
      - name: mikroimages2
        persistentVolumeClaim:
          claimName: micro2-images-pvc
      #- name: cpp
      #  persistentVolumeClaim:
      #    claimName: cpp-pvc
      - name: cpp2
        persistentVolumeClaim:
          claimName: cpp2-pvc
      #- name: kube-config
      #  secret:
      #    secretName: cpp-user-kube-config
      - name: externalimagefiles
        persistentVolumeClaim:
          claimName: external-images-pvc

""")

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

    # fetch config
    cpp_config['dardel_user'] = cpp_config['dardel']['user']
    cpp_config['dardel_hostname'] = cpp_config['dardel']['hostname']
    cpp_config['uppmax_user'] = cpp_config['uppmax']['user']
    cpp_config['uppmax_hostname'] = cpp_config['uppmax']['hostname']


    return cpp_config


def connect_db(cpp_config):


    # connect to the db
    logging.info("Connecting to db.")
    connection = None
    connection = psycopg2.connect(  database=cpp_config['postgres']['db'],
                                    user=cpp_config['postgres']['user'],
                                    host=cpp_config['postgres']['host'],
                                    port=cpp_config['postgres']['port'],
                                    password=cpp_config['postgres']['password'])

    # make results into dicts
    cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    return connection, cursor


def generate_random_identifier(length):

    return ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(length))


def get_new_analyses(cursor):
    # ask for all new analyses
    logging.info('Inside get_new_analyses')
    query = '''
             SELECT *
             FROM image_sub_analyses
             WHERE start IS NULL
             ORDER by priority, sub_id
            '''
    logging.debug(query)
    cursor.execute(query)

    analyses = cursor.fetchall()

    return analyses


def handle_new_analyses(cursor, connection):
    logging.info('Inside handle_new_jobs')

    analyses = Database.get_instance().get_new_analyses()

    #logging.debug(f'analyses {analyses}')

    # first run through all and check for unstarted on hpc (dardel)
    for analysis in analyses:
        if analysis.is_cellprofiler_analysis:
            if analysis.is_run_on_dardel:
                logging.debug(f'is dardel analysis: {analysis.analysis_id}')
                # only run analyses that have satisfied dependencies
                if all_dependencies_satisfied(analysis.raw_data, cursor):
                    try:
                        handle_analysis_cellprofiler_dardel(analysis.raw_data, cursor, connection)
                    except Exception as e:
                        logging.error("Exception", e)
                        logging.warning(f"TODO: try to fail only this analysis {analysis.analysis_id}")

def get_channel_map(cursor, analysis):
    cursor.execute(f'''
                            SELECT *
                            FROM channel_map
                            WHERE map_id=(SELECT channel_map_id
                                          FROM plate_acquisition
                                          WHERE id={analysis['plate_acquisition_id']})
    ''')
    channel_map_res = cursor.fetchall()
    channel_map = {}
    for channel in channel_map_res:
        channel_map[channel['channel']] = channel['dye']

    return channel_map

def get_imgsets(cursor, acq_id, well_filter=None, site_filter=None):
        # fetch all images belonging to the plate acquisition
        logging.info('Fetching images belonging to plate acqusition.')

        query = ("SELECT *"
                 " FROM images_all_view"
                 " WHERE plate_acquisition_id=%s")

        if site_filter:
            query += f' AND site IN ({ ",".join( map( str, site_filter )) }) '

        if well_filter:
            query += ' AND well IN (' + ','.join("'{0}'".format(w) for w in well_filter) + ")"


        query += " ORDER BY timepoint, well, site, channel"

        logging.info("query: " + query)

        cursor.execute(query, (acq_id,))
        imgs = cursor.fetchall()

        imgsets = {}
        img_infos = {}
        for img in imgs:

            logging.debug(f'img: {img["path"]}')
            # readability
            imgset_id = f"{img['well']}-{img['site']}"

            # if it has been seen before
            try:
                imgsets[imgset_id] += [img['path']]
                img_infos[imgset_id] += [img]
            # if it has not been seen before
            except KeyError:
                imgsets[imgset_id] = [img['path']]
                img_infos[imgset_id] = [img]

        return imgsets, img_infos

def handle_analysis_cellprofiler_dardel(analysis, cursor, connection):

        analysis_id = analysis["analysis_id"]
        sub_analysis_id = analysis["sub_id"]
        sub_analysis_input_dir = f"/cpp_work/input/{sub_analysis_id}"

        cmd = prepare_analysis_cellprofiler_dardel(analysis, cursor)

        # write sbatch file to input dir (for debug and re-submit functionality)
        sbatch_out_file = f"{sub_analysis_input_dir}/sbatch.sh"
        logging.debug(f"sbatch_out_file: {sbatch_out_file}")
        with open(sbatch_out_file, "w") as file:
            file.write(cmd)

        # submit sbatch job via ssh
        job_id = exec_ssh_sbatch_cmd(cmd)

        if job_id:
            update_sub_analysis_status_to_db(connection, cursor, analysis_id, sub_analysis_id, f"submitted, job_id={job_id}")

            # when all chunks of the sub analysis are sent in, mark the sub analysis as started
            mark_analysis_as_started(cursor, connection, analysis['analysis_id'])
            mark_sub_analysis_as_started(cursor, connection, analysis['sub_id'])


def prepare_analysis_cellprofiler_dardel(analysis, cursor, ):

        logging.info(f"Inside handle_analysis_cellprofiler_dardel, analysis: {analysis}")

        analysis_id = analysis["analysis_id"]
        sub_analysis_id = analysis["sub_id"]

        # fetch the channel map for the acqusition
        logging.debug('Running channel map query.')

        channel_map = get_channel_map(cursor, analysis)

        # make sure channel map is populated
        if len(channel_map) == 0:
            raise ValueError('Channel map is empty, possible error in plate acqusition id.')

        # get analysis settings
        try:
            analysis_meta = analysis['meta']
        except KeyError:
            logging.error(f"Unable to get analysis_meta settings for analysis: sub_id={sub_analysis_id}")

        # check if sites filter is included
        site_filter = None
        if 'site_filter' in analysis_meta:
            site_filter = list(analysis_meta['site_filter'])

        # check if well filter is included
        well_filter = None
        if 'well_filter' in analysis_meta:
            well_filter = list(analysis_meta['well_filter'])

        # set images as image-sets
        imgsets, img_infos = get_imgsets(cursor, analysis['plate_acquisition_id'], well_filter, site_filter)

        # check if all imgsets should be in the same job
        try:
            chunk_size = analysis_meta['batch_size']
            pipeline_file = '/cpp_work/pipelines/' + analysis_meta['pipeline_file']
        except KeyError:
            logging.error(f"Unable to get batch_size details from analysis entry: sub_id={sub_analysis_id}")
            chunk_size = 1
        if chunk_size <= 0:
            # put them all in the same job if chunk size is less or equal to zero
            chunk_size = max(1, len(imgsets))

        # calculate the number of chunks that will be created
        n_imgsets = len(imgsets)
        n_jobs_unrounded = n_imgsets / chunk_size
        n_jobs = math.ceil(n_jobs_unrounded)

        # get common output for all sub analysis
        storage_paths = get_storage_paths_from_analysis_id(cursor, analysis_id)
        ## Make sure result dir exists
        #os.makedirs(f"{storage_paths['full']}", exist_ok=True)

        # create chunks and submit as separate jobs
        random_identifier = generate_random_identifier(8)
        all_cmds = []

        chunkesd_dict = chunk_dict(img_infos, chunk_size)
        for i,imgset_chunk in enumerate(chunkesd_dict):

            # generate names
            job_number = i
            job_id = create_job_id(analysis_id, sub_analysis_id, random_identifier, job_number, n_jobs)
            sub_analysis_input_dir = f"/cpp_work/input/{sub_analysis_id}"
            imageset_file = f"{sub_analysis_input_dir}/cpp-worker-job-{job_id}.csv"
            output_path = f"/cpp_work/output/{sub_analysis_id}/cpp-worker-job-{job_id}/"
            job_name = f"cpp-worker-job-{job_id}"

            # create folder if needed
            os.makedirs(sub_analysis_input_dir, exist_ok=True)

            logging.debug(f"job_timeout={analysis_meta.get('job_timeout')}")

            job_timeout = analysis_meta.get('job_timeout', "10800")
            priority = analysis_meta.get('priority', 0)
            if priority == 1:
                high_priority = True
            else:
                high_priority = False

            cellprofiler_cmd = get_cellprofiler_cmd_hpc(pipeline_file, imageset_file, output_path, job_timeout)

            # Check if icf headers should be added to imgset csv file, default is False
            use_icf = analysis_meta.get('use_icf', False)
            logging.debug("use_icf" + str(use_icf))
             # generate cellprofiler imgset file for this imgset
            imageset_content = make_imgset_csv(imgsets=imgset_chunk, channel_map=channel_map, storage_paths=storage_paths, use_icf=use_icf)

            # write csv
            with open(imageset_file, 'w') as file:
                logging.debug(f"write imgset file: {imageset_file}")
                file.write(imageset_content)

            all_cmds.append(cellprofiler_cmd)


        # write all cmds into a text file into input folder
        with open(f"{sub_analysis_input_dir}/cmds.txt", "w") as file:
            for item in all_cmds:
                file.write(item + "\n")

        # Write all results from dependant sub analyses into the input folder
        result_folder = f"{storage_paths['full']}"
        copy_dependant_results_to_input(sub_analysis_input_dir, result_folder)

        # create sbatch command and exec it on server via ssh
        sub_type = analysis_meta.get('sub_type', "undefifed")
        #logging.debug("sub_type" + str(sub_type))
        cmd = build_ssh_cmd_sbatch_dardel(sub_analysis_id, sub_type)

        return cmd


def build_ssh_cmd_sbatch_dardel(sub_id, sub_type):

    logging.info(f"Inside submit_sbatch_to_dardel: {sub_id}, sub_type {sub_type}")

    max_errors = 10

    account = "naiss2023-22-1320"

    if sub_type == "icf":
        partition = "shared"
        mem = "64GB"
        time = "15:00:00"  # it should only take about 10h, but I have noticed image load problems when running 20+ icf in parallell
        workers = 1

    elif sub_type == "qc":
        partition = "main"
        mem = "220GB"
        time = "10:00:00"
        workers = 100
    else:
        partition = "main"
        mem = "220GB" # "440GB"
        time = "01:00:00"
        workers = 60

    cpp_config = load_cpp_config()

    # Define your command as a string
    cmd = (f"ssh -o StrictHostKeyChecking=no"
           f" {cpp_config['dardel_user']}@{cpp_config['dardel_hostname']}"
           f" sbatch"
           f" --partition {partition}"
           f" --mem {mem}"
	       f" -t {time}"
           f" --account {account}"
	       f" --output=logs/{sub_id}-{sub_type}-slurm.%j.out"
           f" --error=logs/{sub_id}-{sub_type}-slurm.%j.out"
	       f" --chdir /cfs/klemming/home/a/andlar5/cppipeline2/dardel"
	       f" run_cellprofiler_singularity_dardel.sh"
	       f"    -d /cpp_work/input/{sub_id}"
           f"    -o /cpp_work/output/{sub_id}"
           f"    -w {workers}"
           f"    -e {max_errors}"
        )

    logging.debug(f"cmd: {cmd}")

    return cmd

def exec_ssh_sbatch_cmd(cmd):

    job_id = None

    try:
        # Execute the command and capture the output
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        logging.info(f"stdout: {output}")

        # Define a regular expression pattern to match the batch job ID
        pattern = r'Submitted batch job (\d+)'

        match = re.search(pattern, output)
        if match:
            job_id = match.group(1)
        else:
            logging.error(f"No batch job ID found in the std out, stdout: {output}")

    except subprocess.CalledProcessError as e:
        error_message = f"Error: {e.returncode}\n{e.output}"
        logging.error(error_message)

    return job_id

def get_all_dirs(path):
    # Create a Path object from the given path
    path_obj = pathlib.Path(path)

    # Check if the path exists and is a directory
    if not path_obj.exists() or not path_obj.is_dir():
        # Return None if the path is not a directory or does not exist
        return None

    # Initialize an empty list to hold the names of all directories
    dir_names = []

    # Iterate over each item in the directory
    for d in path_obj.iterdir():
        # Check if the item is a directory
        if d.is_dir():
            # Add the directory name to the list
            dir_names.append(d.name)

    # Return the list of directory names
    return dir_names


def fetch_finished_job_families_dardel(cursor, connection):
    logging.info("Inside fetch_finished_job_families_dardel")

    sql = (f"""
            SELECT *
            FROM image_sub_analyses
            WHERE finish IS null
            AND error IS null
            AND meta->>'run_on_dardel' IS NOT null
            """)

    logging.debug("sql: " + sql)

    cursor.execute(sql)

    unfinished_sub_analyses = cursor.fetchall()

    finished_jobs = {}
    for sub_analysis in unfinished_sub_analyses:
        sub_id = sub_analysis['sub_id']
        analysis_id = sub_analysis['analysis_id']
        # logging.debug(f"sub_analyses: {sub_id}")

        sub_analysis_out_path = f"/cpp_work/output/{sub_id}"
        #logging.debug(f'sub_analysis_out_path {sub_analysis_out_path}')

        # list all jobs in output
        if os.path.exists(sub_analysis_out_path):
            all_sub_analyses_jobs = get_all_dirs(sub_analysis_out_path)
            logging.debug(f'len(all_sub_analyses_jobs) {len(all_sub_analyses_jobs)}')
            for job in all_sub_analyses_jobs:

                job_path = os.path.join(sub_analysis_out_path, job)

                #logging.indebugfo(f'job_path {job_path}')

                if os.path.exists(os.path.join(job_path, "error")):
                    # skip this one
                    logging.debug(f"Error job {job}")
                elif os.path.exists(os.path.join(job_path, "finished")):
                    finished_jobs[job] = {"metadata": {"name": job, "sub_id": sub_id, "analysis_id": analysis_id}}
                    logging.debug(f"Job finished: {job}")

            logging.debug(f"Finished jobs after this sub {str(len(finished_jobs))}")


    logging.info("Finished jobs done " + str(len(finished_jobs)))

    # continue processing the finished jobs
    job_buckets = {}
    for job_name,job in finished_jobs.items():

        # get the family name
        job_family = get_job_family_from_job_name(job_name)

        # append all jobs with the same family name into a list
        try:
            job_buckets[job_family].append(job)

        except KeyError:
            job_buckets[job_family] = [job]


    logging.info("Finished buckets: " + str(len(job_buckets)))

    # fetch each familys total number of jobs and compare with the total count
    family_job_count = {}
    finished_families = {}
    for family_name, job_list in job_buckets.items():

        # save the total job count for this family
        family_job_count = get_family_job_count_from_job_name(job_list[0]['metadata']['name'])
        logging.info(f"fam-job-count: {family_job_count}\tfinished-job-list-len: {len(job_list)}")
        # check if there are as many finished jobs as the total job count for the family
        # for debug reasons we also check if the job limit is reached
        if family_job_count == len(job_list):

            # then all jobs in this family are finished and ready to be processed
            finished_families[family_name] = job_list


        # update progress
        done = len(job_list)
        total = family_job_count
        sub_id = job_list[0]['metadata']['sub_id']
        analysis_id = job_list[0]['metadata']['analysis_id']
        update_progress(connection, cursor, analysis_id, sub_id, done, total)



    logging.info("Finished families: " + str(len(finished_families)))
    return finished_families

def update_progress(connection, cursor, analysis_id, sub_id, done, total):

    start_time = get_sub_analysis_start(connection, cursor, sub_id)
    if not start_time:
        start_time = datetime.datetime.now()
    # Calculate elapsed time and average time per job
    elapsed_time = datetime.datetime.now() - start_time
    average_time_per_job = elapsed_time.total_seconds() / done
    jobs_remaining = total - done
    time_remaining = average_time_per_job * jobs_remaining
    hours_remaining = time_remaining / 3600  # Convert seconds to hours
    progress = f"{done} / {total} jobs finished, time remaining: {hours_remaining:.2f} hours"

    update_sub_analysis_progress_to_db(connection, cursor, analysis_id, sub_id, progress)


# goes through all jobs of a family i.e. merges the csvs with the same names into
# a single resulting csv file for the entire family, e.g. ..._Experiment.csv, ..._Image.csv
def merge_family_jobs_csv(family_name, job_list):

    logging.info("Inside merge_family_jobs_csv")

    logging.debug("job_list:" + str(job_list))

    # init
    merged_csvs = {}

    # for each job in the family
    for job in job_list:

        # fetch all csv files in the job folder
        analysis_sub_id = get_analysis_sub_id_from_family_name(family_name)
        job_path = f"/cpp_work/output/{analysis_sub_id}/{job['metadata']['name']}"
        for csv_file in pathlib.Path(job_path).rglob("*.csv"):

            # keep only the path relative to the job_path
            filename = str(csv_file).replace(job_path+'/', '')

            logging.debug("filename" + str(filename))

            # init the file entry if needed
            if filename not in merged_csvs:
                merged_csvs[filename] = {}
                merged_csvs[filename]['rows'] = []

            # read the csv
            with open(csv_file, 'r') as csv_file_handle:

                # svae the first row as header
                merged_csvs[filename]['header'] = csv_file_handle.readline()

                # append the remaining rows as content
                for row in csv_file_handle:
                    merged_csvs[filename]['rows'].append(row)

    logging.info("done merge_family_jobs_csv")

    return merged_csvs

def to32bit(t):
    return t.astype({c: str(t[c].dtype).replace('64', '32') for c in t.columns})

# goes through all jobs of a family i.e. merges the csvs with the same names into
# a single resulting csv file for the entire family, e.g. ..._Experiment.csv, ..._Image.csv

def merge_family_jobs_csv_to_parquet(analysis: Analysis, db: Database):

    logging.info("Inside merge_family_jobs_csv_to_parquet")

    # find all csv files in the sub-analayses folder
    # Put csv-files in dict of lists where dict-key is csv-filename (all files have same name
    # but are in different sub-dirs (job-dirs))
    all_csv_files = pathlib.Path(analysis.analysis_path).rglob("*.csv")
    filename_dict = {}
    for file in all_csv_files:
        filename = os.path.basename(file)
        file_list = filename_dict.setdefault(filename, [])
        file_list.append(file)

    # some files should not be concatenated but only one file should be copied
    # They are being put here into a separate dict and then one file is renemed to another extension than csv
    excludes = ["_experiment_", "_experiment.csv", 'Experiment.csv']
    filename_excluded = {}
    for exclude in excludes:
        for key in list(filename_dict.keys()):
            if exclude in key:
                filename_excluded[key] = filename_dict[key]
                del filename_dict[key]

    # concat all csv-files (per filename), loop filename(key)
    for filename in filename_dict.keys():

        start = time.time()

        files = filename_dict[filename]
        n = 0

        # create concat-csv with all files with current filename, e.g experiment, nuclei, cytoplasm
        is_header_already_included = False
        tmp_csvfile = os.path.join('/tmp/', filename + '.merged.csv.tmp')
        try:
            with open(tmp_csvfile, 'w') as csvout:
                for file in files:
                    with open(file, "r") as f:
                        # only include header once
                        if is_header_already_included:
                            next(f)
                        for row in f:
                            csvout.write(row)
                            is_header_already_included = True

                    if n % 500 == 0:
                        logging.info(f'{n}/{len(files)} {filename}')
                    n = n+1

            logging.debug(f'done concat csv {filename}')
            logging.debug(f"elapsed: {(time.time() - start):.3f}")
            pyarrow.set_cpu_count(5)
            logging.debug(f'start pd.read_csv {tmp_csvfile}')
            df = pd.read_csv(tmp_csvfile, engine='pyarrow')
            logging.debug(f'done pd.read_csv {tmp_csvfile}')
            os.remove(tmp_csvfile)
            logging.info(f'done concat {filename}')
            logging.info(f"elapsed: {(time.time() - start):.3f}")
            logging.info(f'start save as parquet {filename}')
            df = to32bit(df)
            parquetfilename = os.path.splitext(filename)[0] + '.parquet'
            parquetfile = os.path.join(analysis.analysis_path, parquetfilename)
            df.to_parquet(parquetfile)
            logging.info(f'done save as parquet {parquetfile}')
            logging.info(f"elapsed: {(time.time() - start):.3f}")

        except Exception as e:
            errormessage = f"Failed during concat csv files, error {e}"
            logging.error(errormessage)
            db.set_sub_analysis_error(analysis, errormessage)


            # delete all jobs for this sub_analysis
            delete_jobs(analysis.analysis_id)

        finally:
            if os.path.exists(tmp_csvfile):
                os.remove(tmp_csvfile)

    logging.info("done merge_family_jobs_csv_to_parquet")


def delete_jobs(analysis_sub_id):
    logging.info("TODO jsub analysis running jobs should be deleted here")

def copy_results_to_input(to_dir, from_dir):
    logging.info("Inside copy_results_to_input")
    # List of file suffixes to skip, in lower case for case-insensitive comparison
    skip_suffixes = ['.csv', '.log', '.png', '.tif', '.tiff', 'finished']

    # Check if the source (input) directory exists
    if not os.path.exists(from_dir):
        logging.debug(f"The source directory {from_dir} does not exist.")
        return  # Exit the function

    # Ensure the target (output) directory exists; if not, create it.
    if not os.path.exists(to_dir):
        os.makedirs(to_dir)
        logging.debug(f"The target directory {to_dir} was created.")

    # List all the files and directories in the source directory.
    for item in os.listdir(from_dir):
        source = os.path.join(from_dir, item)
        destination = os.path.join(to_dir, item)

        # Check if the current item is a file and not in the skip list.
        if os.path.isfile(source) and not any(source.lower().endswith(suffix) for suffix in skip_suffixes):
            # Copy the file to the destination directory.
            shutil.copy2(source, destination)  # copy2 preserves metadata
            logging.debug(f"File {source} copied to {destination}")
        elif os.path.isfile(source):
            # Log skipping of the file
            logging.debug(f"File {source} skipped due to suffix.")



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


# goes through all the non-csv filescsv of a family of job and copies the result to the result folder
def move_job_results_to_storage(family_name, job_list, storage_root):

    logging.info("Inside move_job_results_to_storage")

    skip_suffixes = ['.csv', '.log']
    skip_files = ['finished']

    files_created = []

    # for each job in the family
    for job in job_list:

        # fetch all files in the job folder
        analysis_sub_id = get_analysis_sub_id_from_family_name(family_name)
        job_path = f"/cpp_work/output/{analysis_sub_id}/{job['metadata']['name']}"
        for result_file in pathlib.Path(job_path).rglob("*"):

            logging.debug("move file: " + str(result_file))

            # exclude files with these extensions
            if result_file.suffix in skip_suffixes or result_file.name in skip_files or pathlib.Path.is_dir(result_file):
                logging.debug("Skipping file: " + str(result_file))
                continue

            # keep only the path relative to the job_path
            filename = str(result_file).replace(job_path+'/', '')

            # create a folder for the file if needed
            subdir_name = os.path.dirname(filename)
            os.makedirs(f"{storage_root['full']}/{subdir_name}", exist_ok=True)

            # move the file to the storage location
            shutil.move(f"{job_path}/{filename}", f"{storage_root['full']}/{filename}")

            # remember the file
            files_created.append(f"{filename}")

            logging.debug("done move file: " + str(filename))

    # move the concatenated output-csv that are in parquet format in sub-analysis dir
    sub_analysis_path = f"/cpp_work/output/{analysis_sub_id}/"
    for result_file in pathlib.Path(sub_analysis_path).glob("*.parquet"):

        # keep only the filename in result
        filename = pathlib.Path(result_file).name

        # move the file to the storage location
        shutil.move(f"{result_file}", f"{storage_root['full']}/{filename}")

        # remember the file
        files_created.append(f"{filename}")

        logging.debug("done copy file: " + str(filename))


    logging.info("Done move_job_results_to_storage")

    return files_created


def create_job_id(analysis_id, sub_analysis_id, random_identifier, job_number, n_jobs):
    return f"{sub_analysis_id}-{random_identifier}-{job_number}-{n_jobs}-{analysis_id}"

def get_family_job_count_from_job_name(job_name):
    match = re.match('cpp-worker-job-\d+-\w+-\d+-(\d+)', job_name)
    return int(match.groups()[0])

def get_job_family_from_job_name(job_name):
    match = re.match('(cpp-worker-job-\d+-\w+)', job_name)
    return match.groups()[0]

def get_analysis_sub_id_from_path(path):
    match = re.match('cpp-worker-job-(\w+)-', path)
    return int(match.groups()[0])

def get_analysis_sub_id_from_family_name(family_name):
    match = re.match('cpp-worker-job-(\w+)-', family_name)
    return int(match.groups()[0])

def get_analysis_id_from_family_name(family_name):
    match = re.match('cpp-worker-job-\d+-\w+-\d+-\d+-(\d+)', family_name)
    return int(match.groups()[0])

def get_analysis_from_db(cursor, analysis_id):

    # fetch all images belonging to the plate acquisition
    logging.info('Fetching plate info from view.')
    query = f"""
                        SELECT *
                        FROM image_analyses_v1
                        WHERE id='{analysis_id}'
                       """ # also NOT IN (select * from images_analysis where analysed=None) or something

    logging.debug(query)
    cursor.execute(query)
    analysis = cursor.fetchone()

    return analysis

def get_sub_analysis_from_db(cursor, analysis_sub_id):

    # fetch all images belonging to the plate acquisition
    logging.debug('Fetching plate info from view1')
    query = f"""
                        SELECT *
                        FROM image_sub_analyses_v1
                        WHERE sub_id=%s
            """            # also NOT IN (select * from images_analysis where analysed=None) or something

    mogrified_query = cursor.mogrify(query)

    # Convert byte string to a normal string and log it
    logging.debug(mogrified_query.decode('utf-8'))

    cursor.execute(query, (analysis_sub_id,))
    sub_analysis = cursor.fetchone()

    if sub_analysis is None:
        logging.error("sub_analysis is None, sub_id not found, should not be able to happen....")

    return sub_analysis

def update_sub_analysis_errormsg_to_db(connection, cursor, analysis_id, sub_analysis_id, errormessage):
    # TODO first get current errormessage, then append id
    update_progress_data_to_db(connection, cursor, analysis_id, sub_analysis_id, "error", errormessage)

def update_sub_analysis_status_to_db(connection, cursor, analysis_id, sub_analysis_id, status):
    update_progress_data_to_db(connection, cursor, analysis_id, sub_analysis_id, "status", status)

def update_sub_analysis_progress_to_db(connection, cursor, analysis_id, sub_analysis_id, progress):
    update_progress_data_to_db(connection, cursor, analysis_id, sub_analysis_id, "progress", progress)

def update_progress_data_to_db(connection, cursor, analysis_id, sub_analysis_id, data_key, data_value):
    logging.info(f"inside update_progress_data_to_db for {data_key}")

    data = f'{{"{data_key}": "{data_value}"}}'

    # update image_sub_analyses
    query = """UPDATE image_sub_analyses
               SET progress = progress || %s
               WHERE sub_id=%s
            """

    logging.debug("query:" + str(query))
    cursor.execute(query, [data, sub_analysis_id])
    connection.commit()

    # update image_analyses
    query = """UPDATE image_analyses
               SET progress = progress || %s
               WHERE id=%s
            """

    data = f'{{"{data_key}_{sub_analysis_id}": "{data_value}"}}'

    logging.debug("query:" + str(query))
    cursor.execute(query, [data, analysis_id])
    connection.commit()


def insert_sub_analysis_results_to_db(connection, cursor, sub_analysis_id, storage_root,  file_list):

    # fetch the result json
    query = f"""SELECT result
                FROM image_sub_analyses
                WHERE sub_id={sub_analysis_id};
            """
    cursor.execute(query)
    row = cursor.fetchone()

    # update the file list
    result = row['result']
    if not result:
        result = {}

    logging.debug("result:" + str(result))

    ### include the job specific folder name into file path

    # martin way
    # result['file_list'] = [storage_root['job_specific']+file_name for file_name in file_list]

    # anders way
    file_list_with_job_specific_path = []
    for file_name in file_list:
        file_name_with_job_specific_path = storage_root['job_specific'] + file_name
        file_list_with_job_specific_path.append(file_name_with_job_specific_path)
    result['job_folder'] = storage_root['job_specific']
    result['file_list'] = file_list_with_job_specific_path


    # Filter file list (remove individual png/tif files and only save path....)
    result['file_list'] = filter_list_remove_imagefiles(result['file_list'])

    # maybe in the future we should do a select first and
    query = f"""UPDATE image_sub_analyses
                SET result=%s,
                    finish=%s
                WHERE sub_id=%s
            """
    logging.debug("query:" + str(query))
    cursor.execute(query, [json.dumps(result), datetime.datetime.now(), sub_analysis_id])
    logging.debug("Before commit")
    connection.commit()
    logging.debug("Commited")

    delete_jobs(sub_analysis_id)

def filter_list_remove_imagefiles(list):
     suffix = ('.png','.jpg','.tiff','.tif')
     return filter_list_remove_files_suffix(list, suffix)

def filter_list_remove_files_suffix(input_list, suffix):

    filtered_list = []
    was_filtered = False
    for file in input_list:
        if file.lower().endswith(suffix):
            # remove filename and add path only to filtered list
            filtered_list.append(os.path.dirname(file) + '/')
            was_filtered = True
        else:
            filtered_list.append(file)

    unique_filtered_list = list(set(filtered_list))

    if was_filtered:
        logging.debug("unique_filtered_list" + str(unique_filtered_list))

    return unique_filtered_list



# go through unfinished analyses and wrap them up if possible (check if sub-analyses belonging to them are all finished)
def handle_finished_analyses(cursor, connection):

    logging.info("Inside handle_finished_analyses")

    # fetch all unfinished analyses
    cursor.execute(f"""
        SELECT *
        FROM image_analyses
        WHERE finish IS NULL AND error IS NULL
        """) # also NOT IN (select * from images_analysis where analysed=None) or something
    analyses = cursor.fetchall()


    # go through the unfinished analyses
    for analysis in analyses:

        ### check if all sub analyses for the analysis are finised

        # get all sub analysis belonging to the analysis

        # fetch all unfinished analyses


        sql = (f"""
            SELECT *
            FROM image_sub_analyses
            WHERE analysis_id={analysis['id']}
            """) # also NOT IN (select * from images_analysis where analysed=None) or something

        #logging.debug("sql" + sql)

        cursor.execute(sql)

        sub_analyses = cursor.fetchall()

        only_finished_subs = True
        no_failed_subs = True
        file_list = []
        # for each sub analysis
        for sub_analysis in sub_analyses:

            # check if it is unfinished
            if not sub_analysis['finish']:

                # if so, flag to move on to the next analysis
                only_finished_subs = False

            # check if it ended unsuccessfully
            if sub_analysis['error']:

                # if so, mark the analysis as failed and contact an adult
                no_failed_subs = False

            # else save the file list
            if only_finished_subs and no_failed_subs:
                file_list += sub_analysis['result']['file_list']
                job_folder = sub_analysis['result']['job_folder']


        # if all sub analyses were successfully finished, save the total file list and finish time in the db
        if only_finished_subs and no_failed_subs:

            # create the result dict
            result = {'file_list': file_list, 'job_folder': job_folder}

            # create timestamp
            finish = datetime.datetime.now()

            # construct query
            query = f""" UPDATE image_analyses
                        SET finish=%s,
                            result=%s
                        WHERE id=%s
            """
            cursor.execute(query, [finish, json.dumps(result), analysis['id'], ])
            connection.commit()


        # if any sub analysis failed, mark the analysis as failed as well
        elif not no_failed_subs:

            set_analysis_error(analysis['id'], cursor, connection )


def set_analysis_error(analysis_id, cursor, connection):
    # create timestamp
    error_time = str(datetime.datetime.now())

    # construct query
    query = f""" UPDATE image_analyses
                        SET error=%s
                        WHERE id=%s
            """
    cursor.execute(query, [error_time, analysis_id])
    connection.commit()


sub_anal_err_count = {}
job_error_set = set()
def handle_sub_analysis_error(cursor, connection, job):

    job_name = job['metadata']['name']

    # only deal with error once
    if job_name in job_error_set:
        return
    else:
        job_error_set.add(job_name)

    # get sub analysis id
    sub_analysis_id = get_analysis_sub_id_from_family_name(job_name)

    # get analysis id
    analysis_id = get_analysis_id_from_family_name(job_name)

    # increment error count for this sub analysis
    sub_anal_err_count[str(sub_analysis_id)] = sub_anal_err_count.get(str(sub_analysis_id), 0) + 1

    error_count = sub_anal_err_count[str(sub_analysis_id)]
    logging.debug("error_count: " + str(error_count))

    # get max_errors for this sub-analysis
    max_errors = get_sub_analysis_max_errors(cursor, connection, sub_analysis_id)

    if error_count > max_errors:
        logging.info(f"error_count is more than max_errors")

        # Check if failed already there
        if not has_sub_analysis_error(cursor, connection, sub_analysis_id):
            errormessage = f"error_count is more than max_errors"
            set_sub_analysis_error(cursor, connection, analysis_id, sub_analysis_id, errormessage)
            add_error_message_to_sub_analysis(cursor, connection, analysis_id, sub_analysis_id, errormessage)

        # delete all jobs for this sub_analysis
    else:
        errormessage = f"error in job {job_name}"
        add_error_message_to_sub_analysis(cursor, connection, analysis_id, sub_analysis_id, errormessage)

    logging.info("done with handle_sub_analysis_error")

def add_error_message_to_sub_analysis(cursor, connection, analysis_id, sub_analysis_id, errormessage):
    update_sub_analysis_errormsg_to_db(connection, cursor, analysis_id, sub_analysis_id, errormessage)


def set_sub_analysis_error(cursor, connection, analysis_id, sub_analysis_id, errormessage="no error message"):

    add_error_message_to_sub_analysis(cursor, connection, analysis_id, sub_analysis_id, errormessage)

    # Set error in sub analyses
    query = """ UPDATE image_sub_analyses
                        SET error=%s
                        WHERE sub_id=%s
            """
    cursor.execute(query, [str(datetime.datetime.now()), sub_analysis_id,])
    connection.commit()


def has_sub_analysis_error(cursor, connection, sub_analysis_id):

    # Set error in sub analyses
    query = """ SELECT error FROM image_sub_analyses
                WHERE sub_id=%s
    """
    cursor.execute(query, [sub_analysis_id,])

    has_error = cursor.fetchone()

    if has_error is None:
        return True
    else:
        return False

def get_sub_analysis_start(connection, cursor, sub_id):
    query = """ SELECT start FROM image_sub_analyses
                WHERE sub_id=%s
            """

    cursor.execute(query, [sub_id,])

    row = cursor.fetchone()

    if row:
        start = row['start']
        start = start.replace(tzinfo=None)
    else:
        start = None

    return start

def get_sub_analysis_max_errors(cursor, connection, sub_analysis_id):

    query = """ SELECT meta->>'max_errors' AS max_errors FROM image_sub_analyses
                WHERE sub_id=%s
    """

    cursor.execute(query, [sub_analysis_id,])
    row = cursor.fetchone()

    if row and row['max_errors']:
        max_errors = row['max_errors']
    else:
        max_errors = 0

    return int(max_errors)


def mark_analysis_as_started(cursor, connection, analysis_id):

    # only update start times where none is set
    query = """ UPDATE image_analyses
                SET start=%s
                WHERE id=%s
                AND start IS NULL
    """
    cursor.execute(query, [str(datetime.datetime.now()), analysis_id,])
    connection.commit()


def mark_sub_analysis_as_started(cursor, connection, sub_analysis_id):

    query = """ UPDATE image_sub_analyses
                SET start=%s
                WHERE sub_id=%s
    """
    cursor.execute(query, [str(datetime.datetime.now()), sub_analysis_id,])
    connection.commit()



def get_storage_paths_from_analysis_id(cursor, analysis_id):

    analysis_info = get_analysis_from_db(cursor, analysis_id)

    plate_barcode = analysis_info["plate_barcode"]
    acquisition_id = analysis_info["plate_acquisition_id"]

    return get_storage_paths(plate_barcode, acquisition_id, analysis_id)

def get_storage_paths_from_sub_analysis_id(cursor, sub_analysis_id):

    logging.debug("Inside get_storage_paths_from_sub_analysis_id")

    analysis_info = get_sub_analysis_from_db(cursor, sub_analysis_id)

    plate_barcode = analysis_info["plate_barcode"]
    acquisition_id = analysis_info["plate_acquisition_id"]
    analysis_id =  analysis_info["analyses_id"]

    return get_storage_paths(plate_barcode, acquisition_id, analysis_id)

def get_storage_paths(plate_barcode, acquisition_id, analysis_id):
    storage_paths = {
        "full": f"/cpp_work/results/{plate_barcode}/{acquisition_id}/{analysis_id}",
        "mount_point":"/cpp_work/",
        "job_specific":f"results/{plate_barcode}/{acquisition_id}/{analysis_id}/"
        }
    return storage_paths

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

        log_level = logging.DEBUG if is_debug() else logging.INFO
        setup_logging(log_level)

        logging.info("isdebug:" + str(is_debug()))

        cpp_config = load_cpp_config()

        init_new_db(cpp_config)

        # set file permissions on ssh key
        #os.chmod('/root/.ssh/id_rsa', 0o600)

        first_reset = True
        while True:

            # reset to avoid stale connections
            connection = None
            cursor = None

            try:

                # init connections
                connection, cursor = connect_db(cpp_config)

                handle_new_analyses(cursor, connection)

                finished_families = fetch_finished_job_families_dardel(cursor, connection)

                # merge finised jobs for each family (i.e. merge jobs for a sub analysis)
                for family_name, job_list in finished_families.items():

                    sub_analysis_id = get_analysis_sub_id_from_family_name(family_name)
                    analysis_id = get_analysis_id_from_family_name(family_name)

                    # final results should be stored in an analysis id based folder e.g. all sub ids beling to the same analyiss id sould be stored in the same folder
                    storage_paths = get_storage_paths_from_sub_analysis_id(cursor, sub_analysis_id)

                    # merge all job csvs into family csv
                    analysis = Database.get_instance().get_analysis_from_sub_id(sub_analysis_id)
                    files_created = merge_family_jobs_csv_to_parquet(analysis, Database.get_instance())

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
