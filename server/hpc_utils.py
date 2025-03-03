import subprocess
import logging
import json
from datetime import datetime, timedelta
import re

from database import Database, Analysis
from config import CONFIG


def build_ssh_cmd_squeue(user: str, hostname: str) -> str:
    """
    Builds the SSH command for executing squeue on the remote cluster.
    """
    logging.info(f"Building SSH command for squeue")
    cmd = (f"ssh -o StrictHostKeyChecking=no {user}@{hostname} "
           f"'squeue --format=\"%.12i %.10T\" --user={user}'")
    logging.debug(f"squeue SSH Command: {cmd}")
    return cmd

def build_ssh_cmd_sacct(user: str, hostname: str) -> str:
    """
    Builds the SSH command for executing sacct on the remote cluster.
    Includes JobID, JobName, and State.
    """
    logging.info(f"Building SSH command for sacct")

    # Calculate start time (10 days ago) for historical job data retrieval
    start_time = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

    # Include JobName in the sacct output format
    # The format order: JobID,JobName,State
    cmd = (f"ssh -o StrictHostKeyChecking=no {user}@{hostname} "
           f"'sacct --format=\"JobID,JobName%100,State\" --starttime={start_time} --user={user}'")

    logging.debug(f"sacct SSH Command: {cmd}")
    return cmd

def build_ssh_cmd_sbatch_hpc(analysis: Analysis):
    return build_ssh_cmd_sbatch_dardel(analysis)

def build_ssh_cmd_sbatch_dardel(analysis: Analysis):

    logging.info(f"Inside build_ssh_cmd_sbatch_dardel: {analysis.sub_id}, sub_type {analysis.sub_type}")

    dardel_cfg = CONFIG.cluster.get('dardel')
    resources = dardel_cfg['resources']
    user = dardel_cfg['user']
    hostname = dardel_cfg['hostname']
    account = dardel_cfg['account']
    max_errors = 10

    # Resource subtype configuration based on sub_type
    resource_subtype = resources.get(analysis.sub_type, resources['default'])
    partition = resource_subtype['partition']
    nTasks = resource_subtype.get('nTasks')
    mem = resource_subtype['mem']
    time = resource_subtype['time']
    workers = resource_subtype['workers']

    # Command construction using f-string
    cmd = (f"ssh -o StrictHostKeyChecking=no {user}@{hostname} sbatch"
           f" --partition {partition}"
           f" --mem {mem}"
           f"{f' -n {nTasks}' if nTasks is not None else ''}"
           f" -t {time}"
           f" --account {account}"
           f" --job-name=cpp_{analysis.id}_{analysis.sub_id}_{analysis.sub_type}"
           f" --output=logs/{analysis.sub_id}-{analysis.sub_type}-slurm.%j.out"
           f" --error=logs/{analysis.sub_id}-{analysis.sub_type}-slurm.%j.out"
           f" --chdir /cfs/klemming/home/a/andlar5/cppipeline2/dardel"
           f" run_cellprofiler_singularity_dardel.sh"
           f" -d /cpp_work/input/{analysis.sub_id}"
           f" -o /cpp_work/output/{analysis.sub_id}"
           f" -w {workers}"
           f" -e {max_errors}")

    logging.debug(f"cmd: {cmd}")
    return cmd

def exec_ssh_cmd(cmd: str) -> str:
    """
    Executes the SSH command to query the remote HPC cluster.
    """
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        logging.info(f"Command output: {output}")
        return output
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing SSH command: {e.output}")
        raise


def exec_ssh_sbatch_cmd(cmd: str) -> str:

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
        raise e

    return job_id

# Regex to extract analysis_id and sub_id from the job name
name_pattern = re.compile(r"cpp_(\d+)_(\d+)_")

def parse_squeue_output(output: str) -> list[dict]:
    """
    Parses squeue output into a list of job statuses including job_id, state, analysis_id, and sub_id.
    """
    job_statuses = []
    for line in output.splitlines()[1:]:  # Skip the header line
        parts = line.split(maxsplit=4)
        # Expected parts: [JOBID, PARTITION, NAME, USER, STATE]
        if len(parts) >= 5:
            job_id = parts[0].strip()
            name = parts[2].strip()
            state = parts[4].strip()

            # Extract analysis_id and sub_id from the job name
            match = name_pattern.search(name)
            if match:
                analysis_id, sub_id = match.groups()
                job_statuses.append({
                    "job_id": job_id,
                    "state": state,
                    "analysis_id": analysis_id,
                    "sub_id": sub_id
                })
            else:
                logging.warning(f"Job name format invalid for squeue line: {name}")
    return job_statuses

def parse_sacct_output(output: str) -> list[dict]:
    """
    Parses sacct output into a list of job statuses including job_id, state, analysis_id, and sub_id.
    Skips job steps like batch or extern entries that are not primary jobs.
    """
    job_statuses = []
    # Regex for extracting analysis_id and sub_id from job name
    name_pattern = re.compile(r"cpp_(\d+)_(\d+)_")

    # Expected sacct format: JobID, JobName (width extended), State
    # The header line might look like: "JobID JobName State ..."
    for line in output.splitlines()[1:]:  # Skip the header
        parts = line.split()
        # We expect at least 3 parts: JobID, JobName, State
        if len(parts) >= 3:
            job_id = parts[0]
            job_name = parts[1]
            state = parts[-1]  # State should be the last field

            # Skip if job_id contains a dot, indicating a job step (e.g., .batch, .extern)
            if '.' in job_id:
                continue

            # Check if job_name matches the cpp pattern
            match = name_pattern.search(job_name)
            if match:
                analysis_id, sub_id = match.groups()
                job_statuses.append({
                    "job_id": job_id.strip(),
                    "state": state.strip(),
                    "analysis_id": analysis_id,
                    "sub_id": sub_id
                })
            else:
                # JobName does not match the expected pattern, skip it
                logging.debug(f"Skipping job with unmatched name: {job_name}")

    return job_statuses


def update_job_status_in_db(job_statuses: list[dict], db: 'Database'):
    """
    Updates the job statuses in the database by using the analysis_id as the key.

    Args:
        job_statuses (list[dict]): A list of dictionaries containing job_id (str), state (str), and analysis_id (str or int).
        db (Database): A database instance to update the statuses.
    """
    for job_status in job_statuses:
        try:
            job_id = job_status['job_id']
            state = job_status['state']

            # Convert analysis_id to integer if it's not already
            analysis_id = int(job_status['analysis_id'])

            logging.info(f"Updating job {job_id} (analysis: {analysis_id}) with state {state}")

            # Get a connection from the connection pool
            conn = db.get_connection()
            with conn.cursor() as cursor:
                # Convert the job state to a JSON string
                status_json = json.dumps({"status": state})

                # Use COALESCE to handle NULL status and treat it as an empty JSON object.
                query = """
                    UPDATE image_analyses
                    SET status = COALESCE(status, '{}'::jsonb) || %s::jsonb
                    WHERE id = %s
                """

                # Log the exact query with substituted parameters using mogrify
                query_with_params = cursor.mogrify(query, [status_json, analysis_id]).decode('utf-8')
                logging.debug(f"Executing query: {query_with_params}")

                # Execute the update query
                cursor.execute(query, [status_json, analysis_id])
                conn.commit()

                logging.info(f"Successfully updated job {job_id} with state {state}")
        except Exception as e:
            logging.error(f"Error updating job {job_id}: {e}")
        finally:
            # Always release the connection back to the pool
            if conn:
                db.release_connection(conn)

def handle_new_analyses_and_update_status(db, user, hostname):
    # Step 1: Fetch running/pending jobs from squeue
    squeue_cmd = hpc_utils.build_ssh_cmd_squeue(user, hostname)
    squeue_output = hpc_utils.exec_ssh_cmd(squeue_cmd)
    squeue_job_statuses = hpc_utils.parse_squeue_output(squeue_output)
    hpc_utils.update_job_status_in_db(squeue_job_statuses, db)

    # Step 2: Fetch completed/canceled jobs from sacct
    sacct_cmd = hpc_utils.build_ssh_cmd_sacct(user, hostname)
    sacct_output = hpc_utils.exec_ssh_cmd(sacct_cmd)
    sacct_job_statuses = hpc_utils.parse_sacct_output(sacct_output)
    hpc_utils.update_job_status_in_db(sacct_job_statuses, db)


def get_cellprofiler_cmd_hpc(pipeline_file: str, imageset_file: str, output_path: str, job_timeout: int) -> str:

    cmd = (f' timeout {job_timeout}'
           f' cellprofiler'
           f' -r'
           f' -c'
           f' -p {pipeline_file}'
           f' --data-file {imageset_file}'
           f' -o {output_path}'
           f' --plugins-directory /CellProfiler/plugins')

    return cmd
