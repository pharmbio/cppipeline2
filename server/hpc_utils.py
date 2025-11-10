import subprocess
import logging
import json
from datetime import datetime, timedelta
import re
import os
import glob
import math

from database import Database, Analysis
from config import CONFIG
import error_utils


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

def build_ssh_cmd_sbatch_hpc(analysis: Analysis, run_location: str):
    """
    Build sbatch command for the target cluster.

    Selection order:
    - use explicit run_location argument if provided
    - else use analysis.run_location property if available

    Supported locations include 'pelle', 'rackham', 'uppmax', 'hpc_dev', 'farmbio'.
    Currently mapped as:
    - 'pelle' -> pelle builder
    - 'rackham' -> rackham builder
    - 'hpc_dev' -> pelle builder (legacy dev cluster)
    - 'uppmax' -> pelle builder (adjust if you add a separate mapping)
    - 'farmbio' -> raise NotImplementedError until defined
    """
    if run_location == 'rackham':
        return build_ssh_cmd_sbatch_rackham(analysis)
    elif run_location == 'pelle':
        return build_ssh_cmd_sbatch_pelle(analysis)
    elif run_location == 'hpc_dev':
        return build_ssh_cmd_sbatch_pelle(analysis)
    elif run_location == 'uppmax':
        return build_ssh_cmd_sbatch_pelle(analysis)
    elif run_location == 'farmbio':
        raise NotImplementedError("run_location 'farmbio' mapping not implemented in build_ssh_cmd_sbatch_hpc")
    else:
        raise ValueError(f"No valid run location in build_ssh_cmd_sbatch_hpc, run_loc={run_location}")

def build_ssh_cmd_sbatch_pelle(analysis: Analysis):

    logging.info(f"Inside build_ssh_cmd_sbatch_pelle: {analysis.sub_id}, sub_type {analysis.sub_type}")

    cluster_cfg = CONFIG.cluster.get('pelle')
    if not cluster_cfg:
        raise Exception("cluster config is None for pelle")
    resources = cluster_cfg['resources']
    user = cluster_cfg['user']
    hostname = cluster_cfg['hostname']
    account = cluster_cfg['account']
    appdir = cluster_cfg['appdir']
    max_errors = 10

    # Resource subtype configuration based on sub_type
    resource_subtype = resources.get(analysis.sub_type, resources['default'])
    partition = resource_subtype['partition']

    # Always use integer ntasks; optional nodes/exclusive flags
    # Prefer 'ntasks' key, fallback to legacy 'nTasks'
    ntasks_cfg = int(resource_subtype.get('ntasks', resource_subtype.get('nTasks', 1)))
    nodes = resource_subtype.get('nodes')  # optional, integer
    exclusive = bool(resource_subtype.get('exclusive', False))  # optional
    nodes_opt = f" --nodes={int(nodes)}" if nodes is not None else ""
    exclusive_opt = " --exclusive" if exclusive else ""

    mem = resource_subtype.get('mem')
    workers_cfg = int(resource_subtype['workers'])
    version = analysis.cellprofiler_version

    # Derive dynamic resources from planned jobs
    n_jobs = count_planned_jobs(analysis)
    # minutes_per_wave can be configured via 'estimated_job_time' per job type.
    # If not present, fall back to legacy 'time' value; if neither, default to 60 minutes.
    def _parse_time_to_minutes(val) -> int | None:
        """Strictly parse time as H:MM:SS and return total minutes.

        Examples: "0:10:00", "10:00:00". Any other format is rejected.
        """
        if val is None:
            return None
        try:
            s = str(val).strip()
            m = re.match(r"^(\d{1,3}):(\d{2}):(\d{2})$", s)
            if not m:
                return None
            hours = int(m.group(1))
            mins = int(m.group(2))
            # seconds are ignored for minutes granularity
            return hours * 60 + mins
        except Exception:
            return None

    # Base estimate from cluster config
    est_job_time_cfg = resource_subtype.get('estimated_job_time', resource_subtype.get('time'))
    minutes_per_wave = _parse_time_to_minutes(est_job_time_cfg) or 60

    # Allow per-sub-analysis override via Analysis.estimated_job_time (format H:MM:SS)
    try:
        meta_override = analysis.estimated_job_time
        override_min = _parse_time_to_minutes(meta_override)
        if override_min is not None:
            minutes_per_wave = override_min
    except Exception:
        # Non-fatal: ignore malformed overrides
        logging.error("Ignoring invalid meta.estimated_job_time override: %s", analysis.estimated_job_time)

    # Estimated memory per concurrent job (worker), from config or fallback to 'mem'
    def _parse_mem_gb(mem_str: str | None) -> int | None:
        try:
            if not mem_str:
                return None
            s = str(mem_str).strip().upper()
            if s.endswith('GB'):
                return int(s[:-2])
            if s.endswith('G'):
                return int(s[:-1])
            return int(s)
        except Exception:
            return None

    est_job_mem_cfg = resource_subtype.get('estimated_job_mem', resource_subtype.get('mem'))
    mem_per_job_gb = _parse_mem_gb(est_job_mem_cfg)
    # Allow per-sub-analysis override via Analysis.estimated_job_mem (e.g., "5GB")
    try:
        mem_override = analysis.estimated_job_mem
        override_mem = _parse_mem_gb(mem_override)
        if override_mem is not None:
            mem_per_job_gb = override_mem
    except Exception:
        logging.error("Ignoring invalid meta.estimated_job_mem override: %s", analysis.estimated_job_mem)

    dyn = calculate_pelle_resources(
        n_jobs=n_jobs,
        workers_cfg=workers_cfg,
        minutes_per_wave=minutes_per_wave,
        mem_per_job_gb=mem_per_job_gb,
    )
    workers = dyn['workers']
    ntasks = dyn['ntasks'] if dyn['ntasks'] is not None else min(ntasks_cfg, workers)
    time = dyn['time']
    # Determine memory: dynamic recommendation from estimated_job_mem per worker,
    # capped to not exceed configured mem
    dyn_mem_gb = dyn.get('mem_gb')
    cfg_mem_gb = _parse_mem_gb(mem)
    if dyn_mem_gb is not None:
        final_mem_gb = min(dyn_mem_gb, cfg_mem_gb) if cfg_mem_gb is not None else dyn_mem_gb
        mem = f"{final_mem_gb}GB"

    ntasks_opt = f" --ntasks {ntasks}"


    # Command construction using f-string
    cmd = (f"ssh -o StrictHostKeyChecking=no {user}@{hostname} sbatch"
           f" --partition {partition}"
           f"{ntasks_opt}{nodes_opt}{exclusive_opt}"
           f" -t {time}"
           f"{f' --mem {mem}' if mem else ''}"
           f" --account {account}"
           f" --job-name=cpp_{analysis.id}_{analysis.sub_id}_{analysis.sub_type}"
           f" --output=logs/{analysis.sub_id}-{analysis.sub_type}-slurm.%j.out"
           f" --error=logs/{analysis.sub_id}-{analysis.sub_type}-slurm.%j.out"
           f" --chdir {appdir}"
           f" run_cellprofiler_apptainer_pelle.sh"
           f" -d /cpp_work/input/{analysis.sub_id}"
           f" -o /cpp_work/output/{analysis.sub_id}"
           f" -w {workers}"
           f" -e {max_errors}"
           f"{f' -V {version}' if version else ''}")

    logging.debug(f"cmd: {cmd}")
    return cmd

def count_planned_jobs(analysis: Analysis) -> int:
    """Count planned jobs for a sub-analysis by scanning input CSVs.

    Looks for files named 'cpp-worker-job-*.csv' in the sub input dir.
    Returns 0 if the directory is missing or no matches are found.
    """
    pattern = os.path.join(analysis.sub_input_dir, 'cpp-worker-job-*.csv')
    try:
        return len(glob.glob(pattern))
    except Exception:
        return 0


def _fmt_minutes_hms(minutes: int) -> str:
    minutes = max(1, int(minutes))
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}:00"


def calculate_pelle_resources(n_jobs: int, workers_cfg: int, minutes_per_wave: int = 60, mem_per_job_gb: int | None = None) -> dict:
    """Calculate dynamic resources for Pelle.

    - workers: min(workers_cfg, max(1, n_jobs))
    - waves: ceil(n_jobs / workers)
    - time: waves * minutes_per_wave (formatted HH:MM:00)
    - ntasks: may be set equal to workers by caller; we return None to allow caller to bound by config
    - mem_gb: dynamic memory recommendation (10GB per concurrent job/worker)
    """
    if workers_cfg is None or workers_cfg <= 0:
        raise ValueError(f"calculate_pelle_resources: workers_cfg must be > 0, got {workers_cfg}")
    if n_jobs is None or n_jobs <= 0:
        raise ValueError(f"calculate_pelle_resources: n_jobs must be > 0, got {n_jobs}")

    workers = max(1, min(workers_cfg, int(n_jobs)))
    waves = max(1, math.ceil(n_jobs / workers))
    time = _fmt_minutes_hms(waves * minutes_per_wave)
    # Memory recommendation: per concurrent job estimate (default 10GB/job)
    per_job = mem_per_job_gb if isinstance(mem_per_job_gb, int) and mem_per_job_gb > 0 else 10
    mem_gb = max(per_job * workers, 10)
    return {"workers": workers, "time": time, "ntasks": None, "mem_gb": mem_gb}

def build_ssh_cmd_sbatch_rackham(analysis: Analysis):

    logging.info(f"Inside build_ssh_cmd_sbatch_rackham: {analysis.sub_id}, sub_type {analysis.sub_type}")

    cluster_cfg = CONFIG.cluster.get('rackham')
    if not cluster_cfg:
        raise Exception("cluster config is None for raxkham")
    resources = cluster_cfg['resources']
    user = cluster_cfg['user']
    hostname = cluster_cfg['hostname']
    account = cluster_cfg['account']
    appdir = cluster_cfg['appdir']
    max_errors = 10

    # Resource subtype configuration based on sub_type
    resource_subtype = resources.get(analysis.sub_type, resources['default'])
    partition = resource_subtype['partition']
    # Always use integer ntasks; optional nodes/exclusive flags
    # Prefer 'ntasks' key, fallback to legacy 'nTasks'
    ntasks = int(resource_subtype.get('ntasks', resource_subtype.get('nTasks', 1)))
    nodes = resource_subtype.get('nodes')  # optional, integer
    exclusive = bool(resource_subtype.get('exclusive', False))  # optional
    nodes_opt = f" --nodes={int(nodes)}" if nodes is not None else ""
    exclusive_opt = " --exclusive" if exclusive else ""
    time = resource_subtype['time']
    mem = resource_subtype.get('mem')
    workers = resource_subtype['workers']
    version = analysis.cellprofiler_version

    # Command construction using f-string
    cmd = (f"ssh -o StrictHostKeyChecking=no {user}@{hostname} sbatch"
           f" --partition {partition}"
           f" --ntasks {ntasks}{nodes_opt}{exclusive_opt}"
           f" -t {time}"
           f"{f' --mem {mem}' if mem else ''}"
           f" --account {account}"
           f" --job-name=cpp_{analysis.id}_{analysis.sub_id}_{analysis.sub_type}"
           f" --output=logs/{analysis.sub_id}-{analysis.sub_type}-slurm.%j.out"
           f" --error=logs/{analysis.sub_id}-{analysis.sub_type}-slurm.%j.out"
           f" --chdir {appdir}"
           f" run_cellprofiler_apptainer_rackham.sh"
           f" -d /cpp_work/input/{analysis.sub_id}"
           f" -o /cpp_work/output/{analysis.sub_id}"
           f" -w {workers}"
           f" -e {max_errors}"
           f"{f' -v {version}' if version else ''}")

    logging.debug(f"cmd: {cmd}")
    return cmd

def exec_ssh_cmd(cmd: str) -> str:
    """
    Executes the SSH command to query the remote HPC cluster.
    """
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        logging.debug(f"Command output:\n{output}")
        return output
    except subprocess.CalledProcessError as e:
        logging.error("exec_ssh_cmd failed; cmd=%s", cmd, exc_info=True)
        raise


def exec_ssh_sbatch_cmd(cmd: str) -> str | None:

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
            logging.error("exec_ssh_sbatch_cmd: missing job id; stdout: %s", output)

    except subprocess.CalledProcessError as e:
        error_message = f"Error: {e.returncode}\n{e.output}"
        logging.error("exec_ssh_sbatch_cmd failed; cmd=%s; %s", cmd, error_message)
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

            logging.debug(f"Updating job {job_id} (analysis: {analysis_id}) with state {state}")

            # Get a connection from the connection pool
            conn = db.get_connection()
            with conn.cursor() as cursor:
                # Convert the job state to a JSON string
                status_json = json.dumps({"uppmax-status": state})

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

                logging.debug(f"Successfully updated job {job_id} with state {state}")
        except Exception as e:
            logging.error(
                "update_job_status_in_db failed; job_id=%s analysis_id=%s",
                job_id,
                job_status.get('analysis_id'),
                exc_info=True,
            )
        finally:
            # Always release the connection back to the pool
            if conn:
                db.release_connection(conn)

def update_hpc_job_status_all():
    return update_hpc_job_status_pelle()

def update_hpc_job_status_pelle():
    return update_hpc_job_status('pelle')


def update_hpc_job_status(cluster: str):
    """
    Update job statuses for the specified cluster.
    Fetches credentials from CONFIG.cluster[cluster] and updates DB rows
    for analyses associated with that run_location.
    """
    cluster_key = str(cluster)
    cluster_cfg = CONFIG.cluster.get(cluster_key)
    if not cluster_cfg:
        raise Exception(f"cluster config is None for {cluster_key}")
    user = cluster_cfg['user']
    hostname = cluster_cfg['hostname']

    logging.info(f"Inside update_hpc_job_status for cluster={cluster_key}")

    # Build commands and fetch outputs
    sacct_cmd = build_ssh_cmd_sacct(user, hostname)
    sacct_output = exec_ssh_cmd(sacct_cmd)
    sacct_job_statuses = parse_sacct_output(sacct_output)

    # Restrict updates to analyses in need of update on this cluster
    db = Database.get_instance()
    allowed_ids: set[int] = set()
    conn = None
    try:
        conn = db.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ia.id
                  FROM image_analyses ia
                  JOIN image_sub_analyses sa ON sa.analysis_id = ia.id
                 WHERE (ia.finish IS NULL
                     OR ia.finish > NOW() - INTERVAL '2000 minutes')
                   AND ia.error IS NULL
                   AND (sa.meta->>'run_location') = %s
                """,
                [cluster_key],
            )
            rows = cursor.fetchall()
            allowed_ids = {int(r[0]) for r in rows}
    except Exception as e:
        logging.error(
            "update_hpc_job_status: fetch active analyses failed; cluster=%s",
            cluster_key,
            exc_info=True,
        )
    finally:
        if conn:
            db.release_connection(conn)

    if not allowed_ids:
        logging.info(f"No active analyses to update for cluster {cluster_key}")
        return

    filtered_statuses = [js for js in sacct_job_statuses if int(js.get('analysis_id', -1)) in allowed_ids]
    if not filtered_statuses:
        logging.info(f"No matching job statuses for {cluster_key} among {len(sacct_job_statuses)} entries")
        return

    update_job_status_in_db(filtered_statuses, db)


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
