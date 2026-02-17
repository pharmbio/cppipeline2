import subprocess
import logging
import json
from datetime import datetime, timedelta
import re
import os
import glob
import math
import time

from dataclasses import dataclass
from typing import Optional
from database import Database, Analysis
from config import CONFIG
import error_utils


_CONSECUTIVE_SACCT_FAILURES = 0
_SACCT_FAILURE_SLACK_THRESHOLD = 3


def is_terminal_failure_state(state: Optional[str]) -> bool:
    if state is None:
        return False
    normalized = state.strip().upper()
    # Treat FAILED*, TIMEOUT*, and NODE_FAIL* as terminal failures.
    return (
        normalized.startswith("FAILED")
        or normalized.startswith("TIMEOUT")
        or normalized.startswith("NODE_FAIL")
    )


@dataclass(frozen=True)
class DynamicResources:
    workers: int
    ntasks: int
    time: str                # "H:MM:SS" TOTAL walltime for whole sbatch job (all waves)
    mem_gb: Optional[int]    # TOTAL memory for sbatch --mem (GB)

    @staticmethod
    def _parse_hhmmss_to_minutes(val) -> Optional[int]:
        if val is None:
            return None
        m = re.match(r"^(\d{1,3}):(\d{2}):(\d{2})$", str(val).strip())
        if not m:
            return None
        h = int(m.group(1))
        mm = int(m.group(2))
        return h * 60 + mm

    @staticmethod
    def _parse_gb(val) -> Optional[int]:
        if val is None:
            return None
        s = str(val).strip().upper()
        s = s.replace("GB", "G")
        if s.endswith("G"):
            s = s[:-1]
        try:
            return int(s)
        except ValueError:
            return None

    @staticmethod
    def _fmt_hhmmss(total_minutes: int) -> str:
        total_minutes = max(1, int(total_minutes))
        h = total_minutes // 60
        mm = total_minutes % 60
        return f"{h}:{mm:02d}:00"

    # ---- TIME rules ----

    @classmethod
    def _resolve_minutes_per_wave(cls, analysis, resource_subtype: dict) -> int:
        # analysis.estimated_job_time overrides subtype.estimated_job_time
        return (
            cls._parse_hhmmss_to_minutes(getattr(analysis, "estimated_job_time", None))
            or cls._parse_hhmmss_to_minutes(resource_subtype.get("estimated_job_time"))
            or 60
        )

    @classmethod
    def _resolve_max_total_time_minutes(cls, analysis, resource_subtype: dict) -> Optional[int]:
        # analysis.time overrides subtype.time
        return (
            cls._parse_hhmmss_to_minutes(getattr(analysis, "time", None))
            or cls._parse_hhmmss_to_minutes(resource_subtype.get("time"))
        )

    # ---- MEM rules ----

    @classmethod
    def _resolve_mem_per_worker_gb(cls, analysis, resource_subtype: dict) -> Optional[int]:
        # analysis.estimated_job_mem overrides subtype.estimated_job_mem
        return (
            cls._parse_gb(getattr(analysis, "estimated_job_mem", None))
            or cls._parse_gb(resource_subtype.get("estimated_job_mem"))
        )

    @classmethod
    def _resolve_max_total_mem_gb(cls, analysis, resource_subtype: dict) -> Optional[int]:
        # analysis.mem overrides subtype.mem  (TOTAL cap)
        return (
            cls._parse_gb(getattr(analysis, "mem", None))
            or cls._parse_gb(resource_subtype.get("mem"))
        )

    @classmethod
    def from_analysis_and_subtype(
        cls,
        *,
        analysis,
        resource_subtype: dict,
        n_jobs: int,
        workers_cfg: int,
        ntasks_cfg: int,
        time_multiplier: float = 1.15,
        time_overhead_min: int = 5,
        mem_overhead_gb: int = 1,
    ) -> "DynamicResources":
        n_jobs_eff = max(0, int(n_jobs))
        workers_cfg = max(1, int(workers_cfg))
        ntasks_cfg = max(1, int(ntasks_cfg))

        # Concurrency
        workers = max(1, min(workers_cfg, n_jobs_eff if n_jobs_eff > 0 else 1))
        ntasks = max(1, min(ntasks_cfg, workers))

        # Waves
        waves = max(1, math.ceil(n_jobs_eff / workers)) if n_jobs_eff > 0 else 1

        # ---- TOTAL TIME = waves * minutes_per_wave, capped by max_total_time ----
        minutes_per_wave = cls._resolve_minutes_per_wave(analysis, resource_subtype)
        est_total_min = int(math.ceil(waves * minutes_per_wave * time_multiplier + time_overhead_min))
        cap_total_min = cls._resolve_max_total_time_minutes(analysis, resource_subtype)
        total_min = min(est_total_min, cap_total_min) if cap_total_min is not None else est_total_min
        time_str = cls._fmt_hhmmss(total_min)

        # ---- TOTAL MEM = workers * mem_per_worker, capped by max_total_mem ----
        mem_per_worker_gb = cls._resolve_mem_per_worker_gb(analysis, resource_subtype)
        cap_total_mem_gb = cls._resolve_max_total_mem_gb(analysis, resource_subtype)

        mem_gb: Optional[int] = None
        if mem_per_worker_gb is not None:
            est_total_mem = max(1, int(workers * mem_per_worker_gb + mem_overhead_gb))
            mem_gb = min(est_total_mem, cap_total_mem_gb) if cap_total_mem_gb is not None else est_total_mem

        return cls(workers=workers, ntasks=ntasks, time=time_str, mem_gb=mem_gb)



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
    Includes JobID, JobName, State and Elapsed.
    """
    logging.info(f"Building SSH command for sacct")

    # Calculate start time (10 days ago) for historical job data retrieval
    start_time = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    # Include JobName in the sacct output format
    # The format order: JobID,JobName,State,Elapsed
    cmd = (f"ssh -o StrictHostKeyChecking=no {user}@{hostname} "
           f"'sacct --format=\"JobID,JobName%100,State,Elapsed\" --starttime={start_time} --user={user}'")

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
        return build_ssh_cmd_sbatch_hpc_for_cluster(analysis, 'pelle')
    elif run_location == 'hpc_dev':
        return build_ssh_cmd_sbatch_hpc_for_cluster(analysis, 'hpc_dev')
    elif run_location == 'uppmax':
        # Currently map uppmax jobs onto the pelle cluster configuration.
        return build_ssh_cmd_sbatch_hpc_for_cluster(analysis, 'pelle')
    elif run_location == 'farmbio':
        raise NotImplementedError("run_location 'farmbio' mapping not implemented in build_ssh_cmd_sbatch_hpc")
    else:
        raise ValueError(f"No valid run location in build_ssh_cmd_sbatch_hpc, run_loc={run_location}")


def build_ssh_cmd_sbatch_hpc_for_cluster(analysis: "Analysis", cluster_name: str) -> str:
    logging.info(
        "Inside build_ssh_cmd_sbatch_hpc_for_cluster: %s, id %s, sub_type %s",
        cluster_name, analysis.sub_id, analysis.sub_type
    )

    cluster_cfg = CONFIG.cluster.get(cluster_name)
    if not cluster_cfg:
        raise Exception(f"cluster config is None for cluster {cluster_name}")

    resources_cfg = cluster_cfg["resources"]
    user = cluster_cfg["user"]
    hostname = cluster_cfg["hostname"]
    account = cluster_cfg["account"]
    appdir = cluster_cfg["appdir"]
    max_errors = analysis.max_errors

    resource_subtype = resources_cfg.get(analysis.sub_type, resources_cfg["default"])

    partition = resource_subtype["partition"]
    workers_cfg = int(resource_subtype["workers"])
    ntasks_cfg = int(resource_subtype.get("ntasks", resource_subtype.get("nTasks", 1)))
    nodes = resource_subtype.get("nodes")
    exclusive = bool(resource_subtype.get("exclusive", False))

    n_jobs = count_planned_jobs(analysis)

    resources = DynamicResources.from_analysis_and_subtype(
        analysis=analysis,
        resource_subtype=resource_subtype,
        n_jobs=n_jobs,
        workers_cfg=workers_cfg,
        ntasks_cfg=ntasks_cfg,
    )

    version = analysis.cellprofiler_version

    nodes_opt = f" --nodes={int(nodes)}" if nodes is not None else ""
    exclusive_opt = " --exclusive" if exclusive else ""
    mem_opt = f" --mem {resources.mem_gb}GB" if resources.mem_gb is not None else ""
    version_opt = f" -V {version}" if version else ""

    cmd = (
        f"ssh -o StrictHostKeyChecking=no {user}@{hostname} sbatch"
        f" --partition {partition}"
        f" --ntasks {resources.ntasks}"
        f"{nodes_opt}{exclusive_opt}"
        f" -t {resources.time}"
        f"{mem_opt}"
        f" --account {account}"
        f" --job-name=cpp_{analysis.id}_{analysis.sub_id}_{analysis.sub_type}"
        f" --output=logs/{analysis.sub_id}-{analysis.sub_type}-slurm.%j.out"
        f" --error=logs/{analysis.sub_id}-{analysis.sub_type}-slurm.%j.out"
        f" --chdir {appdir}"
        f" run_cellprofiler_apptainer_pelle.sh"
        f" -d /cpp_work/input/{analysis.sub_id}"
        f" -o /cpp_work/output/{analysis.sub_id}"
        f" -w {resources.workers}"
        f" -e {max_errors}"
        f"{version_opt}"
    )

    logging.debug("cmd: %s", cmd)
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


def calculate_hpc_resources(n_jobs: int, workers_cfg: int, minutes_per_wave: int = 60, mem_per_job_gb: int | None = None) -> dict:
    """Calculate dynamic resources for HPC jobs (e.g. Pelle).

    - workers: min(workers_cfg, max(1, n_jobs))
    - waves: ceil(n_jobs / workers)
    - time: waves * minutes_per_wave (formatted HH:MM:00)
    - ntasks: may be set equal to workers by caller; we return None to allow caller to bound by config
    - mem_gb: dynamic memory recommendation (10GB per concurrent job/worker)
    """
    if workers_cfg is None or workers_cfg <= 0:
        raise ValueError(f"calculate_hpc_resources: workers_cfg must be > 0, got {workers_cfg}")
    if n_jobs is None or n_jobs <= 0:
        raise ValueError(f"calculate_hpc_resources: n_jobs must be > 0, got {n_jobs}")

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
    max_errors = analysis.max_errors

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
    Parses sacct output into a list of job statuses including job_id, state, elapsed, analysis_id, and sub_id.
    Skips job steps like batch or extern entries that are not primary jobs.
    """
    job_statuses = []
    # Regex for extracting analysis_id and sub_id from job name
    name_pattern = re.compile(r"cpp_(\d+)_(\d+)_")

    lines = output.splitlines()
    total_lines = len(lines)
    skipped_steps = 0
    unmatched_names = 0

    # Expected sacct format: JobID, JobName (width extended), State, Elapsed
    # The header line might look like: "JobID JobName State Elapsed ..."
    for line in lines[1:]:  # Skip the header
        parts = line.split()
        # We expect at least 4 parts: JobID, JobName, State, Elapsed
        if len(parts) >= 4:
            job_id = parts[0]
            job_name = parts[1]
            # Robust to potential additional trailing fields by indexing from the end
            state = parts[-2]
            elapsed = parts[-1]

            # Skip if job_id contains a dot, indicating a job step (e.g., .batch, .extern)
            if '.' in job_id:
                skipped_steps += 1
                continue

            # Check if job_name matches the cpp pattern
            match = name_pattern.search(job_name)
            if match:
                analysis_id, sub_id = match.groups()
                job_statuses.append({
                    "job_id": job_id.strip(),
                    "state": state.strip(),
                    "elapsed": elapsed.strip(),
                    "analysis_id": analysis_id,
                    "sub_id": sub_id
                })
            else:
                # JobName does not match the expected pattern, skip it
                unmatched_names += 1
                logging.debug(f"Skipping job with unmatched name: {job_name}")

    logging.debug(
        "parse_sacct_output: total_lines=%d, primary_jobs=%d, skipped_steps=%d, unmatched_names=%d",
        total_lines,
        len(job_statuses),
        skipped_steps,
        unmatched_names,
    )
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
            elapsed = job_status.get('elapsed')
            sub_id = job_status.get('sub_id')

            # Convert analysis_id to integer if it's not already
            analysis_id = int(job_status['analysis_id'])

            logging.debug(f"Updating job {job_id} (analysis: {analysis_id}) with state {state}")

            # Get a connection from the connection pool
            conn = db.get_connection()
            with conn.cursor() as cursor:
                # Convert the job state (and optional elapsed) to a JSON string
                status_payload = {"uppmax-status": state}
                if elapsed is not None:
                    status_payload["uppmax-elapsed"] = elapsed
                status_json = json.dumps(status_payload)

                # Use COALESCE to handle NULL status and treat it as an empty JSON object.
                # 1) Update parent analysis row
                query_parent = """
                    UPDATE image_analyses
                    SET status = COALESCE(status, '{}'::jsonb) || %s::jsonb
                    WHERE id = %s
                """

                # Log the exact query with substituted parameters using mogrify
                query_parent_with_params = cursor.mogrify(query_parent, [status_json, analysis_id]).decode('utf-8')
                logging.debug(f"Executing parent status update query: {query_parent_with_params}")

                # Execute the update query for parent analysis
                parent_start = time.time()
                cursor.execute(query_parent, [status_json, analysis_id])
                parent_elapsed = time.time() - parent_start
                logging.debug(
                    "Parent status UPDATE for analysis_id=%s completed in %.3fs (rowcount=%s)",
                    analysis_id,
                    parent_elapsed,
                    getattr(cursor, "rowcount", "n/a"),
                )

                # If sacct reports a terminal failure/timeout, mark the analysis and sub-analysis as errored.
                if is_terminal_failure_state(state):
                    err_msg = f"HPC sacct state {state} for job {job_id}"
                    logging.debug(
                        "Marking analysis_id=%s (sub_id=%s) as ERROR due to sacct state %s",
                        analysis_id,
                        sub_id,
                        state,
                    )

                    # Parent analysis: set error timestamp and attach error_message into result + status
                    result_patch = json.dumps({"error_message": err_msg[:500]})
                    cursor.execute(
                        """
                        UPDATE image_analyses
                           SET error = NOW(),
                               result = COALESCE(result, '{}'::jsonb) || %s::jsonb
                         WHERE id = %s
                        """,
                        [result_patch, analysis_id],
                    )
                    parent_status_patch = json.dumps({
                        "state": "ERROR",
                        "error_message": err_msg[:200],
                    })
                    cursor.execute(
                        """
                        UPDATE image_analyses
                           SET status = COALESCE(status, '{}'::jsonb) || %s::jsonb
                         WHERE id = %s
                        """,
                        [parent_status_patch, analysis_id],
                    )

                    # Sub-analysis: if we know sub_id, mark sub row as errored and
                    # add a concise error key on the parent status.
                    if sub_id is not None:
                        try:
                            sub_id_int = int(sub_id)
                            sub_result_patch = json.dumps({"error_message": err_msg[:500]})
                            cursor.execute(
                                """
                                UPDATE image_sub_analyses
                                   SET error = NOW(),
                                       result = COALESCE(result, '{}'::jsonb) || %s::jsonb
                                 WHERE sub_id = %s
                                """,
                                [sub_result_patch, sub_id_int],
                            )
                            sub_status_patch = json.dumps({
                                "state": "ERROR",
                                "error_message": err_msg[:200],
                            })
                            cursor.execute(
                                """
                                UPDATE image_sub_analyses
                                   SET status = COALESCE(status, '{}'::jsonb) || %s::jsonb
                                 WHERE sub_id = %s
                                """,
                                [sub_status_patch, sub_id_int],
                            )
                            parent_error_key_patch = json.dumps({
                                f"error_{sub_id_int}": err_msg[:200],
                            })
                            cursor.execute(
                                """
                                UPDATE image_analyses
                                   SET status = COALESCE(status, '{}'::jsonb) || %s::jsonb
                                 WHERE id = %s
                                """,
                                [parent_error_key_patch, analysis_id],
                            )
                        except Exception:
                            logging.error(
                                "Failed to mark sub-analysis error for sacct state; sub_id=%s",
                                sub_id,
                                exc_info=True,
                            )

                # 2) Update sub-analysis row, if we have a sub_id
                if sub_id is not None:
                    try:
                        sub_id_int = int(sub_id)
                        query_sub = """
                            UPDATE image_sub_analyses
                               SET status = COALESCE(status, '{}'::jsonb) || %s::jsonb
                               WHERE sub_id = %s
                        """
                        query_sub_with_params = cursor.mogrify(query_sub, [status_json, sub_id_int]).decode('utf-8')
                        logging.debug(f"Executing sub-analysis status update query: {query_sub_with_params}")
                        sub_start = time.time()
                        cursor.execute(query_sub, [status_json, sub_id_int])
                        sub_elapsed = time.time() - sub_start
                        logging.debug(
                            "Sub-analysis status UPDATE for sub_id=%s completed in %.3fs (rowcount=%s)",
                            sub_id_int,
                            sub_elapsed,
                            getattr(cursor, "rowcount", "n/a"),
                        )
                    except Exception:
                        logging.error("Failed to update sub-analysis status; sub_id=%s", sub_id, exc_info=True)

                commit_start = time.time()
                conn.commit()
                commit_elapsed = time.time() - commit_start
                logging.debug(
                    "update_job_status_in_db: COMMIT for analysis_id=%s completed in %.3fs",
                    analysis_id,
                    commit_elapsed,
                )

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

    # Build commands and fetch outputs. Sacct/SSH can be flaky, so we only
    # send Slack alerts if several consecutive calls fail.
    sacct_cmd = build_ssh_cmd_sacct(user, hostname)
    global _CONSECUTIVE_SACCT_FAILURES
    try:
        logging.debug("update_hpc_job_status: starting sacct ssh call")
        sacct_output = exec_ssh_cmd(sacct_cmd)
        logging.debug(
            "update_hpc_job_status: sacct ssh completed; output size=%d bytes",
            len(sacct_output) if isinstance(sacct_output, str) else -1,
        )
        _CONSECUTIVE_SACCT_FAILURES = 0
    except Exception as e:
        _CONSECUTIVE_SACCT_FAILURES += 1
        logging.warning(
            "update_hpc_job_status: sacct ssh failed (consecutive=%d)",
            _CONSECUTIVE_SACCT_FAILURES,
            exc_info=True,
        )
        if _CONSECUTIVE_SACCT_FAILURES >= _SACCT_FAILURE_SLACK_THRESHOLD:
            # Only send Slack after repeated failures
            error_utils.log_error(
                title="HPC sacct SSH failed repeatedly",
                err=e,
                context={
                    "cluster": cluster_key,
                    "consecutive_failures": _CONSECUTIVE_SACCT_FAILURES,
                    "cmd": sacct_cmd,
                },
                also_raise=False,
            )
        # Do not crash the server loop on transient HPC failures
        return

    sacct_job_statuses = parse_sacct_output(sacct_output)
    logging.info(
        "update_hpc_job_status: parsed %d sacct job rows for cluster=%s",
        len(sacct_job_statuses),
        cluster_key,
    )

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
    logging.info(
        "update_hpc_job_status: %d sacct rows, %d active analyses, %d rows match",
        len(sacct_job_statuses),
        len(allowed_ids),
        len(filtered_statuses),
    )
    if not filtered_statuses:
        logging.info(f"No matching job statuses for {cluster_key}; nothing to update in DB")
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
