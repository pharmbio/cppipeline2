from __future__ import annotations
import os
import pathlib
import logging
from dataclasses import dataclass
from typing import Optional, Any, List, Dict, Tuple
from database import Analysis

JOB_FOLDER_PREFIX = "cpp-worker-job-"

@dataclass(frozen=True)
class JobName:
    sub_id: int
    job_number: int
    n_jobs: int
    analysis_id: int

    @property
    def suffix(self) -> str:
        """The part after the prefix used for folder names."""
        return f"{self.sub_id}-{self.job_number}-{self.n_jobs}-{self.analysis_id}"

    def to_folder_name(self) -> str:
        """Full job folder name including prefix."""
        return f"{JOB_FOLDER_PREFIX}{self.suffix}"

    @classmethod
    def parse_folder_name(cls, name: str) -> Optional[JobName]:
        """
        Parse a folder name created by to_folder_name().
        Returns JobName on success, otherwise None.
        """
        if not name.startswith(JOB_FOLDER_PREFIX):
            logging.debug(f"parse_folder_name: '{name}' does not start with prefix '{JOB_FOLDER_PREFIX}'")
            return None
        suffix = name[len(JOB_FOLDER_PREFIX):]
        parts = suffix.split("-")
        if len(parts) != 4:
            logging.debug(f"parse_folder_name: '{name}' has wrong parts count: {len(parts)} (want 4)")
            return None
        try:
            sub_id, job_number, n_jobs, analysis_id = (int(p) for p in parts)
            return cls(sub_id=sub_id, job_number=job_number, n_jobs=n_jobs, analysis_id=analysis_id)
        except ValueError:
            logging.debug(f"parse_folder_name: non-integer component in '{name}'")
            return None


def _list_subdirs(path: str) -> List[str]:
    try:
        return [d.name for d in os.scandir(path) if d.is_dir()]
    except FileNotFoundError:
        return []


def get_list_of_finished_jobs(analysis: Analysis,
                 finished_only: bool = True,
                 exclude_errors: bool = True) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Return a list of job dicts for a sub-analysis and the total expected jobs (n_jobs) if parseable.

    - Jobs are discovered as subdirectories of `analysis.sub_output_dir`.
    - If `finished_only` is True, include only job dirs with a `finished` marker.
    - If `exclude_errors` is True, skip job dirs with an `error` marker.

    Each job dict has the shape: {"metadata": {"name", "sub_id", "analysis_id"}}.
    The returned `n_jobs` is derived from the folder name, or None if not available.
    """
    logging.debug(f"Inside get_list_of_finished_jobs for sub_id: {analysis.sub_id}")
    job_dirs = _list_subdirs(analysis.sub_output_dir)
    if not job_dirs:
        return [], None

    total_jobs: Optional[int] = None
    analysis_id = analysis.id
    # Determine n_jobs and prefer analysis_id from folder name if present
    for jd in job_dirs:
        jn = JobName.parse_folder_name(jd)
        if jn and jn.sub_id == analysis.sub_id:
            total_jobs = jn.n_jobs
            analysis_id = jn.analysis_id
            break

    job_list: List[Dict[str, Any]] = []
    for jd in job_dirs:
        job_path = os.path.join(analysis.sub_output_dir, jd)
        if exclude_errors and os.path.exists(os.path.join(job_path, "error")):
            logging.debug(f"has error, continue")
            continue
        if finished_only and not os.path.exists(os.path.join(job_path, "finished")):
            logging.debug(f"not finished, continue")
            continue
        job_list.append({"metadata": {"name": jd, "sub_id": analysis.sub_id, "analysis_id": analysis_id}})

    logging.debug(f"total_jobs{total_jobs}, len(job_list) {len(job_list)}")

    return job_list, total_jobs
