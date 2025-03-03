# sync_utils.py
import logging
import os
import subprocess

class SyncManager:
    def __init__(self, remote_user: str, remote_host: str, remote_base_path: str) -> None:
        """
        Initialize with remote user, host, and base path.

        Args:
            remote_user: The username for the remote system.
            remote_host: The remote system host (e.g. 'guestserver').
            remote_base_path: The base directory on the remote system (e.g. '/share/').
        """
        self.remote_user = remote_user
        self.remote_host = remote_host
        self.remote_base_path = remote_base_path.rstrip('/')
        self.local_base_path = '/cpp_work'

    def sync_output_dir_to_remote(self, local_path: str) -> None:
        """
        Sync the local output directory to the remote base path.
        """
        from_local_path = local_path.rstrip('/')
        # Build remote path as "user@host:/base/path"
        to_remote_path = f"{self.remote_user}@{self.remote_host}:{self.remote_base_path}"
        logging.debug(f"sync_output_dir_to_remote: from {from_local_path} to {to_remote_path}")
        self._sync_with_rsync_relative(from_local_path, to_remote_path)

    def sync_input_dir(self, sub_id: str) -> None:
        """
        Sync files from the remote input directory to local.
        """
        from_local_path = f"{self.local_base_path}/input/{sub_id}"
        to_remote_path = f"{self.remote_user}@{self.remote_host}:{self.remote_base_path}/input/"
        logging.debug(f"sync_input_dir: from {from_local_path} to {to_remote_path}")
        self._sync_with_rsync(from_local_path, to_remote_path)

    def sync_pipelines_dir(self, results_dir: str) -> None:
        """
        Sync pipelines directory from remote to local.
        """
        from_remote_path = f"{self.remote_user}@{self.remote_host}:{self.remote_base_path}{results_dir}/*"
        to_local_path = results_dir
        logging.debug(f"sync_pipelines_dir: from {from_remote_path} to {to_local_path}")
        self._sync_with_rsync(from_remote_path, to_local_path)

    def _sync_with_rsync(self, from_path: str, to_path: str) -> None:
        """
        Execute a basic rsync command.
        """
        rsync_command = ["rsync", "-av", from_path, to_path]
        logging.debug(f"Executing rsync: {rsync_command}")
        os.makedirs(to_path, exist_ok=True)
        result = subprocess.run(rsync_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        logging.debug(result.stdout.decode())

    def _sync_with_rsync_relative(self, from_path: str, to_path: str) -> None:
        """
        Execute an rsync command with relative paths.
        """
        rsync_command = ["rsync", "-av", "--relative", "--no-perms", "--omit-dir-times", from_path, to_path]
        logging.debug(f"Executing rsync (relative): {rsync_command}")
        result = subprocess.run(rsync_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        logging.debug(result.stdout.decode())
