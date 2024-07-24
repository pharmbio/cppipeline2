from __future__ import annotations
import logging
import json
from typing import List, Optional, Dict, Any
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import threading


class Analysis:
    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data

    @property
    def finished(self) -> bool:
        """Check if the analysis is finished."""
        return self._data.get('finish') is not None

    @property
    def input_dir(self) -> str:
        """Get the input directory for the analysis."""
        return f"/cpp_work/input/{self._data['analysis_id']}"

    @property
    def is_cellprofiler_analysis(self) -> bool:
        """Determine if the analysis type is 'cellprofiler'."""
        meta_info = self._data.get('meta', {})  # Get the 'meta' sub-dictionary safely
        analysis_type = meta_info.get('type')  # Get the 'type' from 'meta' safely
        return analysis_type == 'cellprofiler'  # Check if the type is 'cellprofiler'

    @property
    def is_run_on_dardel(self) -> bool:
        """Determine if the analysis is set to run on Dardel."""
        return self._data.get('meta', {}).get('run_on_dardel', False)

    @property
    def analysis_id(self) -> Any:
        """Get the analysis ID."""
        return self._data.get('analysis_id')

    @property
    def analysis_sub_id(self) -> Any:
        """Get the analysis sub ID."""
        return self._data.get('sub_id')

    @property
    def analysis_path(self) -> str:
        analysis_path = f"/cpp_work/output/{self._data.get('sub_id')}/"
        logging.debug(f"analysis_path {analysis_path}")
        return analysis_path

    @property
    def raw_data(self) -> Dict[str, Any]:
        """Get the raw analysis data."""
        return self._data

    def update_progress_meta(self, db: Database, data_key, data_value):
        # Assuming self._data contains 'analysis_id' and 'sub_id'
        analysis_id = self._data.get('analysis_id')
        sub_id = self._data.get('sub_id')

        if analysis_id is not None and sub_id is not None:
            db.update_progress_meta(analysis_id, sub_id, data_key, data_value)
        else:
            logging.error("Missing analysis_id or sub_id for metadata update.")

class Database:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:  # Ensure thread-safe singleton creation
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.connection_pool = None  # Ensure the connection_pool is initially set to None
        return cls._instance

    def initialize_connection_pool(self, **connection_info):
        if not hasattr(self, 'initialized'):  # Prevent reinitialization
            if connection_info:
                # Initialize connection pool if it doesn't exist and connection_info is provided
                self.connection_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, **connection_info)
                self.initialized = True  # Mark as initialized
            else:
                raise ValueError("Connection information must be provided to initialize the connection pool.")

    @classmethod
    def get_instance(cls):
        # Return the singleton instance
        return cls()

    def get_connection(self):
        if self.connection_pool is None:
            raise Exception("Connection pool has not been initialized.")
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        if self.connection_pool is not None:
            self.connection_pool.putconn(conn)
        else:
            raise Exception("Connection pool has not been initialized.")

    def get_new_analyses(self) -> List[Analysis]:  # Ensure that Analysis is properly imported
        logging.info('Fetching new analyses')
        query = '''
                SELECT *
                FROM image_sub_analyses
                WHERE start IS NULL
                ORDER BY priority, sub_id
                '''
        analyses = []
        conn = None
        try:
            conn = self.get_connection()  # Manually get a connection
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logging.debug(query)
                cursor.execute(query)
                results = cursor.fetchall()
                analyses = [Analysis(data) for data in results]
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        finally:
            if conn:  # Make sure to return the connection back to the pool
                self.release_connection(conn)
        return analyses

    def get_analysis_from_sub_id(self, sub_id) -> Optional[Analysis]:
        logging.info('Fetching analysis for sub_id')
        query = '''
                SELECT *
                FROM image_sub_analyses
                WHERE sub_id = %s
                '''
        analysis = None  # Initialize analysis as None
        conn = None
        try:
            conn = self.get_connection()  # Manually get a connection
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, [sub_id])
                result = cursor.fetchone()
                if result is not None:
                    analysis = Analysis(result)  # Assume Analysis can be initialized directly from the dict
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        finally:
            if conn:  # Make sure to return the connection back to the pool
                self.release_connection(conn)
        return analysis

    def set_sub_analysis_error(self, annalysis: Analysis, errormessage: str):
        logging.error(f"TODO implement set sub analysis error, errormessage: {errormessage}")

    def update_progress_meta(self, analysis_id, sub_analysis_id, data_key, data_value):
        # Create JSON strings for update
        sub_analysis_data = json.dumps({data_key: data_value})
        analysis_data = json.dumps({f"{data_key}_{sub_analysis_id}": data_value})

        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Update image_sub_analyses
                query_sub = """UPDATE image_sub_analyses
                               SET progress = progress || %s
                               WHERE sub_id = %s"""
                cursor.execute(query_sub, [sub_analysis_data, sub_analysis_id])

                # Update image_analyses
                query_analysis = """UPDATE image_analyses
                                    SET progress = progress || %s
                                    WHERE id = %s"""
                cursor.execute(query_analysis, [analysis_data, analysis_id])

                conn.commit()  # Commit transactions
        except Exception as e:
            logging.error(f"Error updating progress: {e}")
        finally:
            if conn:
                self.release_connection(conn)