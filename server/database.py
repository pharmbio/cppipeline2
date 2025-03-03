from __future__ import annotations
import logging
import json
from typing import List, Optional, Dict, Any
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import threading

# -----------------------------------------------------------------------------
# Helper Function for Z Filter Parsing
# -----------------------------------------------------------------------------
def parse_string_of_num_and_ranges(input_str: str) -> List[int]:
    """
    Take a string like '2-5,7,15-17,12' and turn it into a sorted list:
    [2, 3, 4, 5, 7, 12, 15, 16, 17]
    """
    if input_str.startswith("-"):
        try:
            return [int(input_str)]
        except Exception as e:
            logging.error(f"Error parsing z filter input '{input_str}': {e}")
            return []
    numbers = set()
    for element in input_str.split(','):
        try:
            parts = [int(x) for x in element.split('-')]
        except Exception as e:
            logging.error(f"Error parsing element '{element}': {e}")
            continue
        if len(parts) == 1:
            numbers.add(parts[0])
        else:
            for part in range(min(parts), max(parts) + 1):
                numbers.add(part)
    return sorted(list(numbers))

# -----------------------------------------------------------------------------
# Image Wrapper Class
# -----------------------------------------------------------------------------
class Image:
    def __init__(self, data: Dict[str, Any]) -> None:
        """Initialize the Image wrapper with a dictionary of image data."""
        self._data = data

    @property
    def image_id(self) -> Any:
        return self._data.get('image_id')

    @property
    def plate_acquisition_id(self) -> Any:
        return self._data.get('plate_acquisition_id')

    @property
    def z(self) -> Any:
        return self._data.get('z')

    @property
    def site(self) -> Any:
        return self._data.get('site')

    @property
    def well(self) -> Any:
        return self._data.get('well')

    @property
    def dye(self) -> Any:
        return self._data.get('dye')

    @property
    def timepoint(self) -> Any:
        return self._data.get('timepoint')

    @property
    def path(self) -> Any:
        return self._data.get('path')

    def get_display_url(self) -> str:
        """Return a URL for displaying the image (example implementation)."""
        return f"https://yourdomain.com/images/{self.image_id}"

    def get_data(self) -> Dict[str, Any]:
        """Return the underlying dictionary data."""
        return self._data

    def __repr__(self) -> str:
        return f"<Image id={self.image_id}>"

# -----------------------------------------------------------------------------
# ImageSet Class
# -----------------------------------------------------------------------------
class ImageSet:
    def __init__(self, imgset_id: str) -> None:
        """Initialize the ImageSet with a unique identifier and an empty list of images."""
        self.imgset_id = imgset_id
        self.images: List[Image] = []

    def add_image(self, image: Image) -> None:
        """Add an Image to this ImageSet."""
        self.images.append(image)

    def __iter__(self):
        """Iteration over the images in this ImageSet."""
        return iter(self.images)

    @property
    def all_images(self) -> List[Image]:
        """Return the list of Image objects in this ImageSet."""
        return self.images

    def __repr__(self) -> str:
        return f"<ImageSet id={self.imgset_id}, count={len(self.images)}>"

# -----------------------------------------------------------------------------
# Database Class
# -----------------------------------------------------------------------------
class Database:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:  # Ensure thread-safe singleton creation.
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.connection_pool = None  # Initially, no connection pool.
        return cls._instance

    def initialize_connection_pool(self, **connection_info):
        if not hasattr(self, 'initialized'):  # Prevent reinitialization.
            if connection_info:
                self.connection_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, **connection_info)
                self.initialized = True
            else:
                raise ValueError("Connection information must be provided to initialize the connection pool.")

    @classmethod
    def get_instance(cls) -> Database:
        """Return the singleton Database instance."""
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

    # -------------------------------------------------------------------------
    # Dependency Checking Methods
    # -------------------------------------------------------------------------
    def check_analyses_finished(self, analyses: List[Dict[str, Any]]) -> bool:
        logging.info("Inside check_analyses_finished")
        for analysis in analyses:
            if not analysis.get('finish'):
                return False
        return True

    def all_dependencies_satisfied(self, analysis: Dict[str, Any]) -> bool:
        if analysis.get('depends_on_sub_id'):
            deps = ",".join(map(str, analysis['depends_on_sub_id']))
            logging.debug('Fetching analysis dependencies.')
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f'''
                        SELECT *
                        FROM image_sub_analyses
                        WHERE sub_id IN ({deps})
                    ''')
                    dep_analyses = cursor.fetchall()
                return self.check_analyses_finished(dep_analyses)
            except Exception as e:
                logging.error(f"Error checking dependencies: {e}")
                return False
            finally:
                if conn:
                    self.release_connection(conn)
        else:
            return True

    # -------------------------------------------------------------------------
    # New Method: Get the First (Minimum) Z Plane.
    # -------------------------------------------------------------------------
    def get_middle_z_plane(self, acq_id) -> Optional[Any]:
        logging.info(f'Fetching minimum z-plane for plate acquisition ID: {acq_id}')
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get a sorted list of unique z values
                query = """
                        SELECT DISTINCT z
                        FROM images
                        WHERE plate_acquisition_id = %s
                        ORDER BY z ASC
                        """
                logging.info('Executing query: %s with ID: %s', query.strip(), acq_id)
                cursor.execute(query, (acq_id,))
                results = cursor.fetchall()
                
                # Create a list of unique z values
                z_values = [row['z'] for row in results]
                
                if z_values:
                    # Determine the middle index (using lower middle for even counts)
                    middle_index = (len(z_values) - 1) // 2
                    logging.info(f"Total unique z values: {len(z_values)}. Middle index: {middle_index}")
                    return z_values[middle_index]
                else:
                    logging.warning('No distinct z values found for plate acquisition ID: %s', acq_id)
                    return None
                
        except Exception as e:
            logging.error(f"Error fetching first z plane: {e}")
            return None
        finally:
            if conn:
                self.release_connection(conn)

    # -------------------------------------------------------------------------
    # New Method: Get Images for an Analysis.
    # -------------------------------------------------------------------------
    def get_images(self, plate_acquisition_id: Any, z_filter: List[Any],
                   site_filter: Optional[List[Any]] = None,
                   well_filter: Optional[List[Any]] = None,
                   channels_filter: Optional[List[Any]] = None) -> List[Image]:
        logging.info("Fetching images belonging to plate acquisition.")
        query = (
            "SELECT DISTINCT plate_acquisition_id, plate_barcode, timepoint, well, site, z, channel, dye, path "
            "FROM images_all_view "
            "WHERE plate_acquisition_id = %s"
        )
        params: List[Any] = [plate_acquisition_id]

        # Build the z filter clause (numeric values).
        if z_filter:
            query += " AND z IN (" + ",".join(map(str, z_filter)) + ")"

        # Build the site filter clause.
        if site_filter:
            query += " AND site IN (" + ",".join(map(str, site_filter)) + ")"
        # For well_filter and channels_filter, wrap the values in quotes.
        if well_filter:
            query += " AND well IN (" + ",".join("'" + str(well) + "'" for well in well_filter) + ")"
        if channels_filter:
            query += " AND dye IN (" + ",".join("'" + str(chan) + "'" for chan in channels_filter) + ")"

        query += " ORDER BY timepoint, well, site, channel"
        logging.info("query: " + query)
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall()
                images = [Image(row) for row in rows]
            return images
        except Exception as e:
            logging.error(f"Error fetching images: {e}")
            return []
        finally:
            if conn:
                self.release_connection(conn)

    def get_channelmap(self, plate_acquisition_id: Any) -> List[Dict[str, Any]]:
        """
        Fetch channel map rows for the given plate acquisition.
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = (
                    "SELECT * "
                    "FROM channel_map "
                    "WHERE map_id = ("
                    "   SELECT channel_map_id FROM plate_acquisition WHERE id = %s"
                    ")"
                )
                cursor.execute(query, (plate_acquisition_id,))
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Error fetching channel map from DB: {e}")
            return []
        finally:
            self.release_connection(conn)

    def get_new_analyses(self) -> List[Analysis]:
        logging.info('Fetching new analyses')
        query = (
            "SELECT * "
            "FROM image_sub_analyses_v1 "
            "WHERE start IS NULL "
            "AND error IS NULL "
            "ORDER BY priority, sub_id"
        )
        analyses: List[Analysis] = []
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logging.debug(query)
                cursor.execute(query)
                results = cursor.fetchall()
                analyses = [Analysis(data) for data in results]
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        finally:
            if conn:
                self.release_connection(conn)
        return analyses

    def get_analysis(self, sub_id: int) -> Optional[Analysis]:
        logging.info('Fetching analysis for sub_id')
        query = (
            "SELECT * "
            "FROM image_sub_analyses_v1 "
            "WHERE sub_id = %s"
        )
        analysis_obj: Optional[Analysis] = None
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logging.debug(query)
                cursor.execute(query, [sub_id])
                result = cursor.fetchone()  # Get a single row.
                if result is not None:
                    analysis_obj = Analysis(result)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        finally:
            if conn:
                self.release_connection(conn)
        return analysis_obj

    def set_sub_analysis_error(self, analysis: Analysis, errormessage: str):
        logging.error(f"TODO implement set sub analysis error, errormessage: {errormessage}")

    def update_progress_meta(self, analysis_id, sub_analysis_id, data_key, data_value):
        sub_analysis_data = json.dumps({data_key: data_value})
        analysis_data = json.dumps({f"{data_key}_{sub_analysis_id}": data_value})
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                query_sub = (
                    "UPDATE image_sub_analyses "
                    "SET progress = progress || %s "
                    "WHERE sub_id = %s"
                )
                cursor.execute(query_sub, [sub_analysis_data, sub_analysis_id])
                query_analysis = (
                    "UPDATE image_sub_analyses "
                    "SET progress = progress || %s "
                    "WHERE id = %s"
                )
                cursor.execute(query_analysis, [analysis_data, analysis_id])
                conn.commit()
        except Exception as e:
            logging.error(f"Error updating progress: {e}")
        finally:
            if conn:
                self.release_connection(conn)

# -----------------------------------------------------------------------------
# Analysis Class
# -----------------------------------------------------------------------------
class Analysis:
    def __init__(self, data: Dict[str, Any]) -> None:
        """Initialize the Analysis with a dictionary of analysis data."""
        self._data = data
        self._cached_channelmap: Optional[List[Dict[str, Any]]] = None

    @property
    def finished(self) -> bool:
        return self._data.get('finish') is not None

    @property
    def results_dir(self) -> str:
        return self.storage_paths['full']

    @property
    def sub_input_dir(self) -> str:
        return f"/cpp_work/input/{self.sub_id}"

    @property
    def sub_output_dir(self) -> str:
        return f"/cpp_work/output/{self.sub_id}"

    @property
    def is_cellprofiler_analysis(self) -> bool:
        meta_info = self._data.get('meta', {})
        return meta_info.get('type') == 'cellprofiler'

    @property
    def sub_type(self) -> bool:
        return self._data.get('meta', {}).get('sub_type', "undefined_sub_type")

    @property
    def is_run_on_dardel(self) -> bool:
        return self._data.get('meta', {}).get('run_on_dardel', False)

    @property
    def is_run_on_hpcdev(self) -> bool:
        return self._data.get('meta', {}).get('run_on_hpcdev', False)

    @property
    def id(self) -> Any:
        return self._data.get('analysis_id')

    @property
    def sub_id(self) -> Any:
        return self._data.get('sub_id')

    @property
    def meta(self) -> Dict[str, Any]:
        return self._data.get('meta', {})

    @property
    def analysis_path(self) -> str:
        analysis_path = f"/cpp_work/output/{self._data.get('sub_id')}/"
        logging.debug(f"analysis_path {analysis_path}")
        return analysis_path

    @property
    def raw_data(self) -> Dict[str, Any]:
        return self._data

    @property
    def plate_acquisition_id(self) -> Any:
        return self._data.get('plate_acquisition_id')

    @property
    def plate_barcode(self) -> Optional[str]:
        return self._data.get("plate_barcode")

    @property
    def use_icf(self) -> Optional[bool]:
        return self._data.get("use_icf", False)

    @property
    def priority(self) -> Optional[int]:
        value = self._data.get("priority", 0)
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    @property
    def job_timeout(self) -> Optional[str]:
        return self._data.get("job_timeout", "10800")

    def update_progress_meta(self, data_key, data_value):
        analysis_id = self._data.get('analysis_id')
        sub_id = self._data.get('sub_id')
        if analysis_id is not None and sub_id is not None:
            db = Database.get_instance()
            db.update_progress_meta(analysis_id, sub_id, data_key, data_value)
        else:
            logging.error("Missing analysis_id or sub_id for metadata update.")

    def all_dependencies_satisfied(self) -> bool:
        db = Database.get_instance()
        return db.all_dependencies_satisfied(self._data)

    # -------------------------------------------------------------------------
    # Filter Helper Methods
    # -------------------------------------------------------------------------
    def site_filter(self) -> Optional[List]:
        analysis_meta = self.meta
        if 'site_filter' in analysis_meta:
            return list(analysis_meta['site_filter'])
        return None

    def well_filter(self) -> Optional[List]:
        analysis_meta = self.meta
        if 'well_filter' in analysis_meta:
            return list(analysis_meta['well_filter'])
        return None

    def channels_filter(self) -> Optional[List]:
        analysis_meta = self.meta
        if 'channels' in analysis_meta:
            return list(analysis_meta['channels'])
        return None

    def z_filter(self) -> List[Any]:
        """
        Return a list of z values from the metadata if available;
        otherwise, fetch the first z plane from the database and return it in a list.
        The metadata z filter is expected to be a string like '2-5,7,15-17,12'.
        """
        analysis_meta = self.meta
        if 'z' in analysis_meta:
            return parse_string_of_num_and_ranges(analysis_meta['z'])
        else:
            if not self.plate_acquisition_id:
                logging.error("Missing plate_acquisition_id in analysis data")
                return []
            db = Database.get_instance()
            z_val = db.get_middle_z_plane(self.plate_acquisition_id)
            return [z_val] if z_val is not None else []

    def get_images(self) -> List[Image]:
        db = Database.get_instance()
        z_filter_values = self.z_filter()
        site_filter = self.site_filter()
        well_filter = self.well_filter()
        channels_filter = self.channels_filter()
        return db.get_images(self.plate_acquisition_id, z_filter_values, site_filter, well_filter, channels_filter)

    @property
    def batch_size(self) -> int:
        """
        Return the batch size from metadata.
        If not provided, defaults to -1 (i.e. all image sets in one batch).
        """
        try:
            return int(self.meta.get('batch_size', -1))
        except Exception as e:
            logging.error(f"Error converting batch_size to int: {e}")
            return -1

    def get_all_imgsets(self) -> Dict[str, ImageSet]:
        """
        Group images into ImageSet objects based on a unique identifier (timepoint, well, site, z).
        Returns a dictionary mapping the unique identifier to the corresponding ImageSet.
        """
        logging.info(f"Inside get_all_imgsets, analysis: {self.raw_data}")
        imgs = self.get_images()
        img_sets: Dict[str, ImageSet] = {}
        for img in imgs:
            imgset_id = f"tp{img.timepoint}-{img.well}-{img.site}-{img.z}"
            if imgset_id not in img_sets:
                img_sets[imgset_id] = ImageSet(imgset_id)
            img_sets[imgset_id].add_image(img)
        for set_id, img_set in img_sets.items():
            logging.info(f"ImageSet {set_id} has {len(img_set.all_images)} images")
        return img_sets

    def get_imgset_batches(self, batch_size: Optional[int] = None) -> List[List[ImageSet]]:
        """
        Group image sets into batches.
        If batch_size is -1, all image sets are returned in a single batch.
        If batch_size is not provided, use the value from meta['batch_size'].
        """
        if batch_size is None:
            batch_size = self.batch_size
        imgset_dict = self.get_all_imgsets()
        imgset_list = list(imgset_dict.values())
        if batch_size == -1:
            logging.info("Batch size is -1; returning a single batch with all image sets.")
            return [imgset_list]
        # More verbose batching:
        batches = []
        for i in range(0, len(imgset_list), batch_size):
            batch = imgset_list[i:i + batch_size]
            batches.append(batch)
        logging.info(f"Grouped image sets into {len(batches)} batches (batch size: {batch_size}).")
        return batches

    def get_channelmap(self) -> Dict[int, str]:
        if self._cached_channelmap is not None:
            return self._cached_channelmap

        db = Database.get_instance()
        rows = db.get_channelmap(self.plate_acquisition_id)  # list of dict
        result_dict = {}
        for row in rows:
            channel_id = row["channel"]
            dye = row["dye"]
            result_dict[channel_id] = dye

        # Optional: filter if channels_filter is in use
        cf = self.channels_filter()
        if cf:
            result_dict = {ch_id: val for ch_id, val in result_dict.items() if val in cf}

        self._cached_channelmap = result_dict
        return result_dict


    @property
    def storage_paths(self) -> Dict[str, Any]:
        """
        Return the storage paths for the analysis.
        Builds a dictionary using the analysis's 'plate_barcode', 'plate_acquisition_id', and analysis id.
        """
        plate_barcode = self.plate_barcode
        acquisition_id = self.plate_acquisition_id
        analysis_id = self.id
        return {
            "full": f"/cpp_work/results/{plate_barcode}/{acquisition_id}/{analysis_id}",
            "mount_point": "/cpp_work/",
            "job_specific": f"results/{plate_barcode}/{acquisition_id}/{analysis_id}/"
        }

    @property
    def cellprofiler_version(self) -> Optional[str]:
        """Return the CellProfiler version from the metadata."""
        return self.meta.get('cp_version')

    @property
    def pipeline_file(self) -> Optional[str]:
        """
        Return the full path to the pipeline file.
        If a pipeline file is specified in the metadata, prepend '/cpp_work/pipelines/' to it.
        """
        pipeline = self.meta.get('pipeline_file')
        if pipeline:
            return '/cpp_work/pipelines/' + pipeline
        return None

# -----------------------------------------------------------------------------
# Example Usage
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Configure logging.
    logging.basicConfig(level=logging.DEBUG)

    # Example connection info (update with your real credentials)
    conn_info = {
        "dbname": "yourdbname",
        "user": "youruser",
        "password": "yourpassword",
        "host": "yourhost",
        "port": 5432,
    }

    # Initialize the database connection pool.
    db = Database.get_instance()
    db.initialize_connection_pool(**conn_info)

    # Create an example Analysis object with dependency and filter info.
    example_data = {
        "analysis_id": 123,
        "sub_id": 456,
        "finish": None,  # None means not finished; could be a timestamp when finished.
        "depends_on_sub_id": [111, 222],  # List of dependency sub_ids.
        "plate_acquisition_id": 789,      # Example plate acquisition ID.
        "meta": {
            "type": "cellprofiler",
            "run_on_dardel": True,
            "run_on_hpcdev": False,
            "site_filter": ["A1", "B2"],
            "well_filter": ["W1", "W2"],
            "channels": ["red", "green"],
            # Optionally supply a z filter string, e.g.:
            # "z": "2-5,7,15-17,12",
            "cp_version": "2.2.0",
            "pipeline_file": "example_pipeline.cppipe"
        }
    }
    analysis = Analysis(example_data)

    # Group images into image sets.
    img_sets = analysis.get_all_imgsets()
    logging.info(f"Total ImageSets formed: {len(img_sets)}")
    for set_id, img_set in img_sets.items():
        logging.info(f"ImageSet {set_id}:")
        for image in img_set.all_images:
            logging.info(f"  Image {image.image_id} URL: {image.get_display_url()}")

    # Fetch and log the channel map.
    channel_map = analysis.get_channelmap()
    logging.info(f"Channel map: {channel_map}")

    # Access the new properties.
    logging.info(f"CellProfiler version: {analysis.cellprofiler_version}")
    logging.info(f"Pipeline file: {analysis.pipeline_file}")

    # Access storage paths from the analysis.
    storage_paths = analysis.storage_paths
    logging.info(f"Storage paths from analysis: {storage_paths}")
