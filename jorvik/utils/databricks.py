'''This module provides utility functions to interact with Databricks built in utilities (default spark context, dbutils).'''
import json
from typing import Any, List, Callable

from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException


class DatabricksUtilsError(Exception):
    """Custom exception for Databricks utility errors."""

    def __init__(self, error: str = None):
        message = f"{error}. Ensure you are running this code in a Databricks notebook environment."  # noqa: E501
        super().__init__(message)

def get_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise DatabricksUtilsError("No active Spark session found")
    return spark

def get_dbutils() -> Any:
    """ Gets the Databricks dbutils client
    Returns:
        DBUtils: dbutils client
    Raises:
        DatabricksUtilsError: If the dbutils client configuration cannot be determined.
    """
    spark = get_spark()
    client_config = spark.conf.get("spark.databricks.service.client.enabled", None)

    try:
        if client_config == "true":
            from pyspark.dbutils import DBUtils  # type:ignore
            return DBUtils.SparkServiceClientDBUtils(spark.sparkContext)
        else:
            import IPython  # type: ignore
            return IPython.get_ipython().user_ns["dbutils"]  # type:ignore
    except Exception:
        raise DatabricksUtilsError("Could not determine the dbutils client configuration")

def get_notebook_context() -> dict:
    """ Gets the current notebook context

    Returns:
        dict: notebook context
    """

    return json.loads(
        get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().toJson()  # type:ignore
    )

def get_active_branch() -> str:
    """ Returns the current active Git branch, if applicable.
        For production workflows running from a Workspace (non-Git) folder, an empty string
        is returned instead of a branch name. This indicates that no isolation should be applied.

    Returns:
        str: active_branch or ''
    """

    context = get_notebook_context()

    if "mlflowGitReference" not in context["extraContext"]:
        return ""

    branch = context["extraContext"]["mlflowGitReference"]
    return branch

def get_current_user() -> str:
    """ Gets the current Databricks user"""
    return get_notebook_context()['tags']['user']

def get_cluster_id() -> str:
    """ Gets the current Databricks cluster ID"""
    return get_notebook_context()['tags']['clusterId']

def get_notebook_path() -> str:
    """ Gets the current notebook path"""
    return get_notebook_context()["extraContext"]["notebook_path"]

def sanitize_dbfs_path(path: str) -> str:
    """ Sanitizes a DBFS path by removing the 'dbfs:' and/ or 'mnt' prefix if present."""
    if path.startswith("dbfs:"):
        path = path[4:]
    if path.startswith("mnt"):
        path = path[3:]
    return path

def scan_delta_tables(dir_path: str, max_level: int = 5,
                      level: int = 0, exclude_paths: List[str] = [],
                      path_action: Callable = None) -> List[str]:
    """ Recursively scans a directory and detect delta tables in subdirectories.
    Args:
        dir_path (str): The directory path to scan.
        max_level (int): Maximum depth of subdirectory scanning. Default is 5.
        level (int): Current recursion depth.
        exclude_paths (List[str]): List of paths to exclude from scanning.
        path_action (Callable): Optional function to apply to each detected delta table path.
                                For example register table to Hive Metastore, Optimize, Vacuum, etc.
    Returns:
        List[str]: List of detected delta table paths.
    """
    spark = get_spark()
    dbutils = get_dbutils()

    # If no delta table is detected after max_level, abandon the path
    if level > max_level:
        return []

    # Stop scanning if the directory is in the exclude list
    if sanitize_dbfs_path(dir_path) in [sanitize_dbfs_path(p) for p in exclude_paths]:
        return []

    # Check if current path is a delta table
    try:
        df = spark.read.format("delta").load(dir_path)
        if df is not None:
            if path_action:
                path_action(dir_path)
            return [dir_path]
    except AnalysisException:
        pass  # Not a delta table, continue scanning
    except Exception:  # Detected as delta table but failed to read, possibly due to corruption.
        return []

    delta_paths = []
    subdirs = [f.path for f in dbutils.fs.ls(dir_path) if f.isDir()]

    for p in subdirs:
        delta_paths.extend(scan_delta_tables(p, max_level, level + 1, exclude_paths))

    return delta_paths
