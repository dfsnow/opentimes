import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import boto3
import yaml
from botocore.exceptions import ClientError
from jinja2 import Environment, FileSystemLoader
from utils.cloudflare import get_r2_objects, purge_cloudflare_cache
from utils.constants import DATASET_DICT
from utils.duckdb import create_duckdb_file
from utils.logging import create_logger
from utils.utils import format_size

logger = create_logger(__name__)

with open("params.yaml") as file:
    params = yaml.safe_load(file)
session = boto3.Session(profile_name=params["s3"]["profile"])
s3 = session.client("s3", endpoint_url=params["s3"]["endpoint"])

# Initialize Jinja2 environment and template
jinja_env = Environment(loader=FileSystemLoader("site/templates"))
jinja_index_template = jinja_env.get_template("index.html")

# Load Cloudflare API credentials
CLOUDFLARE_CACHE_ZONE_ID = os.environ.get("CLOUDFLARE_CACHE_ZONE_ID")
CLOUDFLARE_CACHE_API_TOKEN = os.environ.get("CLOUDFLARE_CACHE_API_TOKEN")


def append_duckdb_info(tree: dict, version: str, db_path: Path) -> None:
    """
    Append or overwrite DuckDB file location and information in the file tree.
    Note that this modifies the tree in-place.

    Args:
        tree: Nested dictionary that represents the S3 bucket structure.
        version: The version of the dataset file, as SemVer.
        db_path: Local path to the DuckDB database file created at run time.
    """
    duckdb_info = {
        "filename": f"{version}.duckdb",
        "size": format_size(db_path.stat().st_size),
        "last_modified": datetime.fromtimestamp(db_path.stat().st_mtime)
        .astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat(timespec="seconds"),
    }
    if "databases" not in tree:
        tree["databases"] = {}
    tree["databases"][f"{version}.duckdb"] = duckdb_info

    # Recalculate the total_size of the databases/ directory
    total_size = sum(
        db_path.stat().st_size
        for db_info in tree["databases"].values()
        if isinstance(db_info, dict) and "size" in db_info
    )
    tree["databases"]["total_size"] = format_size(total_size)

    # Update the max_last_modified of the databases/ directory
    if "max_last_modified" not in tree["databases"]:
        tree["databases"]["max_last_modified"] = duckdb_info["last_modified"]
    if duckdb_info["last_modified"] > tree["databases"]["max_last_modified"]:
        tree["databases"]["max_last_modified"] = duckdb_info["last_modified"]


def generate_html_files(
    tree: dict,
    bucket_name: str,
    folder_path: str | Path = "",
) -> None:
    """
    Generate static index.html files representing the directory structure
    of each folder (ala nginx). Each index file contains a table with three
    columns:
        - "Item": A link to the file or directory.
        - "Last Modified": The max last modified time of the file, or of
          contained files in the case of a directory
        - "Size": The file size, or total size of all files in a directory

    Args:
        tree: Dictionary of the bucket structure, as returned by get_r2_objects.
        bucket_name: Name of the R2 bucket.
        folder_path: Path within the R2 bucket to generate files for.
    """
    index_file_path = Path(folder_path) / "index.html"
    html_content = jinja_index_template.render(
        folder_name=folder_path, contents=tree
    )

    retries = 3
    for attempt in range(retries):
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=str(index_file_path.as_posix()),
                Body=html_content,
                ContentType="text/html",
            )
            break
        except ClientError as e:
            if attempt < retries - 1:
                logger.info(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(2**attempt)
            else:
                logger.error(f"Failed after {retries} attempts")
                raise e

    # Recursively create subfolders and their index.html
    for item, subtree in tree.items():
        if isinstance(subtree, dict) and "filename" not in subtree:
            generate_html_files(subtree, bucket_name, Path(folder_path) / item)


def main() -> None:
    logger.info("Retrieving objects from R2 bucket")
    tree, keys = get_r2_objects(s3, params["s3"]["public_bucket"])
    logger.info(f"Retrieved {len(keys)} objects from R2 bucket")

    versions = list(DATASET_DICT.keys())
    for version in versions:
        logger.info(f"Creating DuckDB file for version {version}")
        db_path = Path.cwd() / "site" / f"{version}.duckdb"
        create_duckdb_file(
            tree=tree,
            datasets=list(DATASET_DICT[version].keys()),
            version=version,
            modes=params["times"]["mode"],
            years=params["input"]["year"],
            geographies=params["input"]["census"]["geography"]["all"],
            bucket_name=params["s3"]["public_bucket"],
            base_url=params["s3"]["public_data_url"],
            path=db_path.as_posix(),
        )

        s3.upload_file(
            Filename=db_path.as_posix(),
            Bucket=params["s3"]["public_bucket"],
            Key=f"databases/{version}.duckdb",
        )
        # Update the tree in-place with the new file
        append_duckdb_info(tree, version, db_path)
        logger.info(f"DuckDB file created at {db_path}")

    # Recursively create subfolders and their index.html. Using parallelism
    # here because otherwise this takes forever
    logger.info("Generating HTML index files")
    with ThreadPoolExecutor() as executor:
        futures_dict = {
            executor.submit(
                generate_html_files,
                subtree,
                params["s3"]["public_bucket"],
                Path(item),
            ): item
            for item, subtree in tree.items()
            if isinstance(subtree, dict) and "filename" not in subtree
        }
        for future in as_completed(futures_dict):
            directory = futures_dict[future]
            try:
                future.result()
                logger.info(
                    f"Successfully generated HTML files for directory: {directory}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to generate HTML files for directory {directory}: {str(e)}"
                )
                raise e

    logger.info("Generating root index.html file")
    html_content = jinja_index_template.render(folder_name="", contents=tree)
    s3.put_object(
        Bucket=params["s3"]["public_bucket"],
        Key="index.html",
        Body=html_content,
        ContentType="text/html",
    )

    logger.info(f"Purging {len(keys)} keys from Cloudflare cache")
    purge_cloudflare_cache(
        keys,
        params["s3"]["public_data_url"],
        CLOUDFLARE_CACHE_ZONE_ID,
        CLOUDFLARE_CACHE_API_TOKEN,
    )
    logger.info("Public site created successfully")


if __name__ == "__main__":
    main()
