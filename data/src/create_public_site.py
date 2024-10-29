import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import boto3
import duckdb
import requests as r
import yaml
from botocore.exceptions import ClientError
from jinja2 import Environment, FileSystemLoader
from utils.datasets import DATASET_DICT
from utils.utils import format_size

# Load parameters and connect to S3/R2
params = yaml.safe_load(open("params.yaml"))
session = boto3.Session(profile_name=params["s3"]["profile"])
s3 = session.client("s3", endpoint_url=params["s3"]["endpoint"])

# Initialize Jinja2 environment
env = Environment(loader=FileSystemLoader("site/templates"))
template = env.get_template("index.html")

# Load Cloudflare API credentials
CLOUDFLARE_CACHE_API_TOKEN = os.environ.get("CLOUDFLARE_CACHE_API_TOKEN")
CLOUDFLARE_CACHE_ZONE_ID = os.environ.get("CLOUDFLARE_CACHE_ZONE_ID")


def append_duckdb_info(tree: dict, version: str, db_path: Path) -> None:
    """Append or overwrite DuckDB file location and information in the tree."""
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


def create_duckdb_file(
    tree: dict,
    datasets: list[str],
    version: str,
    modes: list[str],
    years: list[str],
    geographies: list[str],
    bucket_name: str,
    base_url: str,
    path: str,
) -> None:
    """Create a DuckDB database object pointing to all bucket Parquet files."""

    Path(path).unlink(missing_ok=True)
    con = duckdb.connect()
    con.execute("SET autoinstall_known_extensions=1;")
    con.execute("SET autoload_known_extensions=1;")
    con.execute(f"ATTACH '{path}' AS opentimes;")
    con.execute("CREATE SCHEMA IF NOT EXISTS opentimes.public;")
    for dataset in datasets:
        all_dataset_files = []
        for mode in modes:
            for year in years:
                for geography in geographies:
                    dataset_files = [
                        f"{base_url}/{dataset}/{p}"
                        for p in flatten_file_paths(
                            tree, dataset, version, mode, year, geography
                        )
                    ]
                    if not dataset_files:
                        continue

                    all_dataset_files += dataset_files

        if all_dataset_files:
            con.execute(
                f"""
                CREATE OR REPLACE VIEW opentimes.public.{dataset} AS
                SELECT *
                FROM read_parquet(['{"', '".join(all_dataset_files)}'])
                """
            )

    con.close()


def flatten_file_paths(
    tree: dict,
    dataset: str,
    version: str,
    mode: str,
    year: str,
    geography: str,
) -> list[str]:
    """
    Flatten a nested dictionary to return full paths of
    .parquet filenames for a given dataset and version.
    """
    items = []

    def recurse(subtree: dict, parent_key: str = ""):
        for k, v in subtree.items():
            new_key = f"{parent_key}/{k}" if parent_key else k
            if isinstance(v, dict):
                if "filename" in v and v["filename"].endswith(".parquet"):
                    items.append(new_key)
                else:
                    recurse(v, new_key)

    version_str = f"version={version}"
    mode_str = f"mode={mode}"
    year_str = f"year={year}"
    geography_str = f"geography={geography}"
    if (
        dataset in tree
        and version_str in tree[dataset]
        and mode_str in tree[dataset][version_str]
        and year_str in tree[dataset][version_str][mode_str]
        and geography_str in tree[dataset][version_str][mode_str][year_str]
    ):
        recurse(tree[dataset][version_str][mode_str][year_str][geography_str])
    return [
        f"{version_str}/{mode_str}/{year_str}/{geography_str}/{item}"
        for item in items
    ]


def generate_html_files(
    tree: dict,
    bucket_name: str,
    folder_path: str | Path = "",
):
    """Generate index.html files using Jinja2 for each folder."""
    index_file_path = Path(folder_path) / "index.html"
    html_content = template.render(folder_name=folder_path, contents=tree)

    retries = 3
    for attempt in range(retries):
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=str(index_file_path.as_posix()),
                Body=html_content,
                ContentType="text/html",
            )
            break  # Exit the loop if successful
        except ClientError as e:
            if attempt < retries - 1:
                print(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(2**attempt)
            else:
                print(f"Failed after {retries} attempts")
                raise e

    print("Rendered index.html for", str(index_file_path.as_posix()))
    # Recursively create subfolders and their index.html
    for item, subtree in tree.items():
        if isinstance(subtree, dict) and "filename" not in subtree:
            generate_html_files(subtree, bucket_name, Path(folder_path) / item)


def get_s3_objects(bucket_name: str, prefix: str = "") -> tuple[dict, list]:
    """Retrieve a list of objects in the S3 bucket with a given prefix."""
    print("Retrieving objects from S3 bucket...")

    def update_directory_info(directory, size, last_modified):
        if "total_size" not in directory:
            directory["total_size"] = 0
        if "max_last_modified" not in directory:
            directory["max_last_modified"] = last_modified
        directory["total_size"] += size
        if last_modified > directory["max_last_modified"]:
            directory["max_last_modified"] = last_modified

    def format_directory_info(directory):
        if "total_size" in directory:
            directory["total_size"] = format_size(directory["total_size"])

    def traverse_and_format(tree):
        for key, value in tree.items():
            if isinstance(value, dict):
                traverse_and_format(value)
                if "filename" not in value:
                    format_directory_info(value)

    tree: dict = {}
    keys = []
    continuation_token = None

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get("Contents", [])

    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuation_token,
            )
        else:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        objects = response.get("Contents", [])
        for obj in objects:
            path = obj["Key"]
            keys.append(path)
            if path.endswith("index.html"):
                continue
            parts = path.split("/")
            current = tree

            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]

            size = obj["Size"]
            last_modified = (
                obj["LastModified"].replace(microsecond=0).isoformat()
            )
            current[parts[-1]] = {
                "filename": parts[-1],
                "size": format_size(size),
                "last_modified": last_modified,
            }

            current = tree
            for part in parts[:-1]:
                current = current[part]
                update_directory_info(current, size, last_modified)

        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break

    traverse_and_format(tree)
    print(f"Retrieved {len(keys)} objects from S3 bucket.")

    return tree, keys


def purge_cloudflare_cache(
    keys: list[str], zone_id: str | None, token: str | None
) -> None:
    if not token or not zone_id:
        raise ValueError("Cloudflare API token and zone ID must be provided.")
    base_url = params["s3"]["public_data_url"]

    url = f"https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    if len(keys) > 10000:
        print(f"Purging the entire cache due to too many keys ({len(keys)}).")
        r.post(url, headers=headers, json={"purge_everything": True})

    print(f"Purging {len(keys)} keys from the Cloudflare cache.")
    calls_per_minute = 960  # Cloudflare limits to 1000 calls per minute
    call_interval = 60 / calls_per_minute

    for i in range(0, len(keys), 30):
        chunk = [f"{base_url}/{k}" for k in keys[i : i + 30]]
        data = {"files": chunk}
        r.post(url, headers=headers, json=data)
        time.sleep(call_interval)


if __name__ == "__main__":
    tree, keys = get_s3_objects(params["s3"]["public_bucket"])

    versions = list(DATASET_DICT.keys())
    for version in versions:
        print(f"Creating DuckDB file for version {version}...")
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
        print(f"DuckDB file created at {db_path}.")

    # Recursively create subfolders and their index.html
    print("Generating HTML index files...")
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                generate_html_files,
                subtree,
                params["s3"]["public_bucket"],
                Path(item),
            )
            for item, subtree in tree.items()
            if isinstance(subtree, dict) and "filename" not in subtree
        ]
        for future in as_completed(futures):
            future.result()  # Ensure any exceptions are raised

    # Generate the root index.html
    html_content = template.render(folder_name="", contents=tree)
    s3.put_object(
        Bucket=params["s3"]["public_bucket"],
        Key="index.html",
        Body=html_content,
        ContentType="text/html",
    )

    purge_cloudflare_cache(
        keys, CLOUDFLARE_CACHE_ZONE_ID, CLOUDFLARE_CACHE_API_TOKEN
    )
    print("Public site created successfully.")
