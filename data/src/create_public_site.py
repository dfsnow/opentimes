import os
from pathlib import Path
from datetime import datetime, timezone

import boto3
import duckdb
import requests as r
import yaml
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
    tree: dict, datasets: list[str], bucket_name: str, base_url: str, path: str
) -> None:
    """Create a DuckDB database object pointing to all bucket Parquet files."""

    con = duckdb.connect(database=path)
    con.execute("SET autoinstall_known_extensions=1;")
    con.execute("SET autoload_known_extensions=1;")
    for dataset in datasets:
        dataset_files = [
            f"{base_url}/{dataset}/{p}"
            for p in flatten_file_paths(tree, dataset, version)
        ]

        con.execute(
            f"""
            CREATE OR REPLACE VIEW {dataset} AS
            SELECT *
            FROM read_parquet(['{"', '".join(dataset_files)}'])
            """
        )

    con.close()


def flatten_file_paths(tree: dict, dataset: str, version: str) -> list[str]:
    """
    Flatten a nested dictionary to return full paths of
    .parquet filenames for a given dataset and version.
    """
    items = []

    def recurse(subtree: dict, parent_key: str = ""):
        for k, v in subtree.items():
            new_key = f"{parent_key}/{k}" if parent_key else k
            if isinstance(v, dict):
                if (
                    "filename" in v
                    and v["filename"].endswith(".parquet")
                    and version in new_key
                ):
                    items.append(new_key)
                else:
                    recurse(v, new_key)

    if dataset in tree:
        recurse(tree[dataset])
    return items


def generate_html_files(
    tree: dict,
    bucket_name: str,
    folder_path: str | Path = "",
):
    """Generate index.html files using Jinja2 for each folder."""
    index_file_path = Path(folder_path) / "index.html"
    html_content = template.render(folder_name=folder_path, contents=tree)

    s3.put_object(
        Bucket=bucket_name,
        Key=str(index_file_path.as_posix()),
        Body=html_content,
        ContentType="text/html",
    )

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

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get("Contents", [])
    tree: dict = {}

    keys = []
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
        last_modified = obj["LastModified"].replace(microsecond=0).isoformat()
        current[parts[-1]] = {
            "filename": parts[-1],
            "size": format_size(size),
            "last_modified": last_modified,
        }

        current = tree
        for part in parts[:-1]:
            current = current[part]
            update_directory_info(current, size, last_modified)

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

    print(f"Purging {len(keys)} keys from the Cloudflare cache.")
    for i in range(0, len(keys), 30):
        chunk = [f"{base_url}/{k}" for k in keys[i : i + 30]]
        data = {"files": chunk}
        r.post(url, headers=headers, json=data)


if __name__ == "__main__":
    tree, keys = get_s3_objects(params["s3"]["public_bucket"])

    versions = list(DATASET_DICT.keys())
    for version in versions:
        print(f"Creating DuckDB file for version {version}...")
        db_path = Path.cwd() / "site" / f"{version}.duckdb"
        create_duckdb_file(
            tree=tree,
            datasets=list(DATASET_DICT[version].keys()),
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

    print("Generating index.html files...")
    generate_html_files(tree, params["s3"]["public_bucket"])
    purge_cloudflare_cache(
        keys, CLOUDFLARE_CACHE_ZONE_ID, CLOUDFLARE_CACHE_API_TOKEN
    )
    print("Public site created successfully.")
