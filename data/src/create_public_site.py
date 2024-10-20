import argparse
import json
import os
from pathlib import Path

import boto3
import requests as r
import yaml
from jinja2 import Environment, FileSystemLoader

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


def get_s3_objects(bucket_name: str, prefix: str = "") -> tuple[dict, list]:
    """Retrieve a list of objects in the S3 bucket with a given prefix."""
    print("Retrieving objects from S3 bucket...")

    def format_size(size):
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024

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


def save_inventory_json(
    tree: dict, output_dir: str | Path, output_file: str
) -> None:
    """Save the tree structure of the S3 bucket as a JSON file."""
    output_path = Path(output_dir) / output_file

    with open(output_path, "w") as f:
        json.dump(tree, f, indent=2)


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

    for i in range(0, len(keys), 30):
        chunk = [f"{base_url}/{k}" for k in keys[i : i + 30]]
        data = {"files": chunk}
        print(
            f"Purging the following keys from Cloudflare cache: {', '.join(chunk)}"
        )
        r.post(url, headers=headers, json=data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket_name", required=True, type=str)
    parser.add_argument("--output_dir", required=False, type=str)
    parser.add_argument("--output_file", required=False, type=str)
    args = parser.parse_args()

    tree, keys = get_s3_objects(args.bucket_name)
    save_inventory_json(
        tree,
        args.output_dir or Path.cwd() / "site",
        args.output_file or "inventory.json",
    )
    print("Generating index.html files...")
    generate_html_files(tree, args.bucket_name)
    purge_cloudflare_cache(
        keys, CLOUDFLARE_CACHE_ZONE_ID, CLOUDFLARE_CACHE_API_TOKEN
    )
    print("Public site created successfully.")
