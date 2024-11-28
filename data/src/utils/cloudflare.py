import time

import requests as r

from utils.utils import format_size


def _traverse_and_format(tree: dict) -> None:
    """Convert the total size of each directory to a human-readable format."""

    def format_directory_info(directory: dict) -> None:
        if "total_size" in directory:
            directory["total_size"] = format_size(directory["total_size"])

    for value in tree.values():
        if isinstance(value, dict):
            _traverse_and_format(value)
            if "filename" not in value:
                format_directory_info(value)


def _update_directory_info(
    directory: dict, size: int, last_modified: str
) -> None:
    """Update the total size and last modified date of a directory."""
    if "total_size" not in directory:
        directory["total_size"] = 0
    if "max_last_modified" not in directory:
        directory["max_last_modified"] = last_modified
    directory["total_size"] += size
    if last_modified > directory["max_last_modified"]:
        directory["max_last_modified"] = last_modified


def get_r2_objects(
    s3, bucket_name: str, prefix: str = ""
) -> tuple[dict, list]:
    """
    Retrieve a list of objects in a Cloudflare R2 bucket and return them as
    a nested dictionary with metadata.

    Args:
        s3: Boto3 S3 client.
        bucket_name: Name of the S3 bucket.
        prefix: Prefix to filter objects by.

    Returns:
        A tuple containing:
            - A nested dictionary representing the file/directory structure
            - A list of all object keys

    Example returned dictionary:
        {
            "data": {
                "total_size": "501 MB",
                "max_last_modified": "2023-05-20T15:30:00",
                "files": {
                    "filename": "example.txt",
                    "size": "1 MB",
                    "last_modified": "2023-05-20T15:30:00"
                },
                "subdir": {
                    "total_size": "500 MB",
                    "max_last_modified": "2023-05-19T10:15:00",
                    "another_file": {
                        "filename": "another.txt",
                        "size": "500 MB",
                        "last_modified": "2023-05-19T10:15:00"
                    }
                }
            }
        }
    """

    tree: dict = {}
    keys: list[str] = []
    continuation_token = None

    while True:
        params = {"Bucket": bucket_name, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**params)
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
                _update_directory_info(current, size, last_modified)

        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break

    _traverse_and_format(tree)
    return tree, keys


def purge_cloudflare_cache(
    keys: list[str], base_url: str, zone_id: str | None, token: str | None
) -> None:
    """Purge Cloudflare CDN cache for given keys, or all keys if > 10000."""
    if not token or not zone_id:
        raise ValueError("Cloudflare API token and zone ID must be provided.")

    url = f"https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    if len(keys) > 10000:
        r.post(url, headers=headers, json={"purge_everything": True})
        return

    calls_per_minute = 960  # Cloudflare limits to 1000 calls per minute
    call_interval = 60 / calls_per_minute
    for i in range(0, len(keys), 30):
        chunk = [f"{base_url}/{k}" for k in keys[i : i + 30]]
        r.post(url, headers=headers, json={"files": chunk})
        time.sleep(call_interval)
