import time

import requests as r

from utils.utils import format_size


def get_r2_objects(
    s3, bucket_name: str, prefix: str = ""
) -> tuple[dict, list]:
    """Retrieve a list of objects in the S3 bucket with a given prefix."""

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

    return tree, keys


def purge_cloudflare_cache(
    keys: list[str], base_url: str, zone_id: str | None, token: str | None
) -> None:
    if not token or not zone_id:
        raise ValueError("Cloudflare API token and zone ID must be provided.")

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
