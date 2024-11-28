import os
from pathlib import Path

# Path relative to the mounts within the Valhalla Docker container
DOCKER_INTERNAL_PATH = Path(os.environ.get("DOCKER_INTERNAL_PATH", Path.cwd()))
