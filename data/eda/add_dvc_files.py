import os

import yaml

params = yaml.safe_load(open("params.yaml"))

year = "2024"
states = params["input"]["state"]

for state in states:
    print("Processing state:", state)
    os.system(
        f"dvc commit -f intermediate/network/year={year}/geography=state/state={state}/network.dat"
    )
