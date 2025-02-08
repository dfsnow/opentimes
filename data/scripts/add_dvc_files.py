import os

import yaml

with open("params.yaml") as file:
    params = yaml.safe_load(file)

year = "2024"
states = params["input"]["state"]

for state in states:
    print("Processing state:", state)
    os.system(
        f"dvc commit -f intermediate/network/year={year}/geography=state/state={state}/network.dat"
    )
