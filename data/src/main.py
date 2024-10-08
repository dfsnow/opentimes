import sys
import argparse
import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd

from utils.census import points_to_gdf

r5_path = Path.cwd() / "r5-custom.jar"
dir_path = Path.cwd() / "temp"
sys.argv.append("--verbose")
sys.argv.append("--max-memory")
sys.argv.append("16G")
sys.argv.append("--temporary-directory")
sys.argv.append(dir_path.as_posix())
sys.argv.append("--r5-classpath")
sys.argv.append(r5_path.as_posix())

import r5py


def fetch_points(
    year: str, geography: str, state: str
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Return two GeoDataFrames: one of origins and one of destinations, to use
    for routing.
    """
    cenloc_file = (
        Path.cwd()
        / "intermediate"
        / "cenloc"
        / f"year={year}"
        / f"geography={geography}"
        / f"state={state}"
        / f"{state}.parquet"
    )
    destpoint_file = (
        Path.cwd()
        / "intermediate"
        / "destpoint"
        / f"year={year}"
        / f"geography={geography}"
        / f"state={state}"
        / f"{state}.parquet"
    )

    origins = pd.read_parquet(cenloc_file)
    origins = points_to_gdf(origins, "x_4326", "y_4326", "EPSG:4326")
    origins = origins.rename(columns={"geoid": "id"})[["id", "geometry"]]

    destinations = pd.read_parquet(destpoint_file)
    destinations = points_to_gdf(destinations, "x_4326", "y_4326", "EPSG:4326")
    destinations = destinations.rename(columns={"geoid": "id"})[
        ["id", "geometry"]
    ]

    return origins, destinations


def load_network(
    year: str, geography: str, state: str
) -> r5py.TransportNetwork:
    network_file = (
        Path.cwd()
        / "intermediate"
        / "osmextract"
        / f"year={year}"
        / f"geography=state"
        / f"state={state}"
        / f"{state}.osm.pbf"
    )

    network = r5py.TransportNetwork(osm_pbf=network_file.as_posix())

    return network


#
# def calculate_times(
#     origins: pd.DataFrame,
#     destinations: pd.DataFrame,
#     network: r5py.TransportNetwork,
# ) -> None:
#     """
#     Calculate times between provided origins and destinations using the
#     provided street network.
#     """


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    args, _ = parser.parse_known_args()

    origins, destinations = fetch_points(args.year, args.geography, args.state)
    network = load_network(args.year, args.geography, args.state)
    origins["geometry"] = network.snap_to_network(origins["geometry"])
    destinations["geometry"] = network.snap_to_network(
        destinations["geometry"]
    )

    travel_time_matrix = r5py.TravelTimeMatrixComputer(
        network,
        origins=origins,
        destinations=destinations,
        transport_modes=[r5py.TransportMode.CAR],
        max_time=datetime.timedelta(seconds=30000),
    ).compute_travel_times()

    travel_time_matrix.to_parquet(f"temp_{args.state}.parquet")
