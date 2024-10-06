import argparse
import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import r5py

from utils.census import points_to_gdf


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
    origins = points_to_gdf(origins, "x_4326_wt", "y_4326_wt", "EPSG:4326")
    origins = origins[["geoid", "geometry"]]

    destinations = pd.read_parquet(destpoint_file)
    destinations = points_to_gdf(
        destinations, "x_4326_wt", "y_4326_wt", "EPSG:4326"
    )
    destinations = destinations[["geoid", "geometry"]]

    return origins, destinations


def load_network(
    year: str, geography: str, state: str
) -> r5py.TransportNetwork:
    network_file = (
        Path.cwd()
        / "intermediate"
        / "osmextract"
        / "year=state"
        / f"geography={geography}"
        / f"{state}"
        / f"{state}.osm.pbf"
    )

    network = r5py.TransportNetwork(network_file)

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
    parser.add_argument(
        "--year",
        required=True,
        help="The year of the point data.",
        type=str,
    )
    parser.add_argument(
        "--geography",
        required=True,
        help="The geography type of the point data.",
        type=str,
    )
    parser.add_argument(
        "--state",
        required=True,
        help="The two-digit state code for the point data.",
        type=str,
    )
    args = parser.parse_args()

    origins, destinations = fetch_points(args.year, args.geography, args.state)
    network = load_network(args.year, args.geography, args.state)

    travel_time_matrix = r5py.TravelTimeMatrixComputer(
        network,
        origins=origins,
        destinations=destinations,
        transport_modes=[r5py.TransportMode.CAR],
        departure=datetime.datetime(int(args.year), 1, 1, 14, 0, 0),
    ).compute_travel_times()
