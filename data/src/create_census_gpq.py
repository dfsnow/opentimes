import argparse
import os
import shutil
import tempfile
from pathlib import Path

import geopandas as gpd


def create_census_gpq(
    year: str, geography: str, state: str | None = None
) -> None:

    input_dir = (
        Path.cwd()
        / "input"
        / "tiger"
        / f"year={year}"
        / f"geography={geography}"
    )
    if not state:
        input_file = input_dir / f"{geography}.zip"
    else:
        input_dir = input_dir / f"state={state}"
        input_file = input_dir / f"{state}.zip"

    with tempfile.TemporaryDirectory() as tmpdirname:
        shutil.unpack_archive(input_file, tmpdirname)
        tmpdir_path = Path(tmpdirname)

        shapefile_path = None
        for shp_file in tmpdir_path.rglob("*.shp"):
            shapefile_path = shp_file
            break

        if shapefile_path is None:
            raise FileNotFoundError("Shapefile not found in TIGER file")

        gdf = gpd.read_file(shapefile_path)
        gdf.columns = gdf.columns.str.lower()
        cols_to_keep = ["geoid", "geometry"]
        gdf = gdf.drop(columns=[col for col in gdf.columns if col not in cols_to_keep])
        gdf = gdf.to_crs("EPSG:4326")

        # Grab the unweighted centroid
        gdf["unweighted_centroid"] = gdf.geometry.centroid
        gdf["unweighted_centroid_x"] = gdf.centroid.x
        gdf["unweighted_centroid_y"] = gdf.centroid.y
        gdf = gdf.drop(columns=["unweighted_centroid"])

        breakpoint()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create Census GeoParquet extract"
    )
    parser.add_argument(
        "--year", required=True, help="The year of the TIGER/Line data."
    )
    parser.add_argument(
        "--geography",
        required=True,
        help="The geography type of the shapefile.",
    )
    parser.add_argument(
        "--state",
        required=False,
        help="The two-digit state code for the shapefile.",
    )

    args = parser.parse_args()

    create_census_gpq(args.year, args.geography, args.state)
