import shutil
import tempfile
from pathlib import Path

import pandas as pd
import geopandas as gpd


def load_shapefile(path: str | Path) -> gpd.GeoDataFrame:
    """
    Load a shapefile into as a GeoDataFrame by first unpacking into a
    temporary directory.

    :param path: Path to the shapefile.
    :return: A GeoDataFrame containing the shapefile contents.
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        shutil.unpack_archive(path, tmpdirname)
        tmpdir_path = Path(tmpdirname)

        shapefile_path = None
        for shp_file in tmpdir_path.rglob("*.shp"):
            shapefile_path = shp_file
            break

        if shapefile_path is None:
            raise FileNotFoundError("Shapefile not found in file")

        gdf = gpd.read_file(shapefile_path)

        return gdf


def split_geoid(df: pd.DataFrame, geoid_col: str) -> pd.DataFrame:
    """
    Split a Census GEOID into component parts and append them as new columns.

    :param df: A pandas DataFrame containing county, tract, or block GEOIDs.
    :param geoid_col: The column name containing the GEOIDs.
    :return: The DataFrame with the split components appended as new columns.
    """
    def split_geoid_value(geoid):
        if len(geoid) == 5:
            state = geoid[:2]
            county = geoid[2:5]
            tract = None
            block = None
        elif len(geoid) == 11:
            state = geoid[:2]
            county = geoid[2:5]
            tract = geoid[5:11]
            block = None
        elif len(geoid) == 15:
            state = geoid[:2]
            county = geoid[2:5]
            tract = geoid[5:11]
            block = geoid[11:15]
        else:
            raise ValueError("GEOID must be either 5, 11, or 15 digits long")

        return pd.Series({
            'state': state,
            'county': county,
            'tract': tract,
            'block': block
        })

    split_df = df[geoid_col].apply(split_geoid_value)
    return df.join(split_df)
