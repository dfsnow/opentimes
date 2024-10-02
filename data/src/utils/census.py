import shutil
import tempfile
from pathlib import Path

import pandas as pd
import geopandas as gpd


def extract_centroids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract centroids from the INTPTLON and INTPTLAT columns of Census
    shapefiles. Returns one centroid as WGS84 and another as a planar
    projection.
    """
    df["geometry"] = gpd.points_from_xy(
        x=df["intptlon"], y=df["intptlat"], crs="EPSG:4326"
    )
    gdf = gpd.GeoDataFrame(data=df, geometry="geometry", crs="EPSG:4326")
    gdf["x_4326"] = gdf.geometry.x
    gdf["y_4326"] = gdf.geometry.y
    gdf.to_crs("EPSG:5071", inplace=True)
    gdf["x_5071"] = gdf.geometry.x
    gdf["y_5071"] = gdf.geometry.y
    gdf.drop(columns=["geometry"], inplace=True)
    df = pd.DataFrame(gdf)
    df = split_geoid(df, "geoid")

    return df


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
        gdf.columns = gdf.columns.str.lower()
        gdf.columns = gdf.columns.str.replace(r"\d+", "", regex=True)

        return gdf


def split_geoid(df: pd.DataFrame, geoid_col: str) -> pd.DataFrame:
    """
    Split a Census GEOID into component parts and append them as new columns.

    :param df: A pandas DataFrame containing county, tract, or block GEOIDs.
    :param geoid_col: The column name containing the GEOIDs.
    :return: The DataFrame with the split components appended as new columns.
    """
    def split_geoid_value(geoid):
        result = {
            "state": geoid[:2],
            "county": geoid[2:5]
        }
        if len(geoid) == 11:
            result["tract"] = geoid[5:11]
        elif len(geoid) == 15:
            result["tract"] = geoid[5:11]
            result["block"] = geoid[11:15]
        elif len(geoid) != 5:
            raise ValueError("GEOID must be either 5, 11, or 15 digits long")

        return pd.Series(result)

    split_df = df[geoid_col].apply(split_geoid_value)
    return df.join(split_df)
