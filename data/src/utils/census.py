import shutil
import tempfile
from pathlib import Path

import pandas as pd
import geopandas as gpd


def calculate_weighted_mean(
    df: pd.DataFrame,
    group_cols: str | list[str],
    weight_col: str,
    value_cols: str | list[str],
):
    """
    Calculate the weighted mean of specified columns.

    :param df: The DataFrame containing the data.
    :param group_cols: The columns to group by.
    :param weight_col: The column to use as weights.
    :param value_cols: The columns to calculate the weighted mean for.
    :return: A DataFrame with the weighted means.
    """

    def weighted_mean(group, weight_col, value_col):
        return (group[value_col] * group[weight_col]).sum() / group[
            weight_col
        ].sum()

    grouped = df.groupby(group_cols)
    weighted_means = grouped.apply(
        lambda x: pd.Series(
            {col: weighted_mean(x, weight_col, col) for col in value_cols}
        )
    ).reset_index()

    return weighted_means


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
        result = {"state": geoid[:2], "county": geoid[2:5]}
        if len(geoid) == 11:
            result["tract"] = geoid[5:11]
        elif len(geoid) == 15:
            result["tract"] = geoid[5:11]
            result["block_group"] = geoid[11:12]
            result["block"] = geoid[11:15]
        elif len(geoid) != 5:
            raise ValueError("GEOID must be either 5, 11, or 15 digits long")

        return pd.Series(result)

    split_df = df[geoid_col].apply(split_geoid_value)
    return df.join(split_df)


def transform_5071_to_4327(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert x and y coordinates from EPSG:5071 to EPSG:4326.
    """
    df["geometry"] = gpd.points_from_xy(
        x=df["x_5071"], y=df["y_5071"], crs="EPSG:5071"
    )
    gdf = gpd.GeoDataFrame(data=df, geometry="geometry", crs="EPSG:5071")
    gdf.to_crs("EPSG:4326", inplace=True)
    gdf["x_4326"] = gdf.geometry.x
    gdf["y_4326"] = gdf.geometry.y
    gdf.drop(columns=["geometry"], inplace=True)
    df = pd.DataFrame(gdf)

    return df

