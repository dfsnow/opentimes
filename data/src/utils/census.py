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
    Calculate the weighted mean of specified columns. Returns the unweighted
    mean if the total weight is zero.

    Args:
        df: The DataFrame containing the data.
        group_cols: The columns to group by.
        weight_col: The column to use as weights.
        value_cols: The columns to calculate the weighted mean for.

    Returns:
        A DataFrame with the weighted means.
    """

    def weighted_mean(group, weight_col, value_col):
        total_weight = group[weight_col].sum()
        if total_weight == 0:
            return group[value_col].mean()
        return (group[value_col] * group[weight_col]).sum() / total_weight

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
    gdf = points_to_gdf(df, "intptlon", "intptlat", "EPSG:4326")
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

    Args:
        path: Path to the shapefile.

    Returns:
        A GeoDataFrame containing the shapefile contents.
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


def points_to_gdf(
    df: pd.DataFrame, x_col: str, y_col: str, crs: str
) -> gpd.GeoDataFrame:
    """
    Convert a DataFrame with x and y columns to a GeoDataFrame.
    """
    df["geometry"] = gpd.points_from_xy(x=df[x_col], y=df[y_col], crs=crs)
    gdf = gpd.GeoDataFrame(data=df, geometry="geometry", crs=crs)
    return gdf


def split_geoid(df: pd.DataFrame, geoid_col: str) -> pd.DataFrame:
    """
    Split a Census GEOID into component parts and append them as new columns.

    Args:
        df: A pandas DataFrame containing county, tract, or block GEOIDs.
        geoid_col: The column name containing the GEOIDs.

    Returns:
        The DataFrame with the split components appended as new columns.
    """

    def split_geoid_value(geoid):
        length_to_slices = {
            2: {"state": slice(0, 2)},
            5: {"state": slice(0, 2), "county": slice(2, 5)},
            11: {
                "state": slice(0, 2),
                "county": slice(2, 5),
                "tract": slice(5, 11),
            },
            12: {
                "state": slice(0, 2),
                "county": slice(2, 5),
                "tract": slice(5, 11),
                "block_group": slice(11, 12),
            },
            15: {
                "state": slice(0, 2),
                "county": slice(2, 5),
                "tract": slice(5, 11),
                "block_group": slice(11, 12),
                "block": slice(11, 15),
            },
        }

        if len(geoid) not in length_to_slices:
            raise ValueError(
                "GEOID must be either 2, 5, 11, 12, or 15 digits long"
            )

        slices = length_to_slices[len(geoid)]
        result = {key: geoid[slc] for key, slc in slices.items()}

        return pd.Series(result)

    split_df = df[geoid_col].apply(split_geoid_value)
    return df.join(split_df)


def transform_5071_to_4326(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert x and y coordinates from EPSG:5071 to EPSG:4326.
    """
    gdf = points_to_gdf(df, "x_5071", "y_5071", "EPSG:5071")
    gdf.to_crs("EPSG:4326", inplace=True)
    gdf["x_4326"] = gdf.geometry.x
    gdf["y_4326"] = gdf.geometry.y
    gdf.drop(columns=["geometry"], inplace=True)
    df = pd.DataFrame(gdf)

    return df
