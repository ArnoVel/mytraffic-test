import numpy as np

def haversine(lat_one, lon_one, lat_two, lon_two):
    """
    This method uses the ‘haversine’ formula to calculate the distance between two points specified using latitude
    and longitude. This gives a distance 'as the crow flies', that is around the earth's surface.

    Args:
        lat_one (float): First point's latitude in degrees
        lon_one (float): First point's longitude in degrees
        lat_two (float): Second point's latitude in degrees
        lon_two (float): Second point's longitude in degrees

    Returns:
        float: distance between the two points in km
    """
    earth_radius = 6371.0088
    # convert to radians
    lat_one, lon_one, lat_two, lon_two = map(np.radians, [lat_one, lon_one, lat_two, lon_two])
    # compute delta angle
    delta_lat = lat_two - lat_one
    delta_lon = lon_two - lon_one
    # haversine formula
    a = np.sin(delta_lat/2)**2 + np.cos(lat_one) * np.cos(lat_two) * np.sin(delta_lon/2) **2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    distance = earth_radius * c

    return float(distance)

def compute_distance(df):
    """
    Takes a Spark DataFrame and computes the distance between two gps coordinates
    using the appropriate columns for each row.

    Args:
        df (pyspark.sql.dataframe.DataFrame): input DataFrame with gps coordinates
    Returns:
        pyspark.sql.dataframe.DataFrame: column with the corresponding distance for each row
    """
    distances = df.rdd.map(lambda row: (haversine(float(row[7]),float(row[6]), float(row[9]), float(row[8])), float(row[11])))

    return distances.toDF(["distance", "trip_duration"])

def compute_average_speed(df):
    """
    Takes a pandas DataFrame and computes the average speed
    using the appropriate columns for each row.

    Args:
        df (pd.DataFrame): input DataFrame with gps coordinates and duration
    Returns:
        pd.Series: column with the corresponding average speed for each row in m/s
    """
    distances = compute_distance(df)
    average_speed = distances.rdd.map(lambda row: (1000 * row[0] / row[1], ))

    return average_speed.toDF(["average_speed"])
