from pyspark.sql import SparkSession, DataFrame
import geopy.distance

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE

#Obtem as latitudes e longitudes das estações de partida e chegada
def compute_distance(_spark: SparkSession, df: DataFrame) -> DataFrame:
    start_latitude = df["start_station_latitude"]
    start_longitude = df["start_station_longitude"]
    end_latitude = df["end_station_latitude"]
    end_longitude = df["end_station_longitude"]
    
    
    #Calculo da distancia entre as estações
    distance = geopy.distance.geodesic((start_latitude, start_longitude),
                                     (end_latitude, end_longitude)).km
    df["distance"] = distance
    print(df.head(5))
    return df

def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')