from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder\
            .appName("broadcast_join")\
            .config("spark.driver.memory", "4g")\
            .config("spark.executor.memory", "4g")\
            .config("spark.memory.offHeap.enabled", "true")\
            .config("spark.memory.offHeap.size", "8g")\
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .config("spark.dynamicAllocation.minExecutors", "1")\
            .config("spark.dynamicAllocation.maxExecutors", "50")\
            .getOrCreate()

# Read data from csv
df_matches = spark.read.option("header", "true")\
                       .option("inferSchema", "true")\
                       .csv("/home/iceberg/data/matches.csv")

df_medals_matches_players = spark.read.option("header", "true")\
                                 .option("inferSchema", "true")\
                                 .csv("/home/iceberg/data/medals_matches_players.csv")

df_maps = spark.read.option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("/home/iceberg/data/maps.csv")

df_medals = spark.read.option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("/home/iceberg/data/medals.csv")

# Broadcast join
df_medals_per_game = df_medals_matches_players.join(df_medals, "medal_id", "inner").join(df_matches, "match_id", "inner")
df_medals_per_map = df_medals_per_game.join(broadcast(df_maps), "mapid", "inner")

df_medals_per_map.show()
