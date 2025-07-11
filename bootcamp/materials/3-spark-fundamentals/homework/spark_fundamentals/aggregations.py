from pyspark.sql import SparkSession

spark = SparkSession.builder\
            .appName("IcebergTableManagement")\
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
            .config("spark.sql.bucketing.enabled", "true")\
            .config("spark.sql.join.preferSortMergeJoin", "false")\
            .getOrCreate()

# Top Players
df_top_players = spark.sql(
    """
        SELECT player_gamertag, AVG(player_total_kills) AS total_kill_rate
        FROM master_data
        GROUP BY player_gamertag
        ORDER BY total_kill_rate DESC
    """
).sortWithinPartitions("total_kill_rate")

df_top_players.write\
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.top_players")

# Top Playlists
df_top_playlist = spark.sql(
    """
    SELECT playlist_id, COUNT(*) AS playlist_occurence
    FROM master_data
    GROUP BY playlist_id
    ORDER BY playlist_occurence DESC
    """
).sortWithinPartitions("playlist_id")

df_top_playlist.write\
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.top_playlist")

# Top Maps
df_top_map = spark.sql(
    """
    SELECT mapid, COUNT(*) AS map_occurence
    FROM master_data
    GROUP BY mapid
    ORDER BY map_occurence DESC
    """
).sortWithinPartitions("map_occurence")

df_top_map.write\
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.top_map")

# Top Maps for Killing Spree
df_spree_map = spark.sql(
    """
    SELECT mapid, medal_id, COUNT(*) AS medals_achieved
    FROM master_data m
    WHERE medal_id = 2430242797
    GROUP BY ALL
    ORDER BY medals_achieved DESC
    """
).sortWithinPartitions("medals_achieved")

df_spree_map.write\
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.spree_map")
