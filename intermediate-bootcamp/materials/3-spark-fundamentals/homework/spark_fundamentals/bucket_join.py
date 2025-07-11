from pyspark.sql import SparkSession

spark = SparkSession.builder\
            .appName("IcebergTableManagement")\
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
            .config("spark.sql.bucketing.enabled", "true")\
            .config("spark.sql.join.preferSortMergeJoin", "false")\
            .getOrCreate()

# Read data from csv
df_matches = spark.read.option("header", "true")\
                       .option("inferSchema", "true")\
                       .csv("/home/iceberg/data/matches.csv")

df_match_details = spark.read.option("header", "true")\
                        .option("inferSchema", "true")\
                        .csv("/home/iceberg/data/match_details.csv")

df_medals_matches_players = spark.read.option("header", "true")\
                                 .option("inferSchema", "true")\
                                 .csv("/home/iceberg/data/medals_matches_players.csv")

# Re-order data
df_matches = df_matches.repartition(16, "match_id").sortWithinPartitions("match_id")
df_match_details = df_match_details.repartition(16, "match_id").sortWithinPartitions("match_id")
df_medals_matches_players = df_medals_matches_players.repartition(16, "match_id").sortWithinPartitions("match_id")

# Write data in buckets
df_matches.write\
    .bucketBy(16, "match_id")\
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.matches_bucketed")

df_match_details.write\
    .bucketBy(16, "match_id")\
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.match_details_bucketed")

df_medals_matches_players.write\
    .bucketBy(16, "match_id")\
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.medals_matches_players_bucketed")

# Read Iceberg managed tables
df_matches = spark.read.table("demo.bootcamp.matches_bucketed")
df_match_details = spark.read.table("demo.bootcamp.match_details_bucketed")
df_medals_matches_players = spark.read.table("demo.bootcamp.medals_matches_players_bucketed")

df_matches.createOrReplaceTempView("matches")
df_match_details.createOrReplaceTempView("match_details")
df_medals_matches_players.createOrReplaceTempView("medals_matches_players")

# Bucket Join
df_matches.createOrReplaceTempView("matches")
df_match_details.createOrReplaceTempView("match_details")
df_medals_matches_players.createOrReplaceTempView("medals_matches_players")

df = spark.sql("""
    SELECT
        m.match_id,
        md.player_gamertag,
        mapid,
        is_team_game,
        playlist_id,
        game_variant_id,
        is_match_over,
        completion_date,
        match_duration,
        game_mode,
        map_variant_id,
        previous_spartan_rank,
        spartan_rank,
        previous_total_xp,
        total_xp,
        previous_csr_tier,
        previous_csr_designation,
        previous_csr,
        previous_csr_percent_to_next_tier,
        previous_csr_rank,
        current_csr_tier,
        current_csr_designation,
        current_csr,
        current_csr_percent_to_next_tier,
        current_csr_rank,
        player_rank_on_team,
        player_finished,
        player_average_life,
        player_total_kills,
        player_total_headshots,
        player_total_weapon_damage,
        player_total_shots_landed,
        player_total_melee_kills,
        player_total_melee_damage,
        player_total_assassinations,
        player_total_ground_pound_kills,
        player_total_shoulder_bash_kills,
        player_total_grenade_damage,
        player_total_power_weapon_damage,
        player_total_power_weapon_grabs,
        player_total_deaths,
        player_total_assists,
        player_total_grenade_kills,
        did_win,
        team_id,
        medal_id,
        count
    FROM matches m
    INNER JOIN match_details md ON m.match_id = md.match_id
    INNER JOIN medals_matches_players mmp ON m.match_id = mmp.match_id AND md.player_gamertag = mmp.player_gamertag
""")

df.createOrReplaceTempView("master_data")
