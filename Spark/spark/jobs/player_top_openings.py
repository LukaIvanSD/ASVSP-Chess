import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
def top_openings_stats(df, top_n=5):
  white_df = df.select(
      F.col("Players.WhitePlayerName").alias("Player"),
      "GameType",
      F.col("Opening.OpeningName").alias("Opening"),
      F.when(F.col("Result.Result") == "White Won", "Win")
        .when(F.col("Result.Result") == "Black Won", "Loss")
        .otherwise("Draw").alias("Outcome")
  )
  black_df = df.select(
      F.col("Players.BlackPlayerName").alias("Player"),
      "GameType",
      F.col("Opening.OpeningName").alias("Opening"),
      F.when(F.col("Result.Result") == "Black Won", "Win")
        .when(F.col("Result.Result") == "White Won", "Loss")
        .otherwise("Draw").alias("Outcome")
  )

  player_games = white_df.unionByName(black_df).repartition("Player")
  def top_n_openings(input_df, group_cols):
      stats = input_df.groupBy(*group_cols, "Opening") \
                      .agg(
                          F.count("*").alias("Games"),
                          F.sum(F.when(F.col("Outcome") == "Win", 1).otherwise(0)).alias("Wins"),
                          F.sum(F.when(F.col("Outcome") == "Loss", 1).otherwise(0)).alias("Losses"),
                          F.sum(F.when(F.col("Outcome") == "Draw", 1).otherwise(0)).alias("Draws")
                      )
      stats = stats.withColumn("WinPct", F.col("Wins") / F.col("Games")) \
                    .withColumn("LossPct", F.col("Losses") / F.col("Games")) \
                    .withColumn("DrawPct", F.col("Draws") / F.col("Games"))

      rank_window = Window.partitionBy(*group_cols).orderBy(F.col("Games").desc())
      stats = stats.withColumn("Rank", F.row_number().over(rank_window)) \
             .filter(F.col("Rank") <= top_n) \
             .drop("Rank")
      return stats

  per_type_top = top_n_openings(player_games, ["Player", "GameType"])
  per_type_struct = per_type_top.groupBy("Player", "GameType").agg(
      F.collect_list(
          F.struct(
              "Opening", "Games", "Wins", "Losses", "Draws", "WinPct", "LossPct", "DrawPct"
          )
      ).alias("Top")
  )

  global_top = top_n_openings(player_games, ["Player"]) \
                  .withColumn("GameType", F.lit("ALL"))
  global_struct = global_top.groupBy("Player").agg(
      F.collect_list(
          F.struct(
              "Opening", "Games", "Wins", "Losses", "Draws", "WinPct", "LossPct", "DrawPct"
          )
      ).alias("Top")
  ).withColumn("GameType", F.lit("ALL"))

  final_df = per_type_struct.unionByName(global_struct) \
                            .groupBy("Player") \
                            .agg(
                                F.collect_list(
                                    F.struct("GameType", "Top")
                                ).alias("TopOpenings")
                            )

  return final_df.select(
      F.col("Player").alias("playerName"),
      "TopOpenings"
  )




if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("GOLD-Player-Top-Openings").getOrCreate()
    quiet_logs(spark)

    df_silver = spark.read.parquet(silver_path + "/game_type_partition")

    df_trend = top_openings_stats(df_silver, top_n=5)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "player_top_openings").save()
    spark.stop()