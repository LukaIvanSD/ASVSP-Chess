import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def player_elo_gain_trend(df, window_size=10):
  white_df = df.select(
      F.col("Players.WhitePlayerName").alias("PlayerName"),
      F.col("DateTime"),
      F.col("Result.WhiteDiff").alias("Diff")
  )
  black_df = df.select(
      F.col("Players.BlackPlayerName").alias("PlayerName"),
      F.col("DateTime"),
      F.col("Result.BlackDiff").alias("Diff")
  )
  games_df = white_df.unionByName(black_df)

  window_player = Window.partitionBy("PlayerName").orderBy("DateTime")

  games_df = games_df.withColumn("RowNum", F.row_number().over(window_player))

  rolling_window = Window.partitionBy("PlayerName")\
                          .orderBy("RowNum")\
                          .rowsBetween(-(window_size-1), 0)

  games_df = games_df.withColumn("RollingSum", F.sum("Diff").over(rolling_window))
  games_df = games_df.withColumn("WindowStartDate", F.min("DateTime").over(rolling_window))
  games_df = games_df.withColumn("WindowEndDate", F.max("DateTime").over(rolling_window))

  max_window = Window.partitionBy("PlayerName").orderBy(F.desc("RollingSum"))

  result_df = games_df.withColumn("rn", F.row_number().over(max_window)).filter(F.col("rn") == 1)

  result_df = result_df.select(
      "PlayerName",
      F.col("RollingSum").alias("MaxDiffInWindow"),
      F.col("WindowStartDate").alias("StartDate"),
      F.col("WindowEndDate").alias("EndDate")
  )

  return result_df

if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("GOLD-Player-Elo-Gain-Trend").getOrCreate()
    quiet_logs(spark)

    df_silver = spark.read.parquet(silver_path + "/game_type_partition")

    df_trend = player_elo_gain_trend(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "player_elo_gain_trend").save()
    spark.stop()