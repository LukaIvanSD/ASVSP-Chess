import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def player_longest_streaks(df):

  white_df = df.select(
      F.col("DateTime"),
      F.col("Players.WhitePlayerName").alias("PlayerName"),
      F.lit("White").alias("Color"),
      F.col("Result.Result").alias("GameResult")
  )

  black_df = df.select(
      F.col("DateTime"),
      F.col("Players.BlackPlayerName").alias("PlayerName"),
      F.lit("Black").alias("Color"),
      F.col("Result.Result").alias("GameResult")
  )

  games_df = white_df.unionByName(black_df)
  games_df = games_df.repartition("PlayerName")

  games_df = games_df.withColumn(
      "IsWin",
      F.when(
          (F.col("Color") == "White") & (F.col("GameResult") == "White Won"), 1
      ).when(
          (F.col("Color") == "Black") & (F.col("GameResult") == "Black Won"), 1
      ).otherwise(0)
  )

  games_df = games_df.withColumn("IsLoss", 1 - F.col("IsWin"))

  window_player = Window.partitionBy("PlayerName").orderBy("DateTime")

  games_df = games_df.withColumn(
      "WinFlip",
      F.when(F.col("IsWin") != F.lag("IsWin", 1).over(window_player), 1).otherwise(0)
  )
  games_df = games_df.withColumn("WinGroup", F.sum(F.coalesce("WinFlip", F.lit(0))).over(window_player))

  games_df = games_df.withColumn(
      "ColdFlip",
      F.when(F.col("IsLoss") != F.lag("IsLoss", 1).over(window_player), 1).otherwise(0)
  )
  games_df = games_df.withColumn("ColdGroup", F.sum(F.coalesce("ColdFlip", F.lit(0))).over(window_player))

  win_streaks = games_df.groupBy("PlayerName", "WinGroup").agg(
      F.count("*").alias("WinStreakLength"),
      F.min("DateTime").alias("WinStreakStart"),
      F.max("DateTime").alias("WinStreakEnd"),
      F.max("IsWin").alias("WinFlag")
  ).filter(F.col("WinFlag") == 1).drop("WinFlag")

  cold_streaks = games_df.groupBy("PlayerName", "ColdGroup").agg(
      F.count("*").alias("ColdStreakLength"),
      F.min("DateTime").alias("ColdStreakStart"),
      F.max("DateTime").alias("ColdStreakEnd"),
      F.max("IsLoss").alias("ColdFlag")
  ).filter(F.col("ColdFlag") == 1).drop("ColdFlag")

  win_window = Window.partitionBy("PlayerName").orderBy(F.desc("WinStreakLength"))
  cold_window = Window.partitionBy("PlayerName").orderBy(F.desc("ColdStreakLength"))

  longest_win = win_streaks.withColumn("rn", F.row_number().over(win_window)).filter(F.col("rn") == 1).drop("rn", "WinGroup")
  longest_cold = cold_streaks.withColumn("rn", F.row_number().over(cold_window)).filter(F.col("rn") == 1).drop("rn", "ColdGroup")

  result_df = longest_win.join(longest_cold, on="PlayerName", how="outer")

  return result_df



if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("GOLD-Player-Longest-Streaks").getOrCreate()
    quiet_logs(spark)

    df_silver = spark.read.parquet(silver_path + "/game_type_partition")

    df_trend = player_longest_streaks(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "player_longest_streaks").save()
    spark.stop()