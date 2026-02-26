import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def player_figure_bad_score_streak(df):
  white_df = df.select(
      F.col("GameID"),
      F.col("DateTime"),
      F.col("Players.WhitePlayerName").alias("PlayerName"),
      F.lit("White").alias("Color"),
      F.col("Result.Result").alias("GameResult"),
      F.explode(F.col("`FigureStats.WhiteFigureStats`")).alias("Figure" )
    )

  black_df = df.select(
      F.col("GameID"),
      F.col("DateTime"),
      F.col("Players.BlackPlayerName").alias("PlayerName"),
      F.lit("Black").alias("Color"),
      F.col("Result.Result").alias("GameResult"),
      F.explode(F.col("`FigureStats.BlackFigureStats`")).alias("Figure")
  )

  figures_df = white_df.unionByName(black_df)

  figures_df = figures_df.repartition("PlayerName", F.col("Figure.Figure"))

  figures_df = figures_df.withColumn(
      "Score",
      F.coalesce(F.col("Figure.Best"), F.lit(0)) * 5 +
      F.coalesce(F.col("Figure.Great"), F.lit(0)) * 3 +
      F.coalesce(F.col("Figure.Good"), F.lit(0)) * 2 -
      F.coalesce(F.col("Figure.Inaccuracy"), F.lit(0)) * 1 -
      F.coalesce(F.col("Figure.Mistake"), F.lit(0)) * 3 -
      F.coalesce(F.col("Figure.Blunder"), F.lit(0)) * 5 +
      F.coalesce(F.col("Figure.Captures"), F.lit(0)) * 0.5 +
      F.coalesce(F.col("Figure.Checks"), F.lit(0)) * 0.5
  )

  figures_df = figures_df.withColumn(
      "IsWin",
      F.when(
          (F.col("Color") == "White") & (F.col("GameResult") == "White Won"), 1
      ).when(
          (F.col("Color") == "Black") & (F.col("GameResult") == "Black Won"), 1
      ).otherwise(0)
  )

  figures_df = figures_df.withColumn(
      "IsNonPositive",
      F.when(F.col("Score") <= 0, 1).otherwise(0)
  )

  figures_df = figures_df.withColumn(
      "PositiveFlag",
      F.when(F.col("Score") > 0, 1).otherwise(0)
  )

  window_spec = Window.partitionBy(
      "PlayerName",
      F.col("Figure.Figure")
  ).orderBy("DateTime")

  figures_df = figures_df.withColumn(
      "GroupId",
      F.sum("PositiveFlag").over(window_spec)
  )

  streak_df = (
      figures_df
      .filter(F.col("IsNonPositive") == 1)
      .groupBy(
          "PlayerName",
          F.col("Figure.Figure").alias("Figure"),
          "GroupId"
      )
      .agg(
          F.count("*").alias("StreakLength"),
          F.sum("IsWin").alias("WinsInStreak")
      )
      .withColumn(
          "WinPercentage",
          F.when(
              F.col("StreakLength") > 0,
              F.col("WinsInStreak") / F.col("StreakLength") * 100
          ).otherwise(0.0)
      )
  )

  window_max = Window.partitionBy(
      "PlayerName",
      "Figure"
  ).orderBy(F.desc("StreakLength"))

  final_df = (
      streak_df
      .withColumn("rn", F.row_number().over(window_max))
      .filter(F.col("rn") == 1)
      .drop("rn", "GroupId")
  )

  struct_df = final_df.withColumn(
        "FigureInfo",
        F.struct(
            F.col("Figure"),
            F.col("StreakLength"),
            F.col("WinsInStreak"),
            F.col("WinPercentage")
        )
    )

  aggregated_df = struct_df.groupBy("PlayerName").agg(
      F.collect_list("FigureInfo").alias("Figures")
  )

  return aggregated_df


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("GOLD-Player-Figure-Bad-Score-Streak").getOrCreate()
    quiet_logs(spark)

    df_silver = spark.read.parquet(silver_path + "/game_type_partition")

    df_trend = player_figure_bad_score_streak(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "player_figure_bad_score_streak").save()
    spark.stop()