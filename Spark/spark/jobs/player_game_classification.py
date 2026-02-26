import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def classify_style(df):
  return df.withColumn(
      "Classification",
      F.when(F.abs(F.col("AvgEvalSwing")) < 0.4, "Glatke")
        .when((F.abs(F.col("AvgEvalSwing")) >= 0.4) & (F.abs(F.col("MaxEvalSwing")) < 1.5), "Uravnotežene")
        .when((F.abs(F.col("MaxEvalSwing")) >= 1.5) & (F.abs(F.col("MaxEvalSwing")) < 3.0), "Oštre")
        .when((F.abs(F.col("MaxEvalSwing")) >= 3.0) & (F.col("LeadChanges") <= 3), "Tesne")
        .when((F.abs(F.col("MaxEvalSwing")) >= 3.0) & (F.col("LeadChanges") > 3), "Divlje")
        .otherwise("Iznenađujuće")
  )
def player_game_classification(df):

  white_df = df.select(
    F.col("Players.WhitePlayerName").alias("Player"),
    F.col("Result.Result").alias("Outcome"),
    F.col("EvalStats.AvgEvalSwing").alias("AvgEvalSwing"),
    F.col("EvalStats.MaxEvalSwing").alias("MaxEvalSwing"),
    F.col("EvalStats.LeadChanges").alias("LeadChanges"),
    F.col("DateTime").cast("date").alias("Date"),
  )

  white_df = classify_style(white_df)

  white_df = white_df.withColumn(
      "PlayerResult",
      F.when(F.col("Outcome") == "White Won", "Win")
        .when(F.col("Outcome") == "Black Won", "Loss")
        .otherwise("Draw")
  )

  black_df = df.select(
      F.col("Players.BlackPlayerName").alias("Player"),
      F.col("Result.Result").alias("Outcome"),
      F.col("EvalStats.AvgEvalSwing").alias("AvgEvalSwing"),
      F.col("EvalStats.MaxEvalSwing").alias("MaxEvalSwing"),
      F.col("EvalStats.LeadChanges").alias("LeadChanges"),
      F.col("DateTime").cast("date").alias("Date"),
  )

  black_df = classify_style(black_df)

  black_df = black_df.withColumn(
      "PlayerResult",
      F.when(F.col("Outcome") == "Black Won", "Win")
        .when(F.col("Outcome") == "White Won", "Loss")
        .otherwise("Draw")
  )

  all_df = white_df.unionByName(black_df)

  daily = all_df.groupBy("Player", "Classification", "Date").agg(
      F.count("*").alias("Count"),
      F.sum(F.when(F.col("PlayerResult") == "Win", 1).otherwise(0)).alias("Wins"),
      F.sum(F.when(F.col("PlayerResult") == "Loss", 1).otherwise(0)).alias("Losses"),
      F.sum(F.when(F.col("PlayerResult") == "Draw", 1).otherwise(0)).alias("Draws"),
  )

  daily = daily.withColumn("WinPct", F.round(F.col("Wins") / F.col("Count") * 100, 2)) \
                .withColumn("LossPct", F.round(F.col("Losses") / F.col("Count") * 100, 2)) \
                .withColumn("DrawPct", F.round(F.col("Draws") / F.col("Count") * 100, 2))

  classification_trend = daily.groupBy("Player", "Classification").agg(
      F.collect_list(
          F.struct(
              F.col("Date"),
              F.col("Count"),
              F.col("Wins"),
              F.col("Losses"),
              F.col("Draws"),
              F.col("WinPct"),
              F.col("LossPct"),
              F.col("DrawPct"),
          )
      ).alias("Trend")
  )

  final_df = classification_trend.groupBy("Player").agg(
      F.collect_list(
          F.struct(
              F.col("Classification"),
              F.col("Trend")
          )
      ).alias("Classifications")
  )

  return final_df


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("GOLD-Player-Game-Classification").getOrCreate()
    quiet_logs(spark)

    df_silver = spark.read.parquet(silver_path + "/game_type_partition")

    df_trend = player_game_classification(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "player_game_classification").save()
    spark.stop()