import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def day_period_above_average(df):
    df = df.withColumn(
        "Hour",
        F.hour("DateTime")
    )
    df = df.withColumn(
        "TimeOfDay",
        F.when((F.col("Hour") >= 5) & (F.col("Hour") < 12), "Morning")
            .when((F.col("Hour") >= 12) & (F.col("Hour") < 18), "Day")
            .otherwise("Evening")
    )

    white_df = df.select(
        F.col("Players.WhitePlayerName").alias("Player"),
        "TimeOfDay",
        F.when(F.col("Result.Result") == "White Won", "Win")
            .when(F.col("Result.Result") == "Black Won", "Loss")
            .otherwise("Draw").alias("Outcome")
    )
    black_df = df.select(
        F.col("Players.BlackPlayerName").alias("Player"),
        "TimeOfDay",
        F.when(F.col("Result.Result") == "Black Won", "Win")
            .when(F.col("Result.Result") == "White Won", "Loss")
            .otherwise("Draw").alias("Outcome")
    )

    player_games = white_df.unionByName(black_df)

    global_window = Window.partitionBy("Player")

    player_games = player_games.withColumn(
        "TotalGames",
        F.count("*").over(global_window)
    ).withColumn(
        "TotalWins",
        F.sum(F.when(F.col("Outcome")=="Win",1).otherwise(0)).over(global_window)
    ).withColumn(
        "GlobalWinRate",
        F.col("TotalWins") / F.col("TotalGames")
    )

    time_stats = player_games.groupBy("Player", "TimeOfDay") \
        .agg(
            F.count("*").alias("Games"),
            F.sum(F.when(F.col("Outcome")=="Win",1).otherwise(0)).alias("Wins"),
            F.sum(F.when(F.col("Outcome")=="Loss",1).otherwise(0)).alias("Losses"),
            F.sum(F.when(F.col("Outcome")=="Draw",1).otherwise(0)).alias("Draws"),
            F.max("GlobalWinRate").alias("GlobalWinRate")
        )

    time_stats = time_stats.withColumn(
        "WinRate",
        F.col("Wins") / F.col("Games")
    )

    above_avg = time_stats.filter(F.col("WinRate") > F.col("GlobalWinRate"))

    result = above_avg.groupBy("Player", "GlobalWinRate") \
        .agg(
            F.collect_list("TimeOfDay").alias("TimePeriods"),
            F.collect_list("Games").alias("GamesList"),
            F.collect_list("Wins").alias("WinsList"),
            F.collect_list("Losses").alias("LossesList"),
            F.collect_list("Draws").alias("DrawsList"),
            F.collect_list("WinRate").alias("WinRateList")
        )

    return result.select(
        F.col("Player").alias("playerName"),
        "GlobalWinRate",
        "TimePeriods",
        "GamesList",
        "WinsList",
        "LossesList",
        "DrawsList",
        "WinRateList"
    )


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Gold-Day-Period-Above-Average").getOrCreate()
    quiet_logs(spark)

    df_silver = spark.read.parquet(silver_path + "/game_type_partition")

    df_trend = day_period_above_average(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "day_period_above_average").save()
    spark.stop()