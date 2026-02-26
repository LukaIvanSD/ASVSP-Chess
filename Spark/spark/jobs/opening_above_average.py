import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def top_openings_by_gametype(df_silver, min_games=100):
    df_grouped = (
        df_silver
        .groupBy("GameType", "OpeningName")
        .agg(
            F.count("*").alias("UsedNum"),
            F.sum("WhiteWin").alias("Wins")
        )
        .withColumn("WinRate", F.col("Wins") / F.col("UsedNum"))
    )

    window_by_gametype = Window.partitionBy("GameType")
    df_grouped = df_grouped.withColumn(
        "GlobalWinRate",
        F.sum("Wins").over(window_by_gametype) / F.sum("UsedNum").over(window_by_gametype)
    )

    df_filtered = df_grouped.filter(
        (F.col("UsedNum") >= min_games) &
        (F.col("WinRate") > F.col("GlobalWinRate"))
    )

    df_pivot = (
        df_filtered
        .groupBy("GameType")
        .agg(
            F.collect_list(
                F.struct(
                    "OpeningName",
                    "UsedNum",
                    "WinRate",
                    "GlobalWinRate"
                )
            ).alias("TopOpenings")
        )
    )

    return df_pivot


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Gold-Openings-Above-Average").getOrCreate()
    quiet_logs(spark)

    paths = [
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=28",
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=27",
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=26",
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=25",
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=24"
    ]

    df_silver = spark.read.parquet(*paths)

    df_trend = top_openings_by_gametype(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "openings_above_average").save()


    spark.stop()