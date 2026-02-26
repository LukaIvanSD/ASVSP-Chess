import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


from pyspark.sql import functions as F
from pyspark.sql.window import Window

def compute_opening_popularity_last5(df_silver):

    df_daily = (
        df_silver
        .groupBy("GameDate", "OpeningName")
        .agg(
            F.count("*").alias("UsedNum"),
            F.sum("WhiteWin").alias("WhiteWin"),
            F.sum("BlackWin").alias("BlackWin"),
            F.sum("Draw").alias("Draw")
        )
    )

    window_day = Window.partitionBy("GameDate").orderBy(F.desc("UsedNum"))
    df_ranked = df_daily.withColumn("Rank", F.row_number().over(window_day))

    df_top_openings = df_ranked.filter(F.col("Rank") == 1).select("OpeningName").distinct()

    days = [row["GameDate"] for row in df_silver.select("GameDate").distinct().collect()]
    df_days = spark.createDataFrame([(d,) for d in days], ["GameDate"])
    df_all = df_top_openings.crossJoin(df_days)

    df_full = (
        df_all
        .join(df_daily, on=["GameDate", "OpeningName"], how="left")
        .fillna({"UsedNum": 0, "WhiteWin": 0, "BlackWin": 0, "Draw": 0})
    )

    df_trend = (
        df_full
        .groupBy("OpeningName")
        .agg(
            F.collect_list(
                F.struct("GameDate", "UsedNum", "WhiteWin", "BlackWin", "Draw")
            ).alias("Trend")
        )
    )

    return df_trend


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Gold-Opening-Popularity-Last5Days").getOrCreate()
    quiet_logs(spark)

    paths = [
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=28",
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=27",
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=26",
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=25",
        f"{silver_path}/day_partition/Year=2025/Month=4/Day=24"
    ]

    df_silver = spark.read.parquet(*paths)

    df_trend = compute_opening_popularity_last5(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "opening_popularity_trend").save()


    spark.stop()