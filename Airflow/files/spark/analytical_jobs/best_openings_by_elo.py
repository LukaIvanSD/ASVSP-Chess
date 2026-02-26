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

def openings_profitability_by_elo(df_silver, min_games=100):
    df_grouped = (
        df_silver
        .groupBy("EloCategory", "OpeningName")
        .agg(
            F.count("*").alias("UsedNum"),
            F.sum("WhiteWin").alias("Wins"),
            F.sum("BlackWin").alias("Losses"),
            F.sum("Draw").alias("Draws")
        )
        .withColumn("WinRate", F.col("Wins") / F.col("UsedNum"))
        .withColumn("LossRate", F.col("Losses") / F.col("UsedNum"))
        .withColumn("DrawRate", F.col("Draws") / F.col("UsedNum"))
        .filter(F.col("UsedNum") >= min_games)
    )

    window_elo_win = Window.partitionBy("EloCategory").orderBy(F.desc("WinRate"))
    window_elo_loss = Window.partitionBy("EloCategory").orderBy(F.desc("LossRate"))
    window_elo_draw = Window.partitionBy("EloCategory").orderBy(F.desc("DrawRate"))

    df_ranked = (
        df_grouped
        .withColumn("WinRank", F.row_number().over(window_elo_win))
        .withColumn("LossRank", F.row_number().over(window_elo_loss))
        .withColumn("DrawRank", F.row_number().over(window_elo_draw))
    )

    df_top_win = df_ranked.filter(F.col("WinRank") == 1).select(
        "EloCategory", "OpeningName", "UsedNum", "WinRate"
    )
    df_top_loss = df_ranked.filter(F.col("LossRank") == 1).select(
        "EloCategory", "OpeningName", "UsedNum", "LossRate"
    )
    df_top_draw = df_ranked.filter(F.col("DrawRank") == 1).select(
        "EloCategory", "OpeningName", "UsedNum", "DrawRate"
    )

    df_result = (
        df_top_win.alias("win")
        .join(df_top_loss.alias("loss"), on="EloCategory")
        .join(df_top_draw.alias("draw"), on="EloCategory")
        .select(
            F.col("win.EloCategory"),
            F.struct(
                F.col("win.OpeningName"),
                F.col("win.WinRate"),
                F.col("win.UsedNum")
            ).alias("TopWinOpening"),
            F.struct(
                F.col("loss.OpeningName"),
                F.col("loss.LossRate"),
                F.col("loss.UsedNum")
            ).alias("TopLossOpening"),
            F.struct(
                F.col("draw.OpeningName"),
                F.col("draw.DrawRate"),
                F.col("draw.UsedNum")
            ).alias("TopDrawOpening")
        )
    )

    return df_result


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Gold-Best-Openings-By-Elo").getOrCreate()
    quiet_logs(spark)

    df_silver = spark.read.parquet(silver_path + "/day_partition")

    df_trend = openings_profitability_by_elo(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "best_openings_by_elo").save()


    spark.stop()