import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from pyspark.sql.functions import from_json, col, explode



def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def player_figure_analysis(df):
    white_figures = df.select(
        col("Players.WhitePlayerName").alias("Player"),
        explode(col("`FigureStats.WhiteFigureStats`")).alias("Figure")
    )

    black_figures = df.select(
        col("Players.BlackPlayerName").alias("Player"),
        explode(col("`FigureStats.BlackFigureStats`")).alias("Figure")
    )

    all_figures = white_figures.unionByName(black_figures)

    figures_with_score = all_figures.withColumn(
        "Score",
        col("Figure.Best")*5 +
        col("Figure.Great")*3 +
        col("Figure.Good")*2 -
        col("Figure.Inaccuracy")*1 -
        col("Figure.Mistake")*3 -
        col("Figure.Blunder")*5 +
        col("Figure.Captures")*0.5 +
        col("Figure.Checks")*0.5
    )

    total_scores = figures_with_score.groupBy("Player", col("Figure.Figure")).agg(
        F.sum("Score").alias("TotalScore"),
        F.sum("Figure.Moves").alias("TotalMoves"),
        F.sum("Figure.Best").alias("Best"),
        F.sum("Figure.Great").alias("Great"),
        F.sum("Figure.Good").alias("Good"),
        F.sum("Figure.Inaccuracy").alias("Inaccuracy"),
        F.sum("Figure.Mistake").alias("Mistake"),
        F.sum("Figure.Blunder").alias("Blunder"),
        F.sum("Figure.Captures").alias("Captures"),
        F.sum("Figure.Checks").alias("Checks")
    )

    windowDesc = Window.partitionBy("Player").orderBy(F.col("TotalScore").desc())
    windowAsc = Window.partitionBy("Player").orderBy(F.col("TotalScore").asc())

    ranked = total_scores.withColumn("RankDesc", F.row_number().over(windowDesc)) \
                            .withColumn("RankAsc", F.row_number().over(windowAsc))

    best_figures = ranked.filter(F.col("RankDesc") == 1).select(
        "Player",
        F.struct(
            F.col("Figure").alias("Figure"),
            F.col("TotalScore").alias("Score"),
            F.col("TotalMoves").alias("Moves"),
            "Best", "Great", "Good", "Inaccuracy", "Mistake", "Blunder", "Captures", "Checks"
        ).alias("Best")
    )

    worst_figures = ranked.filter(F.col("RankAsc") == 1).select(
        "Player",
        F.struct(
            F.col("Figure").alias("Figure"),
            F.col("TotalScore").alias("Score"),
            F.col("TotalMoves").alias("Moves"),
            "Best", "Great", "Good", "Inaccuracy", "Mistake", "Blunder", "Captures", "Checks"
        ).alias("Worst")
    )

    final_df = best_figures.join(
        worst_figures, on="Player", how="inner"
    )

    return final_df



if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <silver_input> <mongo_uri> <mongo_collection>")
        sys.exit(1)

    silver_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Gold-Player-Figure-Analysis").getOrCreate()
    quiet_logs(spark)

    df_silver = spark.read.parquet(silver_path + "/game_type_partition")

    df_trend = player_figure_analysis(df_silver)

    df_trend.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "player_figure_analysis").save()
    spark.stop()