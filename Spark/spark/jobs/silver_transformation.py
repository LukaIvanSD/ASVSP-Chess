import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window as Window



def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def transform_bronze_to_silver_day_partition(df):

    df_transformed = (
        df
        .withColumn("GameDate", F.to_date(F.col("Date.DateTime")))
        .withColumn("Year", F.year("GameDate"))
        .withColumn("Month", F.month("GameDate"))
        .withColumn("Day", F.dayofmonth("GameDate"))

        .withColumn("OpeningName", F.col("Opening.Name"))
        .withColumn("ECO", F.col("Opening.ECO"))

        .withColumn("Rated", F.when(F.col("IsRated") == True, 1).otherwise(0))

        .withColumn(
            "AvgElo",
            (F.col("WhitePlayer.Rating") + F.col("BlackPlayer.Rating")) / 2
        )

        .withColumn(
            "EloCategory",
            F.when((F.col("AvgElo") >= 2200) & (F.col("AvgElo") < 2300), "CandidateMaster")
            .when((F.col("AvgElo") >= 2300) & (F.col("AvgElo") < 2400), "FideMaster")
            .when((F.col("AvgElo") >= 2400) & (F.col("AvgElo") < 2500), "IM_Level")
            .when((F.col("AvgElo") >= 2500) & (F.col("AvgElo") < 2600), "GM_Level")
            .when((F.col("AvgElo") >= 2600) & (F.col("AvgElo") < 2700), "Elite_GM")
            .when(F.col("AvgElo") >= 2700, "Super_GM")
            .otherwise("Low_Elo")
        )

        .withColumn(
            "WhiteWin",
            F.when(F.col("Result.Result") == "White Won", 1).otherwise(0)
        )
        .withColumn(
            "BlackWin",
            F.when(F.col("Result.Result") == "Black Won", 1).otherwise(0)
        )
        .withColumn(
            "Draw",
            F.when(F.col("Result.Result") == "Draw", 1).otherwise(0)
        )

        .select(
            "GameDate",
            "Year",
            "Month",
            "Day",
            "OpeningName",
            "ECO",
            "GameType",
            "Rated",
            "EloCategory",
            "WhiteWin",
            "BlackWin",
            "Draw"
        )
    )

    return df_transformed

def compute_eval_stats(white_moves, black_moves):
    all_moves = white_moves.unionByName(black_moves)

    w = Window.partitionBy("GameID").orderBy("PlayerMoveIndex")

    all_moves = all_moves.withColumn("PrevEval", F.lag("Eval").over(w))

    all_moves = all_moves.withColumn(
        "EvalDiff",
        F.when(
            F.col("PrevEval").isNull(),
            F.lit(0.0)
        ).otherwise(
            F.when(
                F.signum(F.col("Eval")) == F.signum(F.col("PrevEval")),
                F.when(F.col("Color") == "White", F.col("Eval") - F.col("PrevEval"))
                .when(F.col("Color") == "Black", F.col("PrevEval") - F.col("Eval"))
            ).otherwise(
                F.abs(F.col("Eval")) + F.abs(F.col("PrevEval"))
            )
        )
    )
    all_moves = all_moves.withColumn(
        "LeadChange",
        F.when(F.signum("Eval") != F.signum("PrevEval"), 1).otherwise(0)
    )

    eval_stats = all_moves.groupBy("GameID").agg(
        F.sum("LeadChange").alias("LeadChanges"),
        F.max("Eval").alias("MaxWhiteAdvantage"),
        F.min("Eval").alias("MaxBlackAdvantage"),
        F.avg("EvalDiff").alias("AvgEvalSwing"),
        F.max("EvalDiff").alias("MaxEvalSwing"),
    )
    return eval_stats,all_moves


def compute_figure_stats(all_moves_with_eval_lag):

    moves_with_type = all_moves_with_eval_lag.withColumn(
        "MoveAccuracyType",
        F.when(F.col("EvalDiff") >= 10, "Best")
         .when(F.col("EvalDiff") >= 3, "Great")
         .when(F.col("EvalDiff") >= 0.1, "Good")
         .when(F.col("EvalDiff") >= -0.1, "Inaccuracy")
         .when(F.col("EvalDiff") >= -2, "Mistake")
         .otherwise("Blunder")
    )
    white_moves = moves_with_type.filter(F.col("Color")=="White") \
        .select(
            "GameID",
            "MoveNumber",
            "Move",
            "MoveType",
            F.col("Figure"),
            F.col("IsCapture"),
            F.col("IsCheck"),
            F.col("MoveAccuracyType")
        )
    black_moves = moves_with_type.filter(F.col("Color")=="Black") \
        .select(
            "GameID",
            "MoveNumber",
            "Move",
            "MoveType",
            F.col("Figure"),
            F.col("IsCapture"),
            F.col("IsCheck"),
            F.col("MoveAccuracyType")
        )

    def agg_figure_stats(df, color_name):
        stats = df.groupBy("GameID", "Figure").agg(
            F.count("*").alias("Moves"),
            F.sum(F.when(F.col("MoveAccuracyType")=="Best",1).otherwise(0)).alias("Best"),
            F.sum(F.when(F.col("MoveAccuracyType")=="Great",1).otherwise(0)).alias("Great"),
            F.sum(F.when(F.col("MoveAccuracyType")=="Good",1).otherwise(0)).alias("Good"),
            F.sum(F.when(F.col("MoveAccuracyType")=="Inaccuracy",1).otherwise(0)).alias("Inaccuracy"),
            F.sum(F.when(F.col("MoveAccuracyType")=="Mistake",1).otherwise(0)).alias("Mistake"),
            F.sum(F.when(F.col("MoveAccuracyType")=="Blunder",1).otherwise(0)).alias("Blunder"),
            F.sum(F.col("IsCapture").cast("int")).alias("Captures"),
            F.sum(F.col("IsCheck").cast("int")).alias("Checks")
        )

        stats_struct = stats.groupBy("GameID").agg(
            F.collect_list(
                F.struct(
                    "Figure",
                    "Moves",
                    "Best",
                    "Great",
                    "Good",
                    "Inaccuracy",
                    "Mistake",
                    "Blunder",
                    "Captures",
                    "Checks"
                )
            ).alias(f"{color_name}FigureStats")
        )
        return stats_struct

    white_stats = agg_figure_stats(white_moves, "White")
    black_stats = agg_figure_stats(black_moves, "Black")
    figure_stats = white_stats.join(black_stats, "GameID", "outer")

    return figure_stats

def compute_game_level(df):

    df_level = df.withColumn(
        "AvgElo",
        (F.col("WhiteRating") + F.col("BlackRating")) / 2
    ).withColumn(
        "GameLevel",
        F.when((F.col("AvgElo") >= 2200) & (F.col("AvgElo") < 2300), "CandidateMaster")
        .when((F.col("AvgElo") >= 2300) & (F.col("AvgElo") < 2400), "FideMaster")
        .when((F.col("AvgElo") >= 2400) & (F.col("AvgElo") < 2500), "IM_Level")
        .when((F.col("AvgElo") >= 2500) & (F.col("AvgElo") < 2600), "GM_Level")
        .when((F.col("AvgElo") >= 2600) & (F.col("AvgElo") < 2700), "Elite_GM")
        .when(F.col("AvgElo") >= 2700, "Super_GM")
        .otherwise("Low_Elo")
    )

    return df_level.select(
        "GameID",
        F.struct(
            "GameLevel"
        ).alias("GameLevelInfo")
    )
def compute_time_stats(all_moves_with_eval_lag):
    w = Window.partitionBy("GameID", "Color").orderBy("PlayerMoveIndex")
    moves = all_moves_with_eval_lag.withColumn(
        "PrevClock",
        F.lag("Clock").over(w)
    )
    moves = moves.withColumn(
        "TimeSpent",
        F.when(F.col("PrevClock").isNull(), F.col("Clock"))
         .otherwise(F.col("PrevClock") - F.col("Clock"))
    )
    time_agg = moves.groupBy("GameID", "Color").agg(
        F.sum("TimeSpent").alias("TimeSpent")
    )
    time_pivot = time_agg.groupBy("GameID").pivot("Color", ["White", "Black"]).sum("TimeSpent")
    time_stats = time_pivot.withColumnRenamed("White", "WhiteTimePlayed") \
                           .withColumnRenamed("Black", "BlackTimePlayed") \
                           .withColumn("TotalTimePlayed", F.col("WhiteTimePlayed") + F.col("BlackTimePlayed"))

    return time_stats
def transform_bronze_to_silver_game_type_partition(df):

    base = (
            df.withColumn("TotalMoves", F.size("Moves"))
            .select(
                "GameID",
                F.struct(
                    F.col("Opening.ECO").alias("ECO"),
                    F.col("Opening.Name").alias("OpeningName")
                ).alias("Opening"),
                F.struct(
                    F.col("WhitePlayer.Name").alias("WhitePlayerName"),
                    F.col("BlackPlayer.Name").alias("BlackPlayerName")
                ).alias("Players"),
                "GameType",
                "TotalMoves",
                F.struct(
                    F.col("Result.Result").alias("Result"),
                    F.col("Result.ResultType").alias("ResultType"),
                    F.col("Result.WhiteDiff").alias("WhiteDiff"),
                    F.col("Result.BlackDiff").alias("BlackDiff")
                ).alias("Result"),
                F.col("Date.DateTime").alias("DateTime"),
                F.col("Date.Timestamp").alias("Timestamp"),
                "Moves",
                "TimeControl",
                F.col("IsRated").alias("IsRated"),
                F.col("WhitePlayer.Rating").alias("WhiteRating"),
                F.col("BlackPlayer.Rating").alias("BlackRating")
            )
        )
    moves_df = base.select(
        "GameID",
        F.posexplode("Moves").alias("MoveIndex", "Move")
        )
    white_moves = moves_df.select(
        "GameID",
        (F.col("MoveIndex")*2).alias("PlayerMoveIndex"),
        F.col("Move.MoveNumber").alias("MoveNumber"),
        F.col("Move.White.*")
    ).withColumn("Color", F.lit("White"))
    black_moves = moves_df.select(
        "GameID",
        (F.col("MoveIndex")*2 + 1).alias("PlayerMoveIndex"),
        F.col("Move.MoveNumber").alias("MoveNumber"),
        F.col("Move.Black.*")
    ).withColumn("Color", F.lit("Black"))
    eval_stats,all_moves_with_eval_lag = compute_eval_stats(white_moves, black_moves)
    figure_stats = compute_figure_stats(all_moves_with_eval_lag)
    time_stats = compute_time_stats(all_moves_with_eval_lag)
    game_level = compute_game_level(base)

    final_df = base \
    .drop("Moves", "TimeControl","WhiteRating","BlackRating") \
    .join(eval_stats, "GameID", "left") \
    .join(time_stats, "GameID", "left") \
    .join(figure_stats, "GameID", "left") \
    .join(game_level, "GameID", "left") \
    .withColumn(
        "EvalStats",
        F.struct(
            "LeadChanges",
            "MaxWhiteAdvantage",
            "MaxBlackAdvantage",
            "AvgEvalSwing",
            "MaxEvalSwing"
        )
    ).withColumn(
        "TimePlayed",
        F.struct(
            "TotalTimePlayed",
            "WhiteTimePlayed",
            "BlackTimePlayed"
        )
    ).withColumnRenamed("WhiteFigureStats", "FigureStats.WhiteFigureStats") \
    .withColumnRenamed("BlackFigureStats", "FigureStats.BlackFigureStats") \
    .withColumnRenamed("GameLevelInfo", "GameLevel.GameLevel")

    final_df = final_df.drop("WhiteTimePlayed", "BlackTimePlayed", "TotalTimePlayed")

    final_df = final_df.drop("LeadChanges", "MaxWhiteAdvantage", "MaxBlackAdvantage", "AvgEvalSwing", "MaxEvalSwing")

    return final_df


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: spark-submit script.py <input> <output>")
        sys.exit(1)

    bronze_path = sys.argv[1]
    output_silver_dir = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_silver_data").getOrCreate()
    quiet_logs(spark)

    df = spark.read.parquet(bronze_path)
    df.show(5, truncate=False)
    df.printSchema()

    df_silver_day_partition = transform_bronze_to_silver_day_partition(df)

    (
        df_silver_day_partition
        .write
        .mode("overwrite")
        .partitionBy("Year", "Month", "Day")
        .parquet(output_silver_dir + "/day_partition")
    )
    df_silver_day_partition.show(3, truncate=False)
    df_silver_day_partition.printSchema()

    df_silver_game_type_partition = transform_bronze_to_silver_game_type_partition(df)

    df_silver_game_type_partition.show(3, truncate=False)
    df_silver_game_type_partition.printSchema()
    (
        df_silver_game_type_partition
        .write
        .mode("overwrite")
        .partitionBy("GameType")
        .parquet(output_silver_dir+"/game_type_partition")
    )


    spark.stop()