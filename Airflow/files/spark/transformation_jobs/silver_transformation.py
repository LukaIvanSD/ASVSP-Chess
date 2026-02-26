import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def transform_bronze_to_silver(df):
    opening = df.F.col("Opening")


    df_transformed = (
        df
        .withColumn("GameDate", F.to_date(F.col("Date.DateTime")))
        .withColumn("Year", F.year("GameDate"))
        .withColumn("Month", F.month("GameDate"))
        .withColumn("Day", F.dayofmonth("GameDate"))

        .withColumn("OpeningName", opening.Name)
        .withColumn("ECO", opening.ECO)

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
            "Opening",
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

    df_silver = transform_bronze_to_silver(df)

    (
        df_silver
        .write
        .mode("overwrite")
        .partitionBy("Year", "Month", "Day")
        .parquet(output_silver_dir)
    )
    df_silver.show(5, truncate=False)

    spark.stop()