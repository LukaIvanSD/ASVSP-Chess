import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <input> <output>")
        sys.exit(1)

    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("initial_analytics").getOrCreate()
    quiet_logs(spark)

    schema = StructType([
        StructField("Event", StringType()),
        StructField("Date", StructType([
            StructField("Date", StringType()),
            StructField("Time", StringType())
        ])),
        StructField("WhitePlayer", StructType([
            StructField("Name", StringType()),
            StructField("Rating", IntegerType())
        ])),
        StructField("BlackPlayer", StructType([
            StructField("Name", StringType()),
            StructField("Rating", IntegerType())
        ])),
        StructField("Winner", StructType([
            StructField("Name", StringType()),
            StructField("Termination", StringType())
        ])),
        StructField("Opening", StructType([
            StructField("Name", StringType()),
            StructField("ECO", StringType())
        ])),
        StructField("TimeControl", StructType([
            StructField("Initial", IntegerType()),
            StructField("Increment", IntegerType())
        ])),
        StructField("Moves", ArrayType(StructType([
            StructField("MoveNumber", IntegerType()),
            StructField("White", StructType([
                StructField("Move", StringType()),
                StructField("Eval", StringType()),
                StructField("Clock", StringType())
            ])),
            StructField("Black", StructType([
                StructField("Move", StringType()),
                StructField("Eval", StringType()),
                StructField("Clock", StringType())
            ]))
        ])))
    ])

    df = spark.read.parquet("hdfs://namenode:9000/transformed_data/initial_transformed_data")
    df.printSchema()
    df.show(5)

    usersWithMostGamesAsWhite = df.groupBy("WhitePlayer.Name").count().withColumnRenamed("Name", "PlayerName").withColumnRenamed("count", "GamesPlayed").orderBy(F.desc("GamesPlayed")).limit(15)
    usersWithMostGamesAsWhite.show()

    usersWithMostGamesAsBlack = df.groupBy("BlackPlayer.Name").count().withColumnRenamed("Name", "PlayerName").withColumnRenamed("count", "GamesPlayed").orderBy(F.desc("GamesPlayed")).limit(15)
    usersWithMostGamesAsBlack.show()

    usersWithMosGames= usersWithMostGamesAsWhite.union(usersWithMostGamesAsBlack).groupBy("PlayerName").agg(F.sum("GamesPlayed").alias("TotalGamesPlayed")).orderBy(F.desc("TotalGamesPlayed"))


    usersWithMosGames.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "usersWithMostGames").save()
    spark.stop()