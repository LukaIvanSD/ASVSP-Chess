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

def parse_pgn(game):
    meta = {}
    moves = []

    # Regex za meta linije: [Key "Value"]
    meta_pattern = re.compile(r'\[(\w+)\s+"(.*)"\]')

    # Prvo parsiramo sve meta linije
    move_start_index = 0
    game_lines = game.split("\n")
    for idx, line in enumerate(game_lines):
        if line.startswith("1."):
            move_start_index = idx
            break
        if line.startswith("["):
            m = meta_pattern.match(line)
            if m:
                meta[m.group(1)] = m.group(2)
    # Sada parsiramo poteze
    moves_text = " ".join(line.strip() for line in game_lines[move_start_index:])

    move_pattern = re.compile(
        r'(\d+)\.\s*'                       # 1. Broj poteza
        r'([^\s\{\!\?]+)[\!\?]*\s*'         # 2. Beli potez
        r'\{\s*\[%eval\s*([#\d\.-]+)\]'     # 3. Eval belog (OBAVEZAN)
        r'(?:\s*\[%clk\s*([\d:]+)\])?'      # 4. Sat belog (OPCIONI)
        r'\s*\}\s*'                         # Zatvorena zagrada belog
        r'(?:\d+\.\.\.\s*)?'                # Ignorise 1...
        r'([^\s\{\!\?]+)?[\!\?]*\s*'        # 5. Crni potez (OPCIONI kod kraja partije)
        r'(?:\{\s*\[%eval\s*([#\d\.-]+)\]'  # 6. Eval crnog
        r'(?:\s*\[%clk\s*([\d:]+)\])?'      # 7. Sat crnog
        r'\s*\}\s*)?'                       # Kraj bloka za crnog
    )

    for m in move_pattern.finditer(moves_text):
        try:
            moves.append({
                "MoveNumber": int(m.group(1)),
                "White": {
                    "Move": m.group(2),
                    "Eval": m.group(3),
                    "Clock": m.group(4)
                },
                "Black": {
                    "Move": m.group(5),
                    "Eval": m.group(6),
                    "Clock": m.group(7)
                }
            })
        except:
            continue # Preskoci lose formatiran potez

    tc = meta.get("TimeControl", "0+0")
    parts = tc.split("+")
    try:
        initial = int(parts[0]) if parts[0].isdigit() else 0
        increment = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    except:
        initial, increment = 0, 0

    return {
        "Event": meta.get("Event"),
        "Date": {"Date": meta.get("UTCDate"), "Time": meta.get("UTCTime")},
        "WhitePlayer": {"Name": meta.get("White"), "Rating": int(meta["WhiteElo"]) if meta.get("WhiteElo", "").isdigit() else None},
        "BlackPlayer": {"Name": meta.get("Black"), "Rating": int(meta["BlackElo"]) if meta.get("BlackElo", "").isdigit() else None},
        "Winner": {"Name": meta.get("Result"), "Termination": meta.get("Termination")},
        "Opening": {"Name": meta.get("Opening"), "ECO": meta.get("ECO")},
        "TimeControl": {"Initial": initial, "Increment": increment},
        "Moves": moves
    }


def split_games(lines_iter):
    current_game = []

    for line in lines_iter:
        if line.startswith("[Event "):
            if current_game:
                yield "\n".join(current_game)
                current_game = []
        current_game.append(line)

    if current_game:
        yield "\n".join(current_game)



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit script.py <input> <output>")
        sys.exit(1)

    input_file_path = sys.argv[1]
    output_transformed_dir = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_initial_data").getOrCreate()
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

    lines = spark.read.text(input_file_path).rdd.map(lambda r: r.value).filter(lambda line: line.strip() != "")
    games_rdd = lines.mapPartitions(split_games)
    parsed_rdd = games_rdd.map(parse_pgn)
    df = spark.createDataFrame(parsed_rdd, schema)
    df_partitioned = df.withColumn("partition_date", F.col("Date.Date"))
    df_partitioned.write.mode("overwrite") \
        .partitionBy("partition_date") \
        .parquet(f"{output_transformed_dir}/initial_transformed_data")

    df.show(5)
    spark.stop()