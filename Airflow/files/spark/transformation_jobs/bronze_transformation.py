import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime
import os
def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

import re
def parse_eval(eval_str,eval_before):
    if eval_str is None:
        return None, None  # eval, mateIn
    eval_str = eval_str.strip()
    if eval_str.startswith("#"):
        try:
            mate_in = int(eval_str[1:])
            if mate_in > 0:
                return eval_before + 10, mate_in
            else:
                return eval_before - 10, mate_in
        except:
            return None, None
    else:
        try:
            return float(eval_str), None
        except:
            return None, None
def parse_game_type(event_str):
    if not event_str:
        return "Other"
    event_str = event_str.lower()
    if "blitz" in event_str:
        return "Blitz"
    elif "bullet" in event_str:
        return "Bullet"
    elif "rapid" in event_str:
        return "Rapid"
    elif "classical" in event_str:
        return "Classical"
    else:
        return "Other"
def parse_move_type(move_str):
    """Vrati tip poteza: normal, castling, en_passant, promotion"""
    if not move_str:
        return None

    if move_str in ["O-O"]:
        return "small_castling"

    if move_str in ["O-O-O"]:
        return "big_castling"

    if "=" in move_str:  # npr. e8=Q
        return "promotion"

    return "normal"
def parse_figure(move_str):
    """Vrati puno ime figure iz poteza"""
    if not move_str:
        return None
    piece = move_str[0]
    if piece.isupper():
        mapping = {
            "K": "King",
            "Q": "Queen",
            "R": "Rook",
            "B": "Bishop",
            "N": "Knight"
        }
        return mapping.get(piece, "Unknown")
    else:
        return "Pawn"
def parse_is_rated(event_str):
    """Vrati True ako je igra rated, False ako nije"""
    if not event_str:
        return False
    return "rated" in event_str.lower()

def parse_timestamp(date_time_str):
    if not date_time_str:
        return None
    try:
        dt = datetime.strptime(date_time_str, "%Y.%m.%d %H:%M:%S")
        return int(dt.timestamp())
    except:
        return None
def parse_datetime(date_str, time_str):
    if not date_str or not time_str:
        return None
    try:
        return datetime.strptime(date_str + " " + time_str, "%Y.%m.%d %H:%M:%S")
    except:
        return None
def parse_clock(clock_str):
    """Konvertuje sat iz formata HH:MM:SS ili MM:SS u sekunde"""
    if not clock_str:
        return None
    parts = [int(p) for p in clock_str.split(":")]
    if len(parts) == 3:
        return parts[0]*3600 + parts[1]*60 + parts[2]
    elif len(parts) == 2:
        return parts[0]*60 + parts[1]
    return None
def parse_dest_square(move_str):
    if not move_str:
        return None

    move_str = move_str.strip()

    if move_str in ["O-O", "O-O-O"]:
        return move_str

    move_str = move_str.replace("+", "").replace("#", "")

    if "x" in move_str:
        parts = move_str.split("x")
        return parts[-1]

    if len(move_str) >= 2:
        return move_str[-2:]

    return None

def parse_pgn(game):
    meta = {}
    moves = []

    # Regex za meta linije: [Key "Value"]
    meta_pattern = re.compile(r'\[(\w+)\s+"(.*)"\]')

    # Parsiranje meta linija
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

    # Parsiranje poteza
    moves_text = " ".join(line.strip() for line in game_lines[move_start_index:])
    move_pattern = re.compile(
        r'(\d+)\.\s*'                          # Move number
        r'([^\s\{\!\?]+)[\!\?]*\s*'            # White move
        r'(?:\{\s*(?:\[%eval\s*([#\d\.-]+)\])?\s*(?:\[%clk\s*([\d:]+)\])?\s*\})?\s*'  # Optional eval/clock
        r'(?:\d+\.\.\.\s*)?'                   # Optional black move prefix
        r'([^\s\{\!\?]+)?[\!\?]*\s*'           # Black move
        r'(?:\{\s*(?:\[%eval\s*([#\d\.-]+)\])?\s*(?:\[%clk\s*([\d:]+)\])?\s*\})?'  # Optional eval/clock
    )
    eval_before=None
    for m in move_pattern.finditer(moves_text):
        white_eval, white_mate = parse_eval(m.group(3),eval_before)
        eval_before=white_eval
        black_eval, black_mate = parse_eval(m.group(6) if m.group(6) else None,eval_before)
        eval_before=black_eval
        white_move = m.group(2)
        black_move = m.group(5)

        move_entry = {
            "MoveNumber": int(m.group(1)),
            "White": {
                "Move": white_move,
                "Eval": white_eval,
                "Clock": parse_clock(m.group(4)),
                "MoveType": parse_move_type(white_move),
                "Figure": parse_figure(white_move),
                "DestSquare": parse_dest_square(white_move),
                "MateIn": white_mate,
                "IsCapture": "x" in white_move,
                "IsCheck": "+" in white_move,
                "IsCheckmate": "#" in white_move
            },
            "Black": None
        }

        if black_move and black_move not in ["1-0", "0-1", "1/2-1/2"]:
            move_entry["Black"] = {
                "Move": black_move,
                "Eval": black_eval,
                "Clock": parse_clock(m.group(7)),
                "MoveType": parse_move_type(black_move),
                "Figure": parse_figure(black_move),
                "DestSquare": parse_dest_square(black_move),
                "MateIn": black_mate,
                "IsCapture": "x" in black_move,
                "IsCheck": "+" in black_move,
                "IsCheckmate": "#" in black_move
            }

        moves.append(move_entry)


    result_raw = meta.get("Result")
    if result_raw == "1-0":
        result_str = "White Won"
        winner_name = meta.get("White")
        winner_color = "White"
    elif result_raw == "0-1":
        result_str = "Black Won"
        winner_name = meta.get("Black")
        winner_color = "Black"
    elif result_raw == "1/2-1/2":
        result_str = "Draw"
        winner_name = None
        winner_color = None
    else:
        result_str = result_raw
        winner_name = None
        winner_color = None

    result = {
        "WinnerName": winner_name,
        "Color": winner_color,
        "Result": result_str,
        "ResultType": meta.get("Termination")
    }

    date_time = parse_datetime(meta.get("UTCDate"), meta.get("UTCTime"))
    tc = meta.get("TimeControl", "0+0")
    parts = tc.split("+")
    try:
        initial = int(parts[0]) if parts[0].isdigit() else 0
        increment = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    except:
        initial, increment = 0, 0

    return {
        "GameID": meta.get("Site", "").split("/")[-1],
        "IsRated": parse_is_rated(meta.get("Event")),
        "GameType": parse_game_type(meta.get("Event")),
        "Date": {
            "DateTime": date_time,
            "Timestamp": parse_timestamp(date_time.strftime("%Y.%m.%d %H:%M:%S")) if date_time else None
        },
        "WhitePlayer": {"Name": meta.get("White"), "Rating": int(meta["WhiteElo"]) if meta.get("WhiteElo","").isdigit() else None},
        "BlackPlayer": {"Name": meta.get("Black"), "Rating": int(meta["BlackElo"]) if meta.get("BlackElo","").isdigit() else None},
        "Result": result,
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
        StructField("GameID", StringType()),
        StructField("IsRated", BooleanType()),
        StructField("GameType", StringType()),
        StructField("Date", StructType([
            StructField("DateTime", TimestampType()),
            StructField("Timestamp", IntegerType())
        ])),
        StructField("WhitePlayer", StructType([
            StructField("Name", StringType()),
            StructField("Rating", IntegerType())
        ])),
        StructField("BlackPlayer", StructType([
            StructField("Name", StringType()),
            StructField("Rating", IntegerType())
        ])),
        StructField("Result", StructType([
            StructField("WinnerName", StringType()),
            StructField("Color", StringType()),
            StructField("Result", StringType()),
            StructField("ResultType", StringType())
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
                StructField("Eval", FloatType()),
                StructField("Clock", IntegerType()),
                StructField(
                    "MoveType",
                    StringType(),
                    nullable=False,
                    metadata={"enum": ["normal", "castling", "en_passant", "promotion"]}
                ),
                StructField("Figure", StringType()),
                StructField("DestSquare", StringType()),
                StructField("MateIn", IntegerType(), nullable=True),
                StructField("IsCapture", BooleanType(), nullable=False),
                StructField("IsCheck", BooleanType(), nullable=False),
                StructField("IsCheckmate", BooleanType(), nullable=False)
            ])),
            StructField("Black", StructType([
                StructField("Move", StringType()),
                StructField("Eval", FloatType()),
                StructField("Clock", IntegerType()),
                StructField(
                    "MoveType",
                    StringType(),
                    nullable=False,
                    metadata={"enum": ["normal", "castling", "en_passant", "promotion"]}
                ),
                StructField("Figure", StringType()),
                StructField("DestSquare", StringType()),
                StructField("MateIn", IntegerType(), nullable=True),
                StructField("IsCapture", BooleanType(), nullable=False),
                StructField("IsCheck", BooleanType(), nullable=False),
                StructField("IsCheckmate", BooleanType(), nullable=False)
            ]),nullable=True)
        ])))
    ])

    lines = spark.read.text(input_file_path).rdd.map(lambda r: r.value).filter(lambda line: line.strip() != "")
    games_rdd = lines.mapPartitions(split_games)
    parsed_rdd = games_rdd.map(parse_pgn)
    df = spark.createDataFrame(parsed_rdd, schema)
    df.write.mode("overwrite") \
        .parquet(f"{output_transformed_dir}/bronze_transformed_data")

    df.show(5)
    spark.stop()