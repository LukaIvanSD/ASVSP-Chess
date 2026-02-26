import http.client
import json
import re
from kafka import KafkaProducer
from datetime import datetime
import time
from os

conn = http.client.HTTPSConnection("lichess.org")
token = os.getenv("LICHESS_API_TOKEN")
headers = { 'Authorization': f"Bearer {token}" }

users = ['AAlmeidaTX','Chesssknock', 'Odirovski', 'novik70', 'Fleetwood_Mac', 'Revolverman', 'MassterofMayhem', 'morus22', 'JoeAssaad', 'imdejong']


output_path_finished_games = "/opt/airflow/files/chessData/realTimeData/all_games.pgn"

output_path_ongoing_games = "/opt/airflow/files/chessData/realTimeData/ongoing_games.pgn"

output_path_late_moves = "/opt/airflow/files/chessData/realTimeData/late_moves_audit.json"

finishedProducer = KafkaProducer(
    bootstrap_servers=['kafka-broker1-1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
ongoingProducer = KafkaProducer(
    bootstrap_servers=['kafka-broker1-1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_finished = 'chess_finished_games'

topic_ongoing = 'chess_ongoing_games'

with open(output_path_finished_games, "ab") as out_file:
    for username in users:
        conn.request(
            "GET",
            f"/api/games/user/{username}?max=30&rated=true&clocks=true&pgnInJson=true",
            headers={
                **headers,
                "Accept": "application/x-ndjson"
            }
        )

        res = conn.getresponse()
        data = res.read()
        data_str = data.decode()

        if "error" in data_str:
            continue

        out_file.write(b"\n\n")
        out_file.write(f"% Games for user: {username}\n".encode())
        out_file.write(data)

        for line in data_str.strip().splitlines():
            if line.strip() == "":
                continue
            obj = json.loads(line)
            finishedProducer.send(topic_finished, value=obj)

finishedProducer.flush()


pattern = r'\[Site\s+"https://lichess\.org/([A-Za-z0-9]+)"\]'
topic_ongoing = "chess_ongoing_games"

conn = http.client.HTTPSConnection("lichess.org")

try:
    with open(output_path_ongoing_games, "ab") as out_ongoing_file, \
         open(output_path_late_moves, "ab") as out_late_file:
        out_ongoing_file.write(b"\n\n")
        out_ongoing_file.write(f"% Ongoing games for user: AAlmeidaTX\n".encode())
        out_late_file.write(f"\n% Late moves audit start: {datetime.utcnow().isoformat()}\n".encode())
        for username in users:
            if username == 'AAlmeidaTX':
                continue

            print(f"Proveravam partiju protiv: {username}...")

            try:
                conn.request(
                    "GET",
                    f"/api/games/user/AAlmeidaTX?max=1&rated=true&clocks=true&vs={username}",
                    headers=headers
                )
                res = conn.getresponse()

                if res.status == 429:
                    print("Rate limit dosegnut! Spavam 60 sekundi...")
                    time.sleep(60)
                    continue

                data = res.read()
                res.close()

                match = re.search(pattern, data.decode())
                if not match:
                    print(f"Nema aktivne partije sa {username}")
                    continue

                game_id = match.group(1)

                stream_conn = http.client.HTTPSConnection("lichess.org")
                try:
                    stream_conn.request(
                        "GET",
                        f"/api/stream/game/{game_id}",
                        headers=headers
                    )
                    stream_res = stream_conn.getresponse()

                    move_counter = 0
                    first_move_delayed = None
                    first_move_late = None
                    for raw_line in stream_res:
                        if not raw_line or raw_line == b"\n":
                            continue

                        try:
                            event = json.loads(raw_line.decode())


                            if "fen" in event and "id" not in event:
                                event["game_id"] = game_id
                                move_counter += 1
                                if move_counter == 1 and first_move_delayed is None:
                                    event["timestamp"] = datetime.utcnow().isoformat() + "Z"
                                    first_move_delayed = event
                                    first_move_late = event
                                    continue


                            event["timestamp"] = datetime.utcnow().isoformat() + "Z"
                            json_data = json.dumps(event)
                            out_ongoing_file.write(json_data.encode() + b"\n")
                            out_ongoing_file.write(b"\n")

                            ongoingProducer.send(topic_ongoing, value=event)

                            if first_move_delayed and move_counter == 3:
                                ongoingProducer.send(topic_ongoing, value=first_move_delayed)
                                out_ongoing_file.write(json.dumps(first_move_delayed).encode() + b"\n")
                                out_ongoing_file.write(b"\n")
                                first_move_delayed = None

                            if move_counter == 50 and first_move_late:
                                ongoingProducer.send(topic_ongoing, value=first_move_late)

                                log_entry = {
                                    "game_id": game_id,
                                    "event": first_move_late,
                                    "sent_at_processing_time": datetime.utcnow().isoformat()
                                }
                                out_late_file.write(json.dumps(log_entry).encode() + b"\n")
                                out_late_file.flush()
                                first_move_late = None



                            print(f"Potez poslat za igru {game_id}")

                        except json.JSONDecodeError:
                            continue

                        time.sleep(0.5)

                    stream_res.close()
                finally:
                    stream_conn.close()

            except http.client.RemoteDisconnected:
                print("Server je prekinuo vezu. Ponovni pokušaj za 5 sekundi...")
                time.sleep(5)
                conn = http.client.HTTPSConnection("lichess.org")
            except Exception as e:
                print(f"Greška kod korisnika {username}: {e}")
            time.sleep(1)

finally:
    conn.close()
    ongoingProducer.flush()
