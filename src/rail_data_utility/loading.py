import argparse, gzip, json, time, threading
from datetime import datetime, timezone
import stomp
from databricks.sdk.runtime import dbutils

buffer, lock = [], threading.Lock()

class DarwinTopicConnection(stomp.ConnectionListener):
    def on_error(self, frame):
        print('received an error "%s"' % frame.body)

    def on_message(self, frame):
        decompressed_bytes = gzip.decompress(frame.body)
        message_text = decompressed_bytes.decode('utf-8')
        rec = {
            "sequence_number": int(frame.headers["SequenceNumber"]),
            "message_type": frame.headers.get("MessageType"),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "message_xml": message_text,
        }
        with lock:
            buffer.append((frame.headers["message-id"], rec))

def upload_to_volume(path, body):
    dbutils.fs.put(path, body)
    print(f"Uploaded to {path}")

def flush(conn, volume_path, subscription_id = 1):
    with lock:
        batch, buffer[:] = buffer[:], []
    if not batch:
        return
    recs = [r for _, r in batch]
    first, last = recs[0]["sequence_number"], recs[-1]["sequence_number"]
    now = datetime.now(timezone.utc)
    name = f"{now:%Y%m%dT%H%M%SZ}_{first}_{last}.jsonl"
    body = "\n".join(json.dumps(r) for r in recs)
    try:
        upload_to_volume(f"{volume_path}/date={now:%Y-%m-%d}/{name}", body)
        if conn.is_connected():
            for msg_id, _ in batch:
                conn.ack(msg_id, subscription_id)
    except Exception as e:
        print(f"Failed to upload batch {first}-{last}: {e}")
        with lock:
            buffer[:0] = batch  # put the failed batch back into the buffer

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume_path", required=True)
    parser.add_argument("--flush_time", type=int, default=60)
    parser.add_argument("--stomp_host", default="darwin-dist-44ae45.nationalrail.co.uk")
    parser.add_argument("--darwin_topic", default="/topic/darwin.pushport-v16")
    args = parser.parse_args()

    volume_path = args.volume_path
    flush_time = args.flush_time
    stomp_host = args.stomp_host
    darwin_topic = args.darwin_topic

    dbutils.fs.mkdirs(volume_path)

    conn = stomp.Connection([(stomp_host, 61613)], auto_decode=False)
    conn.set_listener('darwin-topic-connection', DarwinTopicConnection())

    username = dbutils.secrets.get(scope = "darwin-topic-creds", key = "username")
    password = dbutils.secrets.get(scope = "darwin-topic-creds", key = "password")

    conn.connect(username, password, wait=True)
    print("Listening for messages...")
    conn.subscribe(destination=darwin_topic, id=1, ack='client-individual')    


    while conn.is_connected():
        flush(conn, volume_path, 1)
        time.sleep(flush_time)

    flush(conn, volume_path, 1)
    raise RuntimeError("Lost connection to Darwin broker")

if __name__ == "__main__":
    main()