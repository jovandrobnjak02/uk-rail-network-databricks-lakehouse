import gzip, json, time, threading
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
        for msg_id, _ in batch:
            conn.ack(msg_id, subscription_id)
    except Exception as e:
        print(f"Failed to upload batch {first}-{last}: {e}")
        with lock:
            buffer[:0] = batch  # put the failed batch back into the buffer

def main(*args, **kwargs):

    volume_path = kwargs["volume_path"]
    flush_time = kwargs.get("flush_time", 60)
    stom_host = kwargs.get("stom_host", "darwin-dist-44ae45.nationalrail.co.uk")
    darwin_topic = kwargs.get("darwin_topic", "/topic/darwin.pushport-v16")

    dbutils.fs.mkdirs(volume_path)

    conn = stomp.Connection([(stom_host, 61613)], auto_decode=False)
    conn.set_listener('darwin-topic-connection', DarwinTopicConnection())

    username = dbutils.secrets.get(scope = "darwin-topic-creds", key = "username")
    password = dbutils.secrets.get(scope = "darwin-topic-creds", key = "password")

    conn.connect(username, password, wait=True)
    print("Listening for messages...")
    conn.subscribe(destination=darwin_topic, id=1, ack='client-individual')    


    while conn.is_connected():
        flush(conn, volume_path, 1)
        time.sleep(flush_time)

    raise RuntimeError("Lost connection to Darwin broker")

if __name__ == "__main__":
    main()