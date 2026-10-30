import argparse
import base64
import gzip
import json
import logging
import threading
import time
from datetime import datetime, timezone

import stomp

logger = logging.getLogger(__name__)

SUBSCRIPTION_ID = 1

buffer, lock = [], threading.Lock()

def get_dbutils():
    # imported lazily: importing databricks.sdk.runtime authenticates against a workspace,
    # which would make this module unimportable in tests without credentials
    from databricks.sdk.runtime import dbutils
    return dbutils

class DarwinTopicConnection(stomp.ConnectionListener):
    def on_error(self, frame):
        logger.error("Received an error frame: %s", frame.body)

    def on_heartbeat_timeout(self):
        logger.warning("Heartbeat timeout: no data from broker, connection considered dead")

    def on_disconnected(self):
        logger.warning("Disconnected from broker")

    def on_message(self, frame):
        seq = frame.headers.get("SequenceNumber", "")
        rec = {
            "sequence_number": int(seq) if seq.isdigit() else None,
            "message_type": frame.headers.get("MessageType"),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "message_xml": None,
            "error": None,
            "raw_body_b64": None,
        }
        try:
            rec["message_xml"] = gzip.decompress(frame.body).decode('utf-8')
        except Exception as e:  # noqa: BLE001 - must never raise on stomp's receiver thread
            # keep the raw bytes so the message can be inspected later instead of crashing the receiver thread
            rec["error"] = repr(e)
            rec["raw_body_b64"] = base64.b64encode(frame.body).decode('ascii')
            logger.warning("Bad message %s: %r", frame.headers.get('message-id'), e)
        if rec["sequence_number"] is None:
            logger.warning("Message %s has no valid SequenceNumber header: %r", frame.headers.get('message-id'), seq)
        with lock:
            buffer.append((frame.headers["message-id"], rec))

def upload_to_volume(path, body):
    get_dbutils().fs.put(path, body)
    logger.info("Uploaded to %s", path)

def flush(conn, volume_path, subscription_id = SUBSCRIPTION_ID):
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
    except Exception:
        logger.exception("Failed to upload batch %s-%s", first, last)
        with lock:
            buffer[:0] = batch  # put the failed batch back into the buffer

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--volume_path", required=True)
    parser.add_argument("--flush_time", type=int, default=60)
    parser.add_argument("--stomp_host", default="darwin-dist-44ae45.nationalrail.co.uk")
    parser.add_argument("--stomp_port", type=int, default=61613)
    parser.add_argument("--darwin_topic", default="/topic/darwin.pushport-v16")
    parser.add_argument("--client_id", required=True)
    parser.add_argument("--secret_scope", default="darwin-topic-creds")
    args = parser.parse_args()

    volume_path = args.volume_path
    flush_time = args.flush_time
    stomp_host = args.stomp_host
    stomp_port = args.stomp_port
    darwin_topic = args.darwin_topic
    client_id = args.client_id
    secret_scope = args.secret_scope

    dbutils = get_dbutils()
    dbutils.fs.mkdirs(volume_path)

    conn = stomp.Connection([(stomp_host, stomp_port)], auto_decode=False, heartbeats=(10000, 10000))
    conn.set_listener('darwin-topic-connection', DarwinTopicConnection())

    username = dbutils.secrets.get(scope = secret_scope, key = "username")
    password = dbutils.secrets.get(scope = secret_scope, key = "password")

    conn.connect(username, password, wait=True, headers={"client-id": client_id})
    logger.info("Listening for messages on %s as %s", darwin_topic, client_id)
    conn.subscribe(destination=darwin_topic, id=SUBSCRIPTION_ID, ack='client-individual',
                   headers={"activemq.subscriptionName": client_id})


    while conn.is_connected():
        flush(conn, volume_path)
        time.sleep(flush_time)

    flush(conn, volume_path)
    raise RuntimeError("Lost connection to Darwin broker")

if __name__ == "__main__":
    main()
