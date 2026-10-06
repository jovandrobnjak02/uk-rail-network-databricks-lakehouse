import stomp
import time
import gzip
from databricks.sdk.runtime import dbutils

class DarwinTopicConnection(stomp.ConnectionListener):
    def on_error(self, frame):
        print('received an error "%s"' % frame.body)

    def on_message(self, frame):
        decompressed_bytes = gzip.decompress(frame.body)
        message_text = decompressed_bytes.decode('utf-8')
        print('received a message "%s"' % message_text)


def main(*args, **kwargs):
    conn = stomp.Connection([('darwin-dist-44ae45.nationalrail.co.uk', 61613)], auto_decode=False)
    conn.set_listener('darwin-topic-connection', DarwinTopicConnection())

    username = dbutils.secrets.get(scope = "darwin-topic-creds", key = "username")
    password = dbutils.secrets.get(scope = "darwin-topic-creds", key = "password")
    conn.connect(username, password, wait=True)
    print("Listening for messages...")
    conn.subscribe(destination='/topic/darwin.pushport-v16', id=1, ack='auto')

    while conn.is_connected():
        time.sleep(3)

    raise RuntimeError("Lost connection to Darwin broker")

if __name__ == "__main__":
    main()