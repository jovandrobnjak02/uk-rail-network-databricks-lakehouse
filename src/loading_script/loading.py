import stomp
import time
from databricks.sdk.runtime import dbutils

class DarwinTopicConnection(stomp.ConnectionListener):
    def on_error(self, frame):
        print('received an error "%s"' % frame.body)

    def on_message(self, frame):
        print('received a message "%s"' % frame.body)


def main(*args, **kwargs):
    conn = stomp.Connection([('darwin-dist-44ae45.nationalrail.co.uk', 61613)])
    conn.set_listener('darwin-topic-connection', DarwinTopicConnection())

    username = dbutils.secrets.get(scope = "darwin-topic-creds", key = "username")
    password = dbutils.secrets.get(scope = "darwin-topic-creds", key = "password")
    conn.connect(username, password, wait=True)

    conn.subscribe(destination='/topic/darwin.pushport-v16', id=1, ack='auto')
    print("Listening for messages...")

    try:
        while True:
            time.sleep(3)
    except KeyboardInterrupt:
        print("\nDisconnecting...")
        conn.disconnect()

if __name__ == "__main__":
    main()