import json

import stomp
import zlib
import io
import time
import logging
import xmltodict
import socket
from dotenv import load_dotenv
import os

load_dotenv()

# Python script susbcribes to rail events on the Darwin PushPort, filters the Train Status events and publishes them
# on a local socket to be consumed by Spark Streaming

logging.basicConfig(format='%(asctime)s %(levelname)s\t%(message)s', level=logging.INFO)

# use env vars
USERNAME = os.environ["DARWIN_USERNAME"]
PASSWORD = os.environ["DARWIN_PASSWORD"]
HOSTNAME = 'darwin-dist-44ae45.nationalrail.co.uk'
HOSTPORT = 61613
# Always prefixed by /topic/ (it's not a queue, it's a topic)
TOPIC = '/topic/darwin.pushport-v16'

CLIENT_ID = socket.getfqdn()
HEARTBEAT_INTERVAL_MS = 15000
RECONNECT_DELAY_SECS = 15

def connect_and_subscribe(connection):
    if stomp.__version__[0] < 5:
        connection.start()

    connect_header = {'client-id': USERNAME + '-' + CLIENT_ID}
    subscribe_header = {'activemq.subscriptionName': CLIENT_ID}

    connection.connect(username=USERNAME,
                       passcode=PASSWORD,
                       wait=True,
                       headers=connect_header)

    connection.subscribe(destination=TOPIC,
                         id='1',
                         ack='auto',
                         headers=subscribe_header)


# Ensure the location entry is always an array. The XML parser sometimes makes it a dictionary if there's only one entry
def normalise_ts_event(ts_event):
    if isinstance(ts_event.get('ns5:Location'), dict):
        # if its a dict, turn it into an array
        ts_event['ns5:Location'] = [ts_event['ns5:Location']]
    return ts_event

class StompClient(stomp.ConnectionListener):

    def __init__(self, client_socket):
        self.client_socket = client_socket

    def on_heartbeat(self):
        logging.info('Received a heartbeat')

    def on_heartbeat_timeout(self):
        logging.error('Heartbeat timeout')

    def on_error(self, frame):
        logging.error('Error occurred:')
        logging.error(frame)

    def on_disconnected(self):
        logging.warning('Disconnected - waiting %s seconds before exiting' % RECONNECT_DELAY_SECS)
        time.sleep(RECONNECT_DELAY_SECS)
        exit(-1)

    def on_connecting(self, host_and_port):
        logging.info('Connecting to ' + host_and_port[0])

    def on_message(self, frame):
        try:
            bio = io.BytesIO()
            bio.write(str.encode('utf-16'))
            bio.seek(0)
            msg = zlib.decompress(frame.body, zlib.MAX_WBITS | 32)
            obj = xmltodict.parse(msg.decode('utf-8'))
            ts_event = obj.get('Pport', {}).get('uR', {}).get('TS', {})
            print('tsevent')

            if ts_event is not None:
                ts_event = normalise_ts_event(ts_event)
                # Send event over socket
                print(json.dumps(ts_event))
                self.client_socket.send((json.dumps(ts_event) + '\n').encode('utf-8'))

        except Exception as e:
            logging.error(str(e))


conn = stomp.Connection12([(HOSTNAME, HOSTPORT)],
                          auto_decode=False,
                          heartbeats=(HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS))

if __name__ == "__main__":
    # Setup a socket to send events to Spark over
    s = socket.socket()
    host = "127.0.0.1"
    port = 5555
    s.bind((host, port))

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Wait for client connection.
    client_socket, addr = s.accept()        # Establish connection with client.

    print("Received request from: " + str(addr))

    conn.set_listener('', StompClient(client_socket))
    connect_and_subscribe(conn)

    while True:
        time.sleep(1)
