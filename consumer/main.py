import socket
import re
import time
import threading
import json

from itertools import pairwise

from google.cloud import pubsub_v1


SERVER_HOST = "127.0.0.1"
SERVER_PORT = 40000
PUBSUB_TOPIC = "projects/websocket-test-438010/topics/telemetry"


class ReceiveError(Exception):
    pass


class SocketClient(object):

    MESSAGE_DELIMITERS = ["LOGIN:", "JOIN:", "ACK:", "JSON:"]

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connected = False
        self.lock = threading.Lock()
        self.ping_timer = None
        self.ping_rate = 20
        self.buffer = ""

    def connect(self):
        if not self.connected:
            try:
                self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client.settimeout(10)
                self.client.connect((self.host, self.port))
                self.set_keepalive()
                self.connected = True
                self.login()
                self.configure_ping()
                print("===> Connected")
            except TimeoutError:
                print("===> Connection timeout")
                self.disconnect()
            except ConnectionRefusedError:
                print("===> Connection refused")
                self.disconnect()

    def login(self):
        if self.connected:
            with self.lock:
                self.client.send(
                    b'LOGIN:::{"user":"myname","password":"mypassword","app":"Manual Test", "app_ver":"1.0.0", "protocol":" AKS V2 Protocol", "protocol_ver":"1.0.0"}'
                )

    def set_keepalive(self, after_idle_sec=1, interval_sec=3, max_fails=5):
        """Set TCP keepalive on an open socket.

        It activates after 1 second (after_idle_sec) of idleness,
        then sends a keepalive ping once every 3 seconds (interval_sec),
        and closes the connection after 5 failed ping (max_fails), or 15 seconds
        """
        self.client.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
        self.client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
        self.client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)

    def disconnect(self):
        self.client.close()
        if self.ping_timer:
            self.ping_timer.cancel()
        self.connected = False
        time.sleep(1)

    def configure_ping(self):

        def ping():
            if self.connected:
                with self.lock:
                    try:
                        self.client.send(b"PING:n::")
                    except BrokenPipeError:
                        print("===> Ping broken pipe")
                        self.disconnect()
                self.configure_ping()

        self.ping_timer = threading.Timer(self.ping_rate, ping)
        self.ping_timer.start()

    def receive(self):
        msg = None

        try:
            with self.lock:
                msg = self.client.recv(2048)
        except TimeoutError:
            print("===> Connection timeout")
            self.disconnect()
            raise ReceiveError
        except ConnectionResetError:
            print("===> Connection reset")
            self.disconnect()
            raise ReceiveError
        except OSError as e:
            if e.errno == 9:
                print("===> Bad file descriptor")
                self.disconnect()
                raise ReceiveError

        if msg == b"":
            print("===> Connection broken")
            self.disconnect()
            raise ReceiveError

        self.buffer = self.buffer + msg.decode("utf-8")
        return self.process_buffer()

    def process_buffer(self):
        data = []

        # A match pair is created when >= 2 delimiters are seen in the buffer
        # Using this pair, we can either remove the ACK from the buffer or extract the json payload between 2 JSON delimiters
        while match_pair := list(pairwise(re.finditer(r"|".join(self.MESSAGE_DELIMITERS), self.buffer, re.MULTILINE))):

            # Get matching pair of delimiters
            first_match = match_pair[0][0]
            second_match = match_pair[0][1]

            # Identify, process and remove LOGIN payloads that may have been consumed
            if first_match.group(0) == "LOGIN:" or second_match.group(0) == "LOGIN:":
                match = re.search(r"LOGIN:.*\+::({.*})", self.buffer)
                if match:
                    json_msg = json.loads(match.group(1))
                    self.ping_rate = int(json_msg["pingRate"])
                    print(f"===> LOGIN reply received. Ping rate is {self.ping_rate}.")
                self.buffer = re.sub(r"LOGIN:.*\+::({.*})", "", self.buffer)

            # Identify and remove ACK payloads that may have been consumed
            if first_match.group(0) == "ACK:" or second_match.group(0) == "ACK:":
                self.buffer = re.sub(r"ACK:.*\+::", "", self.buffer)

            # Process json payload between 2 JSON delimiters (after sneaky ACKs have been hopefully removed)
            if first_match.group(0) == "JSON:" and second_match.group(0) == "JSON:":
                # Get the position of the starting JSON: and ending JSON: delimiters in the buffer
                start = first_match.span()[0]
                end = second_match.span()[0]

                # Extract the json payload
                json_str = re.sub(r"JSON:[0-9]*::", "", self.buffer[start:end])
                try:
                    json_data = json.loads(json_str)
                    data.append({"data": json.dumps(json_data)})
                except json.decoder.JSONDecodeError:
                    print("===> JSON parsing error")
                    print(self.buffer)

                # Remove the extracted json payload from the buffer
                self.buffer = self.buffer[end:]

        return data


def run():
    client = SocketClient(SERVER_HOST, SERVER_PORT)

    pubsub_topic_name = PUBSUB_TOPIC
    pubsub_publisher = pubsub_v1.PublisherClient(
        pubsub_v1.types.BatchSettings(
            max_latency=1,
        )
    )

    while True:
        while not client.connected:
            client.connect()
        try:
            data = client.receive()
        except ReceiveError:
            continue
        if data:
            for d in data:
                # print(d)
                future = pubsub_publisher.publish(
                    pubsub_topic_name, json.dumps(d).encode("utf-8")
                )
                future.add_done_callback(lambda x: x.result())


if __name__ == "__main__":
    run()
