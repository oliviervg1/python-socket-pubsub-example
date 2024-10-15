import socket
import json

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 40000

LOGIN_REPLY = {
    "name":"serverName",
    "ver":"1.2.0",
    "min_ver":"1.0.0",
    "pingRate":"10",
    "timeout":"40"
}


class SocketServer(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connection = None
        self.counter = 0

    def start(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(0)
        self.connection, _ = self.server.accept()
        self.connection.settimeout(0.001)

    def send_data_and_check_ping(self, data):
        self.start()
        try:
            with self.connection:
                while True:
                    for line in data.splitlines():
                        try:
                            self.connection.sendall(line.encode("utf-8"))
                            msg = self.connection.recv(1024).decode("utf-8")
                            print(f"===> Received: {msg}")
                            if "LOGIN" in msg:
                                print("===> Send LOGIN reply")
                                self.connection.sendall(f"LOGIN:n+::{json.dumps(LOGIN_REPLY)}".encode("utf-8"))
                            if "PING" in msg:
                                print("===> Send ACK")
                                self.connection.sendall(b"ACK:n+::")
                        except TimeoutError:
                            continue
                    self.counter += 3
                    print(self.counter)
        except OSError as e:
            if e.errno == 104 or e.errno == 32:
                print(f"===> {e}")
                self.server.close()
                self.counter = 0
                self.send_data_and_check_ping(data)
            else:
                raise


def run():
    server = SocketServer(SERVER_HOST, SERVER_PORT)
    with open("sample_data.txt") as f:
        data = f.read()
    server.send_data_and_check_ping(data)


if __name__ == "__main__":
    run()
