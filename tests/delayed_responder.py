import select
import redis
import sys
import socket
import time
import signal
import sys

def signal_term_handler(signal, frame):
    print("SHUTTING DOWN!")
    try:
        server_socket.shutdown(socket.SHUT_RDWR)
    except:
        pass
    server_socket.close()
    sys.exit(0)

port1 = int(sys.argv[1])
port2 = int(sys.argv[2])
delay = float(sys.argv[3]) / 1000

print("Starting delayed_responder. Delay: {}".format(delay))

hostname = socket.gethostname()
hostname = "0.0.0.0"
output_stream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
output_stream.connect((hostname, port2))

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((hostname, port1))
server_socket.listen(1)
clients = []

signal.signal(signal.SIGTERM, signal_term_handler)
try:
    while 1:

        sockets = [output_stream, server_socket] + clients
        readable, _, _ = select.select(sockets, [], [])
        for read in readable:
            if read == output_stream:
                data = output_stream.recv(1024)
                now = time.time()
                time.sleep(delay)
                delta = time.time() - now
                print("Microtime: {}".format(delta * 1000))
                input_stream.send(data)
            elif read == server_socket:
                input_stream, _ = server_socket.accept()
                clients.append(input_stream)
            elif read == input_stream:
                data = input_stream.recv(1024)
                output_stream.send(data)
except:
    signal_term_handler(None, None)
