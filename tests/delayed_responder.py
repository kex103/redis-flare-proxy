import select
import redis
import sys
import socket
import time
import signal
import sys
import traceback

server_socket = None
start = time.time()
print(start)
block_end = None
# Can only handle 1.

def signal_term_handler(signal, frame):
    print("SHUTTING DOWN!")
    print("Signal: {} Frame: {}".format(signal, frame))
    delta = time.time() - start
    print delta
    sys.exit(0)

port1 = int(sys.argv[1])
port2 = int(sys.argv[2])
delay = float(sys.argv[3]) / 1000
admin_port = None
if len(sys.argv) > 4 and sys.argv[4] != 'None':
    admin_port = int(sys.argv[4])

print("Starting delayed_responder. Delay: {}".format(delay))

hostname = "0.0.0.0"
output_stream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
output_stream.connect((hostname, port2))

def start_server_socket():
    global server_socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((hostname, port1))
    server_socket.listen(5)

start_server_socket()

admin_socket = None
if admin_port:
    admin_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    admin_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    admin_socket.bind((hostname, admin_port))
    admin_socket.listen(5)

clients = []
input_stream = None
admin_stream = None

signal.signal(signal.SIGTERM, signal_term_handler)
try:
    while 1:
        timeout = block_end - time.time() if block_end else None
        if timeout < 0:
            timeout = None
        sockets = [output_stream] + clients
        if admin_socket:
            sockets = sockets + [admin_socket]
        if server_socket:
            sockets = sockets + [server_socket]
        readable, _, errorable = select.select(sockets, [], sockets, timeout)
        if block_end and time.time() > block_end:
            print("BLOCK has ended!")
            block_end = None
            start_server_socket()
        for read in readable:
            if read == output_stream:
                data = output_stream.recv(1024)
                now = time.time()
                time.sleep(delay)
                delta = time.time() - now
                print("Microtime: {}".format(delta * 1000))
                try:
                    input_stream.send(data)
                except:
                    input_stream = None
            elif read == input_stream:
                data = input_stream.recv(1024)
                output_stream.send(data)
                if data:
                    print("Sending data to backend: {}".format(data))
                else:
                    print("Terminating input stream early")
                    delta = time.time() - start
                    print(delta)
                    input_stream.close()
                    clients.remove(input_stream)
                    input_stream = None
            elif read == server_socket:
                input_stream, _ = server_socket.accept()
                clients.append(input_stream)
                print("Got a new client!")
                server_socket.listen(5)
            elif read == admin_socket:
                admin_stream, _ = admin_socket.accept()
                clients.append(admin_stream)
            elif read == admin_stream:
                data = admin_stream.recv(1024)
                print("Got an admin message: {}".format(data))
                words = data.split()
                if not words:
                    continue
                if words[0] == "SETDELAY":
                    delay = float(words[1]) / 1000
                    print("Delay is now {}".format(delay))
                elif words[0] == "BLOCK_NEW_CONNS":
                    block_time = float(words[1]) / 1000
                    block_end = time.time() + block_time
                    server_socket.close()
                    server_socket = None

        for error in errorable:
            print("Error!")
            print("{}".format(error.errno()))
        sys.stdout.flush()
except Exception as e:
    print("Socket error!")
    traceback.print_exc(file=sys.stdout)
    sys.stdout.flush()
except:
    e = sys.exc_info()[0]
    print(e)
    sys.stdout.flush()
    signal_term_handler(None, None)

sys.stdout.flush()


