import select
import redis
import sys
import socket
import time
import signal
import sys
import traceback
import datetime
import threading

def log(msg):
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')
    print "{}: {}".format(st, msg)
    sys.stdout.flush()

def delayed_send(stream, message, delay):
    time.sleep(delay)
    log("Finished sleeping: Sending {}".format(message))
    try:
        stream.send(message)
    except:
        pass

server_socket = None
start = time.time()
log(start)
block_end = None
# Can only handle 1.

def signal_term_handler(signal, frame):
    print("SHUTTING DOWN!")
    print("Signal: {} Frame: {}".format(signal, frame))
    delta = time.time() - start
    log(delta)
    sys.exit(0)

port1 = int(sys.argv[1])
port2 = int(sys.argv[2])
delay = float(sys.argv[3]) / 1000
admin_port = None
if len(sys.argv) > 4 and sys.argv[4] != 'None':
    admin_port = int(sys.argv[4])

log("Starting delayed_responder. Delay: {} from port {} to port {}".format(delay, port1, port2))

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
        log("Start waiting for select")
        readable, _, errorable = select.select(sockets, [], sockets, timeout)
        log("Select finished!")
        if block_end and time.time() > block_end:
            log("BLOCK has ended!")
            block_end = None
            start_server_socket()
        for read in readable:
            log("Readable! {}".format(read))
            if read == output_stream:
                data = output_stream.recv(1024)
                log("Message received: {}".format(data))
                # Should sleep in a separate thread.
                t = threading.Thread(target=delayed_send, args=(input_stream, data, delay))
                t.start()
               # log("Delay finished")
              #  if not input_stream:
              #      log("Skipped sending to client because gone")
               #     continue
              #  try:
              #      input_stream.send(data)
               # except Exception as e:
                 #   log("Failed to send data {} to input stream. Reason: {}".format(data, repr(e)))
                #    input_stream.close()
                #    clients.remove(input_stream)
                #    input_stream = None
            elif read == input_stream:
                data = input_stream.recv(1024)
                output_stream.send(data)
                if data:
                    log("Sending data to backend: {}".format(data))
                else:
                    log("Terminating input stream early")
                    input_stream.close()
                    clients.remove(input_stream)
                    input_stream = None
            elif read == server_socket:
                input_stream, _ = server_socket.accept()
                clients.append(input_stream)
                log("Got a new client!")
                server_socket.listen(5)
            elif read == admin_socket:
                log("Got a new admin client!")
                admin_stream, _ = admin_socket.accept()
                clients.append(admin_stream)
            elif read == admin_stream:
                log("Got something on admin")
                data = admin_stream.recv(1024)
                if not data:
                    log("Terminating admin stream early")
                    admin_stream.close()
                    clients.remove(admin_stream)
                    admin_stream = None
                    continue
                log("Got an admin message: {} {}".format(data, admin_stream))
                words = data.split()
                if not words:
                    continue
                if words[0] == "SETDELAY":
                    delay = float(words[1]) / 1000
                    log("Delay is now {}".format(delay))
                elif words[0] == "BLOCK_NEW_CONNS":
                    block_time = float(words[1]) / 1000
                    block_end = time.time() + block_time
                    server_socket.close()
                    server_socket = None
            else:
                log("Some ex-client?")
                try:
                    clients.remove(read)
                except:
                    pass
                try:
                    read.close()
                except:
                    pass

        for error in errorable:
            log("Error!")
            log("{}".format(error.errno()))
        sys.stdout.flush()
except Exception as e:
    log("Socket error!")
    traceback.print_exc(file=sys.stdout)
    sys.stdout.flush()
except:
    e = sys.exc_info()[0]
    log(e)
    sys.stdout.flush()
    signal_term_handler(None, None)

sys.stdout.flush()


