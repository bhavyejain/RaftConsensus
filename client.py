import socket
import threading
import config
import sys
import time
from threading import Lock
from utils import Colors as c
from queue import PriorityQueue
import pickle
from utils import RaftConsts, Message
from raft import ConsensusModule
from log import Log, LogConsts

connections = {}
client_name = ""
pid = 0
message_queue = PriorityQueue()
local_log = Log()

message_queue_lock = Lock()

def handle_client(client, client_id):
    global message_queue, message_queue_lock
    client.sendall(bytes(f'Client {client_name} connected', "utf-8"))
    while True:
        try:
            raw_message = client.recv(config.BUFF_SIZE)
            if raw_message:
                try:
                    message = pickle.loads(raw_message)
                    print(f'Queueing message from {client_id}')
                    with message_queue_lock:
                        message_queue.put((round(time.time(), 2), message))
                except pickle.UnpicklingError:
                    print(f'{raw_message.decode()}')
            else:
                print(f'handle_client# Closing connection to {client_id}')
                client.close()
                break
        except Exception as e:
            print(f'{c.ERROR}handle_client# Exception thrown in {client_id} thread!{c.ENDC}')
            print(f'Exception: {e.__str__()}, Traceback: {e.__traceback__()}')

def handle_cli(client, client_id):
    client.sendall(bytes(f'Client {client_name} connected', "utf-8"))
    while True:
        try:
            message = client.recv(config.BUFF_SIZE).decode()
            if message:
                print(f'{c.VIOLET}{client_id}{c.ENDC}: {message}')
                if message.startswith("CREATE"):
                    member_clients = message.split()[1:]
                elif message.startswith("PUT"):
                    comp = message.split()
                elif message.startswith("GET"):
                    comp = message.split()
                elif message.startswith("PRINTDICT"):
                    comp = message.split()
                elif message.startswith("FAILLINK"):
                    comp = message.split()
                    conn_name = comp[1] # basically the other client name
                elif message.startswith("FIXLINK"):
                    comp = message.split()
                    conn_name = comp[1] # basically the other client name
                elif message == "FAILPROCESS":
                    print("lala")
                elif message == "PRINTALL":
                    print("hehe")

            else:
                print(f'handle_cli# Closing connection to {client_id}')
                client.close()
                break
        except Exception as e:
            print(f'{c.ERROR}handle_cli# Exception thrown in {client_id} thread!{c.ENDC}')
            print(f'Exception: {e.__str__()}, Traceback: {e.__traceback__()}')

def process_messages():
    global message_queue, message_queue_lock, consensus_module
    while True:
        with message_queue_lock:
            msg = message_queue.get(block=True, timeout=None)
        delta1 = round((time.time() -  msg[0]), 2)
        delta2 = round((config.DEF_DELAY - delta1), 2) if delta1 < config.DEF_DELAY else 0
        time.sleep(delta2)
        message = msg[1]
        print(f'processing message of type {message.m_type.value}')
        consensus_module.handle_message(message)

def receive():
    global consensus_module
    while True:
        # Accept Connection
        client, addr = mySocket.accept()
        client.setblocking(True)
        client_id = client.recv(config.BUFF_SIZE).decode()
        print(f"receive# Connecting with {client_id}...")
        
        if client_id == "CLI":
            target = handle_cli
        else:
            target = handle_client
            connections[client_id] = client
            if len(connections) == (len(config.CLIENT_PORTS) - 1):
                consensus_module.start_module()

        thread = threading.Thread(target=target, args=(client, client_id, ))
        thread.start()

def connect_running_clients():
    for n in range(1, pid):
        client_tc = f'c_{n}'
        print(f'startup# Connecting to {client_tc}...')
        try:
            connections[client_tc] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connections[client_tc].connect((config.HOST, config.CLIENT_PORTS[client_tc]))
            connections[client_tc].setblocking(True)
            connections[client_tc].sendall(bytes(client_name, "utf-8"))
            print(f"startup# {connections[client_tc].recv(config.BUFF_SIZE).decode()}")
            thread = threading.Thread(target=handle_client, args=(connections[client_tc], client_tc,))
            thread.start()
        except:
            print(f'{c.ERROR}startup# Failed to connect to {client_tc}!{c.ENDC}')

if __name__ == "__main__":
    
    global consensus_module
    
    client_name = sys.argv[1]   # c_n
    pid = config.client_name_MAP[client_name]

    consensus_module = ConsensusModule(client_name, local_log, connections)

    print(f'================= BEGIN STARTUP =================')
    print(f'startup# Setting up Client {client_name}...')

    # connect to clients that have started up
    connect_running_clients()

    message_queue_thread = threading.Thread(target=process_messages, args=())
    message_queue_thread.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mySocket:
        mySocket.bind((config.HOST, config.CLIENT_PORTS[client_name]))
        mySocket.listen(5)

        print(f'================= STARTUP COMPLETE =================')
        print('Listening for new connections...')

        receive()