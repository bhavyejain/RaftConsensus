import socket
import threading
import config
import sys
import time
import utils
from threading import Lock
from utils import Colors as c
from queue import PriorityQueue
import pickle
from utils import RaftConsts, Message
from raft import ConsensusModule, StateMachine
from log import Log, LogConsts
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

connections = dict()
client_name = ""
pid = 0
message_queue = PriorityQueue()
local_log = Log()
parent_dict = dict()
counter = 0

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

def add_to_log(entry):
    global local_log, consensus_module
    local_log.append_log(entry)
    consensus_module.update_next_index()
    consensus_module.send_append_rpc()

def handle_cli(client, client_id):
    global local_log, parent_dict, consensus_module
    client.sendall(bytes(f'Client {client_name} connected', "utf-8"))
    while True:
        try:
            message = client.recv(config.BUFF_SIZE).decode()
            if message:
                print(f'{c.VIOLET}{client_id}{c.ENDC}: {message}')
                if message.startswith("CREATE"):
                    member_clients = message.split()[1:]
                    entry = utils.prepare_create_entry(consensus_module.term, client_name, counter, member_clients)
                    add_to_log(entry)
                elif message.startswith("PUT"):
                    comp = message.split()
                    entry = utils.prepare_put_entry(consensus_module.term, comp[1], client_name, (comp[2], comp[3]))
                    add_to_log(entry)
                elif message.startswith("GET"):
                    comp = message.split()
                    entry = utils.prepare_put_entry(consensus_module.term, comp[1], client_name, comp[2])
                    add_to_log(entry)
                elif message.startswith("PRINTDICT"):
                    comp = message.split()
                    dict_id = comp[1]
                    tmp = f'===== Dictionary {dict_id} =====\n'
                    for key, val in parent_dict[dict_id]:
                        tmp = tmp + f'{key} \t : \t {val}\n'
                    tmp = tmp + f'============================='
                    print(tmp)
                elif message.startswith("FAILLINK"):
                    comp = message.split()
                    conn_name = comp[1] # basically the other client name
                    # TODO: add failure logic
                elif message.startswith("FIXLINK"):
                    comp = message.split()
                    conn_name = comp[1] # basically the other client name
                    # TODO: add fix logic
                elif message == "FAILPROCESS":
                    print("I CRASHED!")
                    # TODO: add failure logic
                elif message == "PRINTALL":
                    tmp = f'Dictionary IDs with {client_name} as member:\n'
                    for key in parent_dict.keys():
                        tmp = tmp + f'{key}\n'
                    print(tmp)
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
                print("Starting up consensus module...")
                consensus_module.start_module()
                print(f'================= STARTUP COMPLETE =================')

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
    global consensus_module, state_machine
    
    client_name = sys.argv[1]   # c_n
    pid = config.client_name_MAP[client_name]

    consensus_module = ConsensusModule(client_name, local_log, connections)
    state_machine = StateMachine(client_name, local_log, parent_dict)

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend()
    )
    public_key = private_key.public_key()

    # write public key to disk and make available to all
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    with open(f'{config.FILES_PATH}/{client_name}_key.pem', 'wb') as f:
        f.write(pem)

    print(f'================= BEGIN STARTUP =================')
    print(f'startup# Setting up Client {client_name}...')

    # connect to clients that have started up
    connect_running_clients()

    message_queue_thread = threading.Thread(target=process_messages, args=())
    message_queue_thread.start()

    print("Starting up state machine...")
    state_machine_thread = threading.Thread(target=state_machine.advance_state_machine, args=())
    state_machine_thread.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mySocket:
        mySocket.bind((config.HOST, config.CLIENT_PORTS[client_name]))
        mySocket.listen(5)

        print('Listening for new connections...')

        receive()