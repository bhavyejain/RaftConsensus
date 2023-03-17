import socket
import threading
import config
import sys
import time
import utils
from threading import Lock
from utils import Colors as c
from queue import PriorityQueue, Queue
import pickle
from utils import Consts, RaftConsts, Message, get_decrypted_message, generate_encryption_keys, save_public_key, convert_bytes_to_private_key, convert_bytes_to_public_key
from raft import ConsensusModule, StateMachine
from log import Log, LogConsts
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

static_connections = dict()
connections = dict()
client_name = ""
pid = 0
message_queue = Queue()
parent_dict = dict()
dict_keys = dict() # key - dict id, val - dict {public: public_key, private: private_key}
counter = 0
consensus_module = ...
failed_links = list()
is_failed = False
private_key = ...

message_queue_lock = Lock()

def handle_client(client, client_id):
    global message_queue, message_queue_lock
    client.sendall(bytes(f'Client {client_name} connected', "utf-8"))
    while True:
        try:
            raw_message = client.recv(config.BUFF_SIZE)
            if raw_message:
                try:
                    # NODE_FAIL_HANDLING
                    if is_failed or client_id in failed_links:
                        print("Either node failed or link failed to {}".format(client_id))
                        continue
                    # decrypted_message = get_decrypted_message(private_key, raw_message)
                    # message = pickle.loads(decrypted_message)
                    message = pickle.loads(raw_message)
                    # print(f'Queueing message from {client_id}')
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
    global local_log, parent_dict, dict_keys, consensus_module, is_failed, static_connections, message_queue
    client.sendall(bytes(f'Client {client_name} connected', "utf-8"))
    while True:
        try:
            message = client.recv(config.BUFF_SIZE).decode()
            if message:
                print(f'{c.VIOLET}{client_id}{c.ENDC}: {message}')
                if is_failed and not message.startswith("FIXPROCESS"):
                    print("Node was crashed. Fix it before for anything")
                    continue
                if message.startswith("CREATE"):
                    member_clients = message.split()[1:]
                    entry = utils.prepare_create_entry(consensus_module.term, client_name, counter, member_clients)
                    dict_keys[entry.dict_id] = dict()
                    dict_keys[entry.dict_id][Consts.PUBLIC] = convert_bytes_to_public_key(entry.pub_key)
                    if client_name in member_clients:
                        # Adding dict private key in my config only if I have access
                        private_keys_dict = pickle.loads(entry.pri_keys)
                        encrypted_pvt_key = get_decrypted_message(private_key, private_keys_dict[client_name])
                        dict_keys[entry.dict_id][Consts.PRIVATE] = convert_bytes_to_private_key(encrypted_pvt_key+entry.rem_pri_key)
                    add_to_log(entry)
                elif message.startswith("PUT"):
                    comp = message.split()
                    dict_id = comp[1]
                    entry = utils.prepare_put_entry(consensus_module.term, dict_id, client_name, (comp[2], comp[3]), dict_keys[dict_id][Consts.PUBLIC])
                    add_to_log(entry)
                elif message.startswith("GET"):
                    comp = message.split()
                    dict_id = comp[1]
                    entry = utils.prepare_put_entry(consensus_module.term, dict_id, client_name, comp[2], dict_keys[dict_id][Consts.PUBLIC])
                    add_to_log(entry)
                elif message.startswith("PRINTDICT"):
                    comp = message.split()
                    dict_id = comp[1]
                    tmp = f'===== Dictionary {dict_id} =====\n'
                    for key, val in parent_dict[dict_id].items():
                        tmp = tmp + f'{key} \t : \t {val}\n'
                    tmp = tmp + f'============================='
                    print(tmp)
                elif message.startswith("FAILLINK"):
                    comp = message.split()
                    conn_name = comp[1] # basically the other client name
                    # NODE_FAIL_HANDLING: add failure logic
                    print(f'Failing the link with {conn_name}')
                    failed_links.append(conn_name)
                    connections.pop(conn_name)
                elif message.startswith("FIXLINK"):
                    comp = message.split()
                    conn_name = comp[1] # basically the other client name
                    # NODE_FAIL_HANDLING: add fix link logic
                    print(f'Fixing the link with {conn_name}')
                    failed_links.remove(conn_name)
                    connections[conn_name] = static_connections[conn_name]
                elif message == "FAILPROCESS":
                    print("I CRASHED!")
                    # NODE_FAIL_HANDLING
                    is_failed = True
                    connections = dict()
                    with message_queue_lock:
                        message_queue = Queue()
                    consensus_module.go_to_fail_state()
                elif message == "FIXPROCESS":
                    print("Re born")
                    # NODE_FAIL_HANDLING
                    consensus_module.restore_node()
                    is_failed = False
                    connections = static_connections.copy()
                elif message == "PRINTALL":
                    tmp = f'Dictionary IDs with {client_name} as member:\n'
                    for key in parent_dict.keys():
                        tmp = tmp + f'{key}\n'
                    print(tmp)
                elif message == "START":
                    consensus_module.start_module(parent_dict=parent_dict, dict_keys=dict_keys, private_key=private_key)
            else:
                print(f'handle_cli# Closing connection to {client_id}')
                client.close()
                break
        except Exception as e:
            print(f'{c.ERROR}handle_cli# Exception thrown in {client_id} thread!{c.ENDC}')
            print(f'Exception: {e.__str__()}, Traceback: {e.__traceback__()}')

def process_messages():
    global message_queue, message_queue_lock, consensus_module
    print("Starting message queue processor...")
    while True:
        msg = message_queue.get(block=True, timeout=None)
        delta1 = round((time.time() -  msg[0]), 2)
        delta2 = round((config.DEF_DELAY - delta1), 2) if delta1 < config.DEF_DELAY else 0
        time.sleep(delta2)
        message = msg[1]

        # NODE_FAIL_HANDLING
        if is_failed or message.c_id in failed_links:
            print(f'Not processing the message from {message.c_id} due to failed link')
            continue
        # print(f'processing message of type {message.m_type.value}')
        consensus_module.handle_message(message)

def receive():
    global consensus_module, parent_dict, private_key, static_connections

    while len(connections) < (len(config.CLIENT_PORTS) - 1):
        # Accept Connection
        client, _ = mySocket.accept()
        client.setblocking(True)
        client_id = client.recv(config.BUFF_SIZE).decode()
        print(f"receive# Connecting with {client_id}...")
        
        if client_id == "CLI":
            target = handle_cli
        else:
            target = handle_client
            connections[client_id] = client
        
        thread = threading.Thread(target=target, args=(client, client_id, ))
        thread.start()
    
    private_key, public_key = generate_encryption_keys()
    save_public_key(public_key, client_name)

    print("Setting up consensus module...")
    # NODE_FAIL_HANDLING
    static_connections = connections.copy()
    consensus_module = ConsensusModule(client_name=client_name, log=local_log, connections=connections)
    print(f'================= STARTUP COMPLETE =================')

    while True:
        client, _ = mySocket.accept()
        client.setblocking(True)
        client_id = client.recv(config.BUFF_SIZE).decode()
        if client_id == "CLI":
            print(f"receive# Connecting with {client_id}...")
            thread = threading.Thread(target=handle_cli, args=(client, client_id, ))
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
            connections.pop(client_tc)
            print(f'{c.ERROR}startup# Failed to connect to {client_tc}!{c.ENDC}')

if __name__ == "__main__":
    global local_log, state_machine
    
    print(f'================= BEGIN STARTUP =================')
    client_name = sys.argv[1]   # c_n
    pid = config.CLIENT_ID_MAP[client_name]

    local_log = Log(client_name)
    # state_machine = StateMachine(client_name, local_log, parent_dict)

    print(f'startup# Setting up Client {client_name}...')

    message_queue_thread = threading.Thread(target=process_messages, args=())
    message_queue_thread.start()

    print("Starting up state machine...")
    # state_machine_thread = threading.Thread(target=state_machine.advance_state_machine, args=())
    # state_machine_thread.start()

    # connect to clients that have started up
    connect_running_clients()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mySocket:
        mySocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        mySocket.bind((config.HOST, config.CLIENT_PORTS[client_name]))
        mySocket.listen(5)

        print('Listening for new connections...')

        receive()