from enum import Enum
import config
import pickle
import os
from log import LogEntry, LogConsts
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

class Colors:
    VIOLET = '\033[94m'
    BLUE = '\033[36m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'    # yellow
    ERROR = '\033[91m'   # red
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    GRAY = '\033[90m'
    SUCCESS = '\033[42m'
    FAILED = '\033[41m'
    SELECTED = '\033[7m'
    BLINK = '\033[5m'

class Consts(Enum):
    PORT = "PORT"
    CONNECTION = "CONNECTION"
    MARKER = "MARKER" # for sending marker
    TOKEN = "TOKEN" # for sending token
    SNAP = "SNAP" # for sending snapshot from client
    WITH_TOKEN = f"{Colors.SUCCESS}TOKEN{Colors.ENDC}"
    WITHOUT_TOKEN = "WITHOUT_TOKEN"
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"

class RaftConsts(Enum):
    REQVOTE = "REQVOTE"
    VOTE = "VOTE"
    APPEND = "APPEND"
    RESULT = "RESULT"
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

class Message:
    def __init__(self, m_type, term, c_id=None, l_id=None, lli=None, llt=None, ok=None, entries=None, comm_idx=None, sender=None):
        self.m_type = m_type
        self.term = term
        self.c_id = c_id
        self.l_id = l_id
        self.lli = lli
        self.llt = llt
        self.ok = ok
        self.entries = entries
        self.comm_idx = comm_idx
        self.sender = sender

def get_encrypted_key_path(id):
    path = f'{config.FILES_PATH}/{id}_key.pem'
    return path

def generate_encryption_keys(key_size=4096):
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    return private_key, public_key

def save_public_key(public_key, id):
    # write public key to disk and make available to all
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    path = get_encrypted_key_path(id)
    if os.path.isfile(path):
        print("Path already exists - fix this")
        return
    with open(path, 'wb') as f:
        f.write(pem)

def get_public_key(client_name):
    with open(get_encrypted_key_path(client_name), 'rb') as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
        return public_key

def get_encrypted_message(public_key, message):
    encrypted_message = public_key.encrypt(
                            message,
                            padding.OAEP(
                                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                                algorithm=hashes.SHA256(),
                                label=None
                            )
                        )
    return encrypted_message

def get_decrypted_message(private_key, message):
    decrypted_message = private_key.decrypt(
                            message,
                            padding.OAEP(
                                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                                algorithm=hashes.SHA256(),
                                label=None
                            )
                        )
    return decrypted_message

def broadcast(connections, message):
    for client_name, connection in connections.items():
        public_key = get_public_key(client_name)
        # message is bytes anyways
        encrypted_message = get_encrypted_message(public_key, message)
        connection.sendall(encrypted_message)

def send_message(connections, id, message):
    # NODE_FAIL_HANDLING
    if id in connections:
        public_key = get_public_key(id)
        # message is bytes anyways
        encrypted_message = get_encrypted_message(public_key, message)
        connections[id].sendall(encrypted_message)

def get_pid(client_name):
    return int(client_name.split('_')[1])

CLIENT_COUNT = len(config.CLIENT_PORTS)

def prepare_create_entry(term, id, counter, members):
    dict_id = f'{id}_{counter}'
    private_key, public_key = generate_encryption_keys(1024)
    save_public_key(public_key, dict_id)
    private_key_set = dict()
    for member in members:
        mem_pub_key = get_public_key(member)
        private_key_set[member] = get_encrypted_message(mem_pub_key, private_key)
    encrypted_op = get_encrypted_message(pickle.dumps(public_key, LogConsts.CREATE))
    entry = LogEntry(term=term, op_t=encrypted_op, dict_id=dict_id,
                     members=members, pub_key=public_key,
                     pri_keys=pickle.dumps(private_key_set))
    return entry, public_key, private_key

def prepare_put_entry(term, dict_id, issuer, keyval, dict_pub_key):
    encrypted_op = get_encrypted_message(pickle.dumps(dict_pub_key, LogConsts.PUT))
    encrypted_keyval = get_encrypted_message(pickle.dumps(dict_pub_key, keyval))
    entry = LogEntry(term=term, op_t=encrypted_op, dict_id=dict_id, issuer=issuer, keyval=encrypted_keyval)
    return entry

def prepare_get_entry(term, dict_id, issuer, key, dict_pub_key):
    encrypted_op = get_encrypted_message(pickle.dumps(dict_pub_key, LogConsts.GET))
    encrypted_key = get_encrypted_message(pickle.dumps(dict_pub_key, key))
    entry = LogEntry(term=term, op_t=encrypted_op, dict_id=dict_id, issuer=issuer, key=encrypted_key)
    return entry