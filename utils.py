from enum import Enum
import config
from log import LogEntry, LogConsts
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

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

def broadcast(connections, message):
    for _, connection in connections.items():
        connection.sendall(message)
        # bytes(encrypted_msg, "utf-8")

def send_message(connections, id, message):
    # NODE_FAIL_HANDLING
    if id in connections:
        connections[id].sendall(message)

def get_pid(client_name):
    return int(client_name.split('_')[1])

CLIENT_COUNT = len(config.CLIENT_PORTS)

def prepare_create_entry(term, id, counter, members):
    dict_id = f'{id}_{counter}'
    entry = LogEntry(term=term, op_t=LogConsts.CREATE, dict_id=dict_id, members=members)
    return entry

def prepare_put_entry(term, dict_id, issuer, keyval):
    entry = LogEntry(term=term, op_t=LogConsts.PUT, dict_id=dict_id, issuer=issuer, keyval=keyval)
    return entry

def prepare_get_entry(term, dict_id, issuer, key):
    entry = LogEntry(term=term, op_t=LogConsts.PUT, dict_id=dict_id, issuer=issuer, key=key)
    return entry