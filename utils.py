from enum import Enum
import config

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
    for _, connection in connections:
        connection.sendall(message)

def send_message(connections, id, message):
    connections[id].sendall(message)

def get_pid(client_name):
    return int(client_name.split('_')[1])

CLIENT_COUNT = len(config.CLIENT_PORTS)