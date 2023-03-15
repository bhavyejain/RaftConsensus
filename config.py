import os

CLIENT_PORTS = {"c_1": 9260, "c_2": 9261, "c_3": 9262, "c_4": 9263, "c_5": 9264}
BUFF_SIZE = 4096
ENCRYPTED_BUFF_SIZE = 4096
HOST = '127.0.0.1'
DEF_DELAY = 3.00
DEF_TIMEOUT = 12.00
CLIENT_ID_MAP = {"c_1": 1, "c_2": 2, "c_3": 3, "c_4": 4, "c_5": 5}
FILES_PATH = f'{os.getcwd()}/raft_disk'