from enum import Enum
import config
import pickle

class LogConsts(Enum):
    CREATE = "CREATE"
    PUT = "PUT"
    GET = "GET"

class LogEntry:
    def __init__(self, term, index, op_t, dict_id, members=None, pub_key=None, pri_keys=None, issuer=None, key=None, keyval=None):
        self.term = term
        self.index = index
        self.op_t = op_t    # operation type
        self.dict_id = dict_id  # dictionary id
        self.members = members
        self.pub_key = pub_key  # public key of the dictionary
        self.pri_keys = pri_keys    # private key of dict encoded with public key of each client
        self.issuer = issuer
        self.key = key
        self.keyval = keyval    # key value pair to be inserted

class Log:
    def __init__(self):
        self.log = []
        self.commit_index = 0
    
    def get_last_term_idx(self):
        if(len(self.log) == 0):
            return 0, 0
        else:
            last_log = self.log[-1]
            return last_log.term, last_log.index
    
    def write_logs_to_disk(self):
        filename = f'{config.FILES_PATH}/{self.id}_log.log'
        with open(filename, "wb") as log_file:
            for entry in self.log:
                pickle.dump(entry, log_file, pickle.HIGHEST_PROTOCOL)
    
    def read_logs_from_disk(self):
        self.log.clear()
        filename = f'{config.FILES_PATH}/{self.id}_log.log'
        with open(filename, "rb") as log_file:
            entry = pickle.load(log_file)
            self.log.append(entry)
    
    def append_log(self, log_entry):
        _, lli = self.get_last_term_idx()
        log_entry.index = lli + 1
        self.log.append(log_entry)
        self.write_logs_to_disk()
    
    def get_term_at_index(self, idx):
        if idx == 0 or len(self.log) == 0:
            return 0
        end = len(self.log) - 1
        for i in range(end, -1, -1):
            if self.log[i].index == idx:
                return self.log[i].term
        return 0
    
    def handle_incoming_entries(self, entries, lli, comm_idx):
        if not len(entries) == 0:
            idx = 0
            if not lli == 0:
                idx = len(self.log) - 1
                while idx >= 0:
                    if self.log[idx].index == lli:
                        idx = idx + 1
                        break
                    else:
                        idx = idx - 1
            self.log = self.log[:idx]
            for entry in entries:
                self.log.append(entry)

        self.write_logs_to_disk()
        self.commit_index = comm_idx