from enum import Enum
import log
from log import LogConsts
import config
from timer import ElectionTimer
import random
from utils import Consts, RaftConsts, Message, broadcast, send_message, get_decrypted_message, convert_bytes_to_private_key, convert_bytes_to_public_key
import pickle
import time
import threading
from tqdm import tqdm
from threading import Lock

class ConsensusModule:
    def __init__(self, client_name, log, connections):
        self.id = client_name
        random.seed(int(client_name.split('_')[1])*60)
        self.role = RaftConsts.FOLLOWER
        self.timeout = random.randint((config.DEF_DELAY*3 + 1), (config.DEF_DELAY*6)) # 3T+1 < timeout < 5T
        print(f'Setting up consensus module with timeout {self.timeout} seconds')
        self.voted_for = ""
        self.term = 0
        self.election_timer = ElectionTimer(self.timeout, self.start_new_election)
        self.log = log
        self.connections = connections
        self.votes = 0
        self.commit_index = 0
        self.quorum = int(len(config.CLIENT_PORTS)/2) + 1
        self.next_index = {}
        self.state_machine = None
        self.last_append = {}
        self.counter = 0

        for client in config.CLIENT_PORTS.keys():
            if not client == self.id:
                self.next_index[client] = 0
                self.last_append[client] = 0
        self.replies_for_append = {}
        self.sending_rpc = Lock()
        self.pbar = tqdm(total=(self.timeout), desc="Timeout:", colour='CYAN', bar_format='{l_bar}{bar}|')
        print()
    
    def update_counter(self):
        self.counter += 1

    def update_pbar(self):
        while self.election_timer.running:
            self.pbar.update(1)
            time.sleep(1)

    def reset_pbar(self):
        self.pbar.refresh()
        self.pbar.reset()

    def start_module(self, parent_dict, dict_keys, private_key):
        print("Starting consensus module...")
        self.election_timer.start()
        pbar_thread = threading.Thread(target=self.update_pbar, args=())
        pbar_thread.start()

        self.state_machine = StateMachine(self.id, self.log, parent_dict, dict_keys, private_key)
        state_machine_thread = threading.Thread(target=self.state_machine.advance_state_machine, args=())
        state_machine_thread.start()

    def start_new_election(self):
        self.election_timer.restart()
        self.reset_pbar()
        print(f'Starting new election!')
        self.role = RaftConsts.CANDIDATE
        self.term = self.term + 1
        self.voted_for = self.id
        llt, lli = self.log.get_last_term_idx()
        request_vote = Message(m_type=RaftConsts.REQVOTE, term=self.term, c_id=self.id, lli=lli, llt=llt)
        tmp = pickle.dumps(request_vote)
        self.votes = 1
        print(f'Broadcasting vote request for term {self.term}...')
        broadcast(connections=self.connections, message=tmp)
        self.write_state_to_disk()

    def heartbeat(self):
        while self.role == RaftConsts.LEADER:
            with self.sending_rpc:
                curr_time = round(time.time(), 2)
                clients = []
                for client, last in self.last_append.items():
                    if (curr_time - last) > (config.DEF_DELAY * 2):
                        clients.append(client)
                if len(clients) > 0:
                    print(f'Sending heartbeat <3 to {", ".join(clients)}...')
                    self.send_append_rpc(clients=clients)
            time.sleep(0.5)

    def become_leader(self):
        print("I AM LEADER NOW!")
        self.role = RaftConsts.LEADER
        heartbeat_thread = threading.Thread(target=self.heartbeat, args=())
        heartbeat_thread.start()
        _, lli = self.log.get_last_term_idx()
        print(f'lli: {lli}')
        for client in config.CLIENT_PORTS.keys():
            if not client == self.id:
                self.next_index[client] = lli + 1
                self.last_append[client] = 0
        self.replies_for_append.clear()
        self.election_timer.cancel()
        self.write_state_to_disk()

    def handle_vote_request(self, message):
        print(f'Processing vote request from {message.c_id} for term {message.term}')
        vote_granted = False
        if message.term > self.term:
            self.term = message.term
            self.voted_for = ""
            if self.role == RaftConsts.LEADER or self.role == RaftConsts.CANDIDATE:
                self.role = RaftConsts.FOLLOWER
                print("I AM FOLLOWER NOW")
        if message.term == self.term:
            if self.voted_for == "" or self.voted_for == message.c_id:
                llt, lli = self.log.get_last_term_idx()
                if not ((message.llt < llt) or (message.llt == llt and message.lli < lli)):
                    self.voted_for = message.c_id
                    vote_granted = True
                    print(f'Granted vote to {message.c_id} for term {message.term}')
                    self.election_timer.reset()
                    self.reset_pbar()

        if not vote_granted:
            print(f'Refused vote to {message.c_id} for term {message.term}')

        vote = Message(m_type=RaftConsts.VOTE, term=self.term, ok=vote_granted)
        tmp = pickle.dumps(vote)
        send_message(self.connections, message.c_id, tmp)
        self.write_state_to_disk()

    def handle_vote_response(self, message):
        print(f'Vote | {message.ok} | term: {self.term}')
        if message.term > self.term:
            print("I AM FOLLOWER NOW")
            self.role = RaftConsts.FOLLOWER
            self.term = message.term
            self.voted_for = ""
            self.election_timer.reset()
            self.reset_pbar()
        elif message.term == self.term and self.role == RaftConsts.CANDIDATE and message.ok == True:
            self.votes = self.votes + 1
            print(f'Votes collected for term {self.term}: {self.votes}')
            if self.votes >= self.quorum:
                self.become_leader()
        self.write_state_to_disk()

    def send_append_rpc(self, clients=None):
        tmp_f = []
        for follower, next_idx in self.next_index.items():
            if clients == None or (follower in clients):
                entries = self.log.get_entries_from_index(next_idx)
                if not (len(entries) == 0):
                    if not (entries[-1].index in self.replies_for_append.keys()):
                        self.replies_for_append[entries[-1].index] = set()
                lli = next_idx - 1
                llt = self.log.get_term_at_index(lli)
                msg = Message(m_type=RaftConsts.APPEND, term=self.term, l_id=self.id, lli=lli, llt=llt, entries=entries, comm_idx=self.commit_index)
                tmp = pickle.dumps(msg)
                self.last_append[follower] = round(time.time(), 2)
                print(f'Sending AppendRPC to {follower} | {len(entries)} entries | prev_index: {lli} | prev_term: {llt}')
                send_message(self.connections, follower, tmp)
                tmp_f.append(follower)
        
        if len(tmp_f) > 0:
            # print(f'Sending AppendRPC to {", ".join(tmp_f)} | {len(entries)} entries | prev_index: {lli} | prev_term: {llt}')
            self.write_state_to_disk()

    def handle_append_rpc(self, message):
        print(f'{message.l_id}: AppendRPC | term: {message.term} | entries: {len(message.entries)}')
        if message.term < self.term:
            return
        if message.term > self.term:
            self.term = message.term
            self.voted_for = ""
        if self.role == RaftConsts.LEADER or self.role == RaftConsts.CANDIDATE:
            self.role = RaftConsts.FOLLOWER
            print("I AM FOLLOWER NOW")
        self.election_timer.reset()
        self.reset_pbar()
        term_t = self.log.get_term_at_index(message.lli)
        if not term_t == message.llt:
            response = Message(m_type=RaftConsts.RESULT, term=self.term, ok=False, sender=self.id)
            tmp = pickle.dumps(response)
            send_message(self.connections, message.l_id, tmp)
            print(f'Previous term and index do not match, rejected AppendRPC')
            return
        self.log.handle_incoming_entries(message.entries, message.lli, message.comm_idx)
        self.commit_index = message.comm_idx
        if not len(message.entries) == 0:
            last_added_index = message.entries[-1].index
        else:
            last_added_index = 0
        response = Message(m_type=RaftConsts.RESULT, term=self.term, lli=last_added_index, ok=True, sender=self.id)
        tmp = pickle.dumps(response)
        send_message(self.connections, message.l_id, tmp)
        self.write_state_to_disk()

    def handle_append_response(self, message):
        print(f'Processing incoming append response from {message.sender}')
        if self.role == RaftConsts.LEADER and message.term <= self.term:
            if message.ok == False:
                print("Received NACK")
                self.next_index[message.sender] = self.next_index[message.sender] - 1
                self.send_append_rpc(clients=[message.sender])
            else:
                if not message.lli == 0:
                    print(f'Adding response for index {message.lli}')
                    self.replies_for_append[message.lli].add(message.sender)
                    # commit log(s)
                    if len(self.replies_for_append[message.lli]) >= self.quorum and message.lli > self.commit_index:
                        print(f'Committing log index {message.lli}')
                        self.commit_index = message.lli
                        self.log.commit_index = self.commit_index
                    self.next_index[message.sender] = message.lli + 1
        elif self.role == RaftConsts.LEADER and message.term > self.term:
            self.term = message.term
            self.voted_for = ""
            self.role = RaftConsts.FOLLOWER
            print("I AM FOLLOWER NOW")
            self.election_timer.reset()
            self.reset_pbar()
        self.write_state_to_disk()

    # send message to the designated handler
    def handle_message(self, message):
        if message.m_type == RaftConsts.REQVOTE:
            self.handle_vote_request(message)
        elif message.m_type == RaftConsts.VOTE:
            self.handle_vote_response(message)
        elif message.m_type == RaftConsts.APPEND:
            self.handle_append_rpc(message)
        elif message.m_type == RaftConsts.RESULT:
            self.handle_append_response(message)
    
    def update_next_index(self):
        _, lli = self.log.get_last_term_idx()
        for client in config.CLIENT_PORTS.keys():
            if not client == self.id:
                self.next_index[client] = lli + 1

    def write_state_to_disk(self):
        filename = f'{config.FILES_PATH}/{self.id}_statevars.txt'
        with open(filename, "w+") as state:
            state.write(str(self.term))
            state.write(self.voted_for)
            state.write(str(self.counter))

    def read_state_from_disk(self):
        filename = f'{config.FILES_PATH}/{self.id}_statevars.txt'
        with open(filename, "r") as state:
            term_ = state.readline().strip()
            self.term = int(term_)
            self.voted_for = state.readline()
            counter_ = state.readline().strip()
            self.counter = int(counter_)
    
    def go_to_fail_state(self):
        # NODE_FAIL_HANDLING
        self.election_timer.cancel()
        self.next_index = dict()
        self.voted_for = ""
        self.term = 0
        self.log.clear()
        self.votes = 0
        self.commit_index = 0
        self.counter = 0
    
    def restore_node(self):
        # NODE_FAIL_HANDLING
        self.read_state_from_disk()
        self.log.read_logs_from_disk()
        self.commit_index = self.log.commit_index
        if self.state_machine is not None:
            self.state_machine.reset_state_machine()
        self.election_timer.restart()

class StateMachine:
    def __init__(self, client_name, log, parent_dict, dict_keys, private_key):
        self.id = client_name
        self.log = log
        self.parent_dict = parent_dict
        self.dict_keys = dict_keys
        self.last_committed = 0
        self.private_key = private_key
    
    def reset_state_machine(self):
        self.last_committed = 0
        self.parent_dict.clear()
    
    def advance_state_machine(self):
        while True:
            if self.log.commit_index > self.last_committed:
                num_entries = self.log.num_entries()
                while self.last_committed < num_entries and self.last_committed < self.log.commit_index:
                    self.last_committed = self.last_committed + 1
                    print(f'Executing entry at index {self.last_committed}')
                    entry = self.log.get_entry_at_index(self.last_committed)
                    if entry.op_t == LogConsts.CREATE:
                        self.handle_create(entry)
                    elif entry.op_t == LogConsts.PUT:
                        self.handle_put(entry)
                    elif entry.op_t == LogConsts.GET:
                        self.handle_get(entry)
            time.sleep(0.5) # check twice every second

    def handle_create(self, entry):
        new_id = entry.dict_id
        self.dict_keys[new_id] = dict()
        self.dict_keys[new_id][Consts.PUBLIC] = convert_bytes_to_public_key(entry.pub_key)
        if self.id in entry.members:
            self.parent_dict[new_id] = dict()
            self.parent_dict["members"][new_id] = entry.members
            private_keys_dict = pickle.loads(entry.pri_keys)
            encrypted_pvt_key = get_decrypted_message(self.private_key, private_keys_dict[self.id])
            self.dict_keys[new_id][Consts.PRIVATE] = convert_bytes_to_private_key(encrypted_pvt_key+entry.rem_pri_key)
            print(f'Created new dictionary with id {new_id}')
    
    def handle_put(self, entry):
        if entry.dict_id in self.parent_dict.keys():
            if entry.issuer in self.parent_dict["members"][entry.dict_id]:
                key_val = pickle.loads(get_decrypted_message(self.dict_keys[entry.dict_id][Consts.PRIVATE], entry.keyval))
                key = key_val[0]
                val = key_val[1]
                self.parent_dict[entry.dict_id][key] = val
                print(f'Inserted key-value pair ({key}, {val}) in dictionary {entry.dict_id}')
    
    def handle_get(self, entry):
        if entry.dict_id in self.parent_dict.keys():
            if entry.issuer in self.parent_dict["members"][entry.dict_id]:
                key = pickle.loads(get_decrypted_message(self.dict_keys[entry.dict_id][Consts.PRIVATE], entry.key))
                val = self.parent_dict[entry.dict_id][key]
                if self.id == entry.issuer:
                    print(f'Value for key {key} in dictionary {entry.dict_id} : {val}')