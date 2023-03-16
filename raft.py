from enum import Enum
import log
from log import LogConsts
import config
from timer import ElectionTimer
import random
from utils import RaftConsts, Message, broadcast, send_message
import pickle
import time
import threading
from tqdm import tqdm

class ConsensusModule:
    def __init__(self, client_name, log, connections):
        self.role = RaftConsts.FOLLOWER
        self.timeout = random.randint((config.DEF_TIMEOUT*3 + 1), (config.DEF_DELAY*6)) # 3T+1 < timeout < 5T
        self.voted_for = ""
        self.id = client_name
        self.term = 0
        self.election_timer = ElectionTimer(self.timeout, self.start_new_election)
        self.log = log
        self.connections = connections
        self.votes = 0
        self.commit_index = 0
        self.quorum = int(len(config.CLIENT_PORTS)/2) + 1
        self.next_index = {}
        for client, _ in config.CLIENT_PORTS:
            if not client == self.id:
                self.next_index[client] = 0
        self.replies_for_append = {}
        self.pbar = tqdm(total=(self.timeout), desc="Timeout:", ncols=100, colour='CYAN', bar_format='{l_bar}{bar}|')

    def update_pbar(self):
        while True:
            self.pbar.update(1)
            time.sleep(1)

    def reset_pbar(self):
        self.pbar.refresh()
        self.pbar.reset()

    def start_module(self):
        self.election_timer.start()
        pbar_thread = threading.Thread(target=self.update_pbar, args=())
        pbar_thread.start()

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
        self.votes = 0
        broadcast(connections=self.connections, message=tmp)
        self.write_state_to_disk()

    def heartbeat(self):
        while self.role == RaftConsts.LEADER:
            self.election_timer.reset()
            self.reset_pbar()
            print(f'Sending heartbeat <3 ...')
            # llt, lli = self.log.get_last_term_idx()
            # heartbeat = Message(m_type=RaftConsts.APPEND, term=self.term, l_id=self.id, lli=lli, llt=llt, entries=[],comm_idx=self.commit_index)
            # tmp = pickle.dumps(heartbeat)
            # broadcast(connections=self.connections, message=tmp)
            self.send_append_rpc()
            time.sleep(config.DEF_DELAY * 2.5)

    def become_leader(self):
        print("I AM LEADER NOW!")
        self.role = RaftConsts.LEADER
        heartbeat_thread = threading.Thread(target=self.heartbeat, args=())
        heartbeat_thread.start()
        _, lli = self.log.get_last_term_idx()
        for client, _ in config.CLIENT_PORTS:
            if not client == self.id:
                self.next_index[client] = lli + 1
        self.replies_for_append.clear()
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

        if not vote_granted:
            print(f'Refused vote to {message.c_id} for term {message.term}')

        vote = Message(m_type=RaftConsts.VOTE, term=self.term, ok=vote_granted)
        tmp = pickle.dumps(vote)
        send_message(self.connections, message.c_id, tmp)
        self.write_state_to_disk()

    def handle_vote_response(self, message):
        print(f'Processing incoming vote for term {self.term}')
        if message.term > self.term:
            print("I AM FOLLOWER NOW")
            self.role = RaftConsts.FOLLOWER
            self.election_timer.reset()
            self.reset_pbar()
        elif message.term == self.term and self.role == RaftConsts.CANDIDATE and message.ok == True:
            self.votes = self.votes + 1
            print(f'Votes collected for term {self.term}: {self.votes}')
            if self.votes >= self.quorum:
                self.become_leader()
        self.write_state_to_disk()

    def send_append_rpc(self, client=None):
        for follower, next_idx in self.next_index:
            if client == None or client == follower:
                entries = self.log[(next_idx-1):]
                if not (len(entries) == 0 and entries[-1].index in self.replies_for_append.keys()):
                    self.replies_for_append[entries[-1].index] = set()
                lli = next_idx - 1
                llt = self.log.get_term_at_index(lli)
                msg = Message(m_type=RaftConsts.APPEND, term=self.term, l_id=self.id, lli=lli, llt=llt, entries=entries, comm_idx=self.commit_index)
                tmp = pickle.dumps(msg)
                send_message(self.connections, follower, tmp)
                print(f'Sending AppendRPC to {follower} with {len(entries)} entries')
        self.write_state_to_disk()

    def handle_append_rpc(self, message):
        print(f'Processing incoming AppendRPC for term {message.term} from leader {message.l_id} with {len(message.entries)} entries')
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
            send_message(self.connections, message.c_id, tmp)
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
        send_message(self.connections, message.c_id, tmp)

        self.write_state_to_disk()

    def handle_append_response(self, message):
        print(f'Processing incoming append response from {message.sender}')
        if message.ok == False:
            print("Received NACK")
            self.next_index[message.sender] = self.next_index[message.sender] - 1
            self.send_append_rpc(client=message.sender)
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

    def write_state_to_disk(self):
        filename = f'{config.FILES_PATH}/{self.id}_statevars.txt'
        with open(filename, "w+") as state:
            state.write(self.term)
            state.write(self.voted_for)

    def read_state_from_disk(self):
        filename = f'{config.FILES_PATH}/{self.id}_statevars.txt'
        with open(filename, "r") as state:
            term_ = state.readline().strip()
            self.term = int(term_)
            self.voted_for = state.readline()

class StateMachine:
    def __init__(self, client_name, log, parent_dict):
        self.id = client_name
        self.log = log
        self.parent_dict = parent_dict
        self.last_committed = 0
    
    def reset_state_machine(self):
        self.last_committed = 0
    
    def advance_state_machine(self):
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

    def handle_create(self, entry):
        new_id = entry.dict_id
        if self.id in entry.members:
            self.parent_dict[new_id] = dict()
            print(f'Created new dictionary with id {new_id}')
    
    def handle_put(self, entry):
        if entry.dict_id in self.parent_dict.keys():
            key = entry.keyval[0]
            val = entry.keyval[1]
            self.parent_dict[entry.dict_id][key] = val
            print(f'Inserted key-value pair ({key}, {val}) in dictionary {entry.dict_id}')
    
    def handle_get(self, entry):
        if entry.dict_id in self.parent_dict.keys():
            key = entry.key
            val = self.parent_dict[entry.dict_id][key]
            print(f'Value for key {key} in dictionary {entry.dict_id} : {val}')