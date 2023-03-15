from enum import Enum
import log
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
        self.timeout = random.randint((config.DEF_TIMEOUT*3 + 1), (config.DEF_DELAY*5)) # 3T+1 < timeout < 5T
        self.voted_for = ""
        self.id = client_name
        self.term = 0
        self.timer = ElectionTimer(self.timeout, self.start_new_election)
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
        self.pbar = tqdm(total=(self.timeout*2), desc="Timeout:", ncols=100, colour='CYAN', bar_format='{l_bar}{bar}|')
    
    def update_pbar(self):
        while True:
            self.pbar.update(1)
            time.sleep(0.5)
    
    def reset_pbar(self):
        self.pbar.refresh()
        self.pbar.reset()

    def start_module(self):
        self.timer.start()
        pbar_thread = threading.Thread(target=self.update_pbar, args=())
        pbar_thread.start()
    
    def start_new_election(self):
        self.timer.restart()
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
            self.timer.reset()
            self.reset_pbar()
            print(f'Sending heartbeat')
            # llt, lli = self.log.get_last_term_idx()
            # heartbeat = Message(m_type=RaftConsts.APPEND, term=self.term, l_id=self.id, lli=lli, llt=llt, entries=[],comm_idx=self.commit_index)
            # tmp = pickle.dumps(heartbeat)
            # broadcast(connections=self.connections, message=tmp)
            self.send_append_rpc()
            time.sleep(config.DEF_DELAY)

    def become_leader(self):
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
        vote_granted = False
        if message.term > self.term:
            self.term = message.term
            self.voted_for = ""
            if self.role == RaftConsts.LEADER or self.role == RaftConsts.CANDIDATE:
                self.role = RaftConsts.FOLLOWER
        if message.term == self.term:
            if self.voted_for == "" or self.voted_for == message.c_id:
                llt, lli = self.log.get_last_term_idx()
                if not ((message.llt < llt) or (message.llt == llt and message.lli < lli)):
                    self.voted_for = message.c_id
                    vote_granted = True
                    
        vote = Message(m_type=RaftConsts.VOTE, term=self.term, ok=vote_granted)
        tmp = pickle.dumps(vote)
        send_message(self.connections, message.c_id, tmp)
        self.write_state_to_disk()
    
    def handle_vote_response(self, message):
        if message.term > self.term:
            self.role = RaftConsts.FOLLOWER
            self.timer.reset()
            self.reset_pbar()
        elif message.term == self.term and self.role == RaftConsts.CANDIDATE and message.ok == True:
            self.votes = self.votes + 1
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
        self.write_state_to_disk()

    def handle_append_rpc(self, message):
        if message.term < self.term:
            return
        if message.term > self.term:
            self.term = message.term
            self.voted_for = ""
        if self.role == RaftConsts.LEADER or self.role == RaftConsts.CANDIDATE:
            self.role = RaftConsts.FOLLOWER
        self.timer.reset()
        self.reset_pbar()
        
        term_t = self.log.get_term_at_index(message.lli)
        if not term_t == message.llt:
            response = Message(m_type=RaftConsts.RESULT, term=self.term, ok=False, sender=self.id)
            tmp = pickle.dumps(response)
            send_message(self.connections, message.c_id, tmp)
        
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
        if message.ok == False:
            self.next_index[message.sender] = self.next_index[message.sender] - 1
            self.send_append_rpc(client=message.sender)
        else:
            if not message.lli == 0:
                self.replies_for_append[message.lli].add(message.sender)
                # commit log(s)
                if len(self.replies_for_append[message.lli]) >= self.quorum and message.lli > self.commit_index:
                    self.commit_index = message.lli
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