import zmq
import json
import threading
import time
import sys
import signal


signal.signal(signal.SIGINT, signal.SIG_DFL)

class RaftNode:
    

    def __init__(self, node_id, address, peers):
        global socket
        self.node_id = node_id
        self.address = address
        self.peers = peers
        self.state = 'follower'
        self.leader_id = None
        self.term = 0
        self.vote_count = 0
        self.voted_for = None
        self.socket = None
        self.election_timeout = 5  # Election timeout in seconds
        self.heartbeat_interval = 1  # Heartbeat interval in seconds
        self.logs = [] #List of (logterm,value)          
        self.commit_index = 0
        self.last_applied = 0
        self.key_value_store = {}
        self.prevLogIndex=0
        self.prevLogTerm=0
        self.cur_index=0
        if self.node_id == 0:
            self.state = 'leader'
            self.leader_id = self.node_id
            self.logs=[(0,"hello"),(0,"world"),(1,"gg")]
        
        elif self.node_id ==1:
            self.logs=[(0,"hello"),(0,"world")]
        socket=self.socket

    def handle_heartbeat(self, message):
        print(f"Received heartbeat from leader {message['leader_id']}")
        if self.state == 'follower':
            self.reset_election_timeout()

    def handle_vote_request(self, socket,message):
        print(f"Received vote request from candidate {message['candidate_id']}")
        if self.state == 'follower':
            if self.voted_for is None or self.voted_for == message['candidate_id']:
                self.voted_for = message['candidate_id']
                self.reset_election_timeout()

                socket.send_json({"Vote":"True",'No-response':False})
            else:
                socket.send_json({"Vote":"False",'No-response':False})
        else:
            socket.send_json({"Vote":"False",'No-response':False})

    def handle_client_request(self, client_socket,request):
        # if self.state != 'leader':
        #     print("Only leader can handle client requests.")
        #     return

        request_type = request.get('sub-type')
        key = request.get('key')
        value = request.get('value')

        if request_type == 'SET':
            print(f"Received SET request for key '{key}' with value '{value}'")
            self.key_value_store[key] = value
            self.logs.append({'term': self.term, 'command': f"SET {key} {value}"})
            self.replicate_log_entries()
            response = {
                    'status': 'success',
                    'message': f"Value for key '{key}': {value}",
                    'No-response':False
                }
            self.socket.send_json(response)
        
        elif request_type == 'GET':
            print(f"Received GET request for key '{key}'")
            # print(key,self.key_value_store.keys(),key in self.key_value_store.keys())
            if key in self.key_value_store.keys():
                value = self.key_value_store[key]
                response = {
                    'type': 'client_response',
                    'status': 'success',
                    'message': f"Value for key '{key}': {value}",
                    'No-response':False
                }
                self.socket.send_json(response)
            else:
                response = {
                    'type': 'client_response',
                    'status': 'failure',
                    'message': f"Key '{key}' not found",
                    'No-response':False
                }
                client_socket.send_json(response)

    def reset_election_timeout(self):
        self.election_timer.cancel()
        self.vote_count=0
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        print(f"Node:{self.node_id} started election")
        if self.state != 'leader':
            self.state = 'candidate'
            self.term += 1
            self.voted_for = self.node_id
            self.vote_count = 1  # Vote for self
            # self.reset_election_timeout()
            self.send_vote_requests()

    def send_vote_requests(self):
        print(f"Node:{self.node_id} sent vote req")
        for peer in self.peers:
        # for peer in range(0,3):
            if peer != self.node_id:
                request = {
                    'type': 'request_vote',
                    'term': self.term,
                    'candidate_id': self.node_id
                }
                res = self.send_message(peer, request)
                print(res)
                if(res["No-Response"]==True):
                    print(f"No Response from {peer} in voting")
                elif(res['Vote']=='True'):
                    self.vote_count+=1
        print(f"Node {self.node_id}, vote_cnt {self.vote_count}")
        if(self.vote_count >= len(peers)//2):
        # if(self.vote_count >= 2):
            self.state = 'leader'
            print(f"New Leader is {self.node_id}")
            #send leader message

    def send_heartbeat(self):
        while True:
            if self.state == 'leader':
                for peer in self.peers:
                    if peer != self.node_id:
                        request = {
                            'type': 'heartbeat',
                            'term': self.term,
                            'leader_id': self.node_id,
                            'No-response':False
                        }
                        self.send_message(peer, request)
            time.sleep(self.heartbeat_interval)

    def send_message(self, peer, message):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://127.0.0.1:556{peer}")
        socket.send_json(message)
            # response = socket.recv_json()
        # Timer of 
        timeout = 2 #seconds
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        socks = dict(poller.poll(timeout* 1000))  # Convert timeout to milliseconds
        response = {"No-Response":True}
        
        if socket in socks:
            response = socket.recv_json()
            print(f"Response from {peer}: {response}")
        else:
            print(f"No response received from {peer} within {timeout} seconds.")

        # print(f"Response from {peer}: {response}")
        socket.close()
        return response

    def listen_replication_requests(self,request):
        if self.state == 'leader':
            print(request['success'])
            # if request['type'] == 'append_entries':
            #     self.replicate_log_entries(request)

        else:
            # Long Way (Actual Way)
            leader_log_index = request['prevLogIndex']
            leader_log_term = request['prevLogTerm']
            
            if self.term>leader_log_term:
                logresults={
                    'success':False,
                    
                }
            matching_index=-1
            
            for i in range(len(self.logs)-1,-1,-1):
                if self.logs[i][0]==leader_log_term:
                    if i==leader_log_index:
                        matching_index=i
                        break
                    else:
                        matching_index=i-1
                        break
                    
            if matching_index==leader_log_index:
                logresults={
                    'success':True
                }
                self.send_message(self.node_id,logresults)

            else:
                logresults = {
                    'success':False
                }
                self.logs=self.logs[:matching_index+1]
                self.send_message(self.node_id,logresults)
            print("sent")
        
    def replicate_log_entries(self):
        if self.state == 'leader':
            for peer in self.peers:
                if peer != self.node_id:
                    request = {
                        'type': 'append_entries',
                        'term': self.term,
                        'leader_id': self.node_id,
                        'entries': self.logs,
                        'prevLogIndex':self.cur_index,
                        'prevLogTerm':self.logs[self.cur_index][0],
                        'LeaderCommit':self.commit_index
                    }
                    self.send_message(peer, request)
                    print("sent")
       
                
    def run(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(self.address)

       


        
        # self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        # self.election_timer.start()

        # heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        # heartbeat_thread.start()


            

        while True:
            try:
                # message = self.socket.recv_json(zmq.NOBLOCK)
                message = self.socket.recv_json()
                print(message)
                if message['type'] == 'heartbeat':
                    self.handle_heartbeat(message)
                    # self.socket.send_json({"response": "SUC", "address": self.address})
                elif message['type'] == 'request_vote':
                    self.handle_vote_request(self.socket,message)
                    # self.socket.send_json({"response": "SUC", "address": self.address})
                elif message['type'] == 'client_request':
                    print("yes")
                    self.cur_index=len(self.logs)-1
                    self.replicate_log_entries()
                    # self.handle_client_request(self.socket,message)
                elif message['type'] == 'append_entries':
                    print("got")
                    self.listen_replication_requests(message)

            except KeyboardInterrupt:
                print ("W: interrupt received, killing server…")    
                if self.socket:
                    self.socket.close()
                sys.exit(0)


if __name__ == "__main__":
    node_id = int(input("Enter Node ID: "))
    server_address = f"tcp://127.0.0.1:556{node_id}"
    print(f"Node Listening at {server_address}")
    peers = [0, 1]  # Assuming 5 nodes
    node = RaftNode(node_id, server_address, peers)
    node.run()
