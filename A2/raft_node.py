import zmq
import json
import threading
import time
import sys
import signal

#Merge Conflict resolved code

signal.signal(signal.SIGINT, signal.SIG_DFL)

class RaftNode:
    def __init__(self, node_id, address, peers):
        self.node_id = node_id
        self.address = address
        self.peers = peers
        self.state = 'follower'
        self.leader_id = -1
        self.leader_address=f"tcp://127.0.0.1:555{self.leader_id}"
        self.term = 0
        self.vote_count = 0
        self.voted_for = None
        self.socket = None
        self.election_timeout = 5  # Election timeout in seconds
        self.heartbeat_interval = 1  # Heartbeat interval in seconds
        self.logs = []#List of (logterm,value)    
        # Denotes the index of the entry of the log last commited succesfully  
        self.commit_index = -1
        self.last_applied = 0
        self.key_value_store = {}
        self.prevLogIndex=0
        self.prevLogTerm=0
        # self.cur_index={}
        self.cur_index={i:len(self.logs)-1 for i in self.peers}
        if self.node_id == 0:
            # self.state = 'leader'
            # self.leader_id = self.node_id
            self.logs = [{'term': 0, 'command': "SET",'key':"0",'value':"hello"},{'term':0,'command':'SET','key':"1",'value':"world"},{'term':1,'command':'SET','key':"2",'value':"gg"}]
            # self.logs=[(0,"hello"),(0,"world"),(1,"gg")]
            self.key_value_store = {"0":"hello","1":"world","2":"gg"}
            self.commit_index=1
            self.term = 1

        elif self.node_id ==1:
            self.logs = [{'term': 0, 'command': "SET",'key':0,'value':"hello"},{'term':0,'command':'SET','key':1,'value':"world"}]
            self.key_value_store={0:"hello",1:"world"}
            self.term  = 0
        elif self.node_id==2:
            # self.logs=[(0,"hello"),(1,"y")]
            self.logs=[{'term': 0, 'command': "SET",'key':0,'value':"hello"},{'term':1,'command':'SET','key':1,'value':"y"}]
            self.key_value_store={0:"hello",1:"y"}
            self.term = 1

    def handle_heartbeat(self, message):
        print(f"Received heartbeat from leader {message['leader_id']}")
        # self.commit_entries()
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

        request_type = request.get('sub-type')
        key = request.get('key')
        value = request.get('value')

        if request_type == 'SET':
            if self.state != 'leader':
                response={
                    'status':'failure',
                    'leaderId':self.leader_id,
                    'No-response':False
                }
                self.socket.send_json(response)
                return
            print(f"Received SET request for key '{key}' with value '{value}'")
            if self.state=='leader':     
                self.key_value_store[key] = value
            # self.key_value_store[key] = value
            self.logs.append({'term': self.term, 'command': f"SET {key} {value}"})
            response = {
                    'status': 'success',
                    'message': f"Value for key '{key}': {value}",
                    'No-response':False
            }
            self.socket.send_json(response)
            # if self.replicate_log_entries()>=((len(self.peers)-1)//2+1):
            #         # self.commit()
            #     response = {
            #         'status': 'success',
            #         'message': f"Value for key '{key}': {value}",
            #         'No-response':False
            #     }
            #     self.socket.send_json(response)
            #     pass
            # else:
            #         response = {
            #             'type':'client_response',
            #             'status':'failure',
            #             'No-response':False
            #         }
            #         self.socket.send_json(response)

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
        print("DEG:",f"Leader id: {self.leader_id}")
        if self.leader_id!=-1:
            self.reset_election_timeout()
        elif self.state != 'leader':
            print(f"Node:{self.node_id} started election")
            self.state = 'candidate'
            self.term += 1
            self.voted_for = self.node_id
            self.vote_count = 1  # Vote for self
            # self.reset_election_timeout()
            self.send_vote_requests()

    def broadcast_leader(self,leader):
        for peer in self.peers:
            if peer != self.node_id:
                request = {
                    'type': 'leader_message',
                    'term': self.term,
                    'leader_id': self.node_id,
                    'No-response':False
                }
                self.send_recv_message(peer, request)


    def handle_leader_message(self, client_socket,request):
        self.leader_id = request.get('leader_id')
        if(self.state == 'candidate'):
            self.state = 'follower'
        print(f"Node {self.node_id} got leader id:{self.leader_id}")
        client_socket.send_json({"response": "ACK", "address": self.address})
        #implement term handling from leader

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
                res = self.send_recv_message(peer, request)
                # print("DEB",res)
                if(res['No-response']==True):
                    print(f"No Response from {peer} in voting")
                elif(res['Vote']=='True'):
                    self.vote_count+=1
        print(f"Node {self.node_id}, vote_cnt {self.vote_count}")
        if(self.vote_count >= len(peers)//2 +1):
        # if(self.vote_count >= 2):
            self.state = 'leader'
            print(f"New Leader is {self.node_id}")
            self.broadcast_leader(self.node_id)


    def send_heartbeat(self):
        dealers=[]
        context = zmq.Context()
        while True:
            if self.state == 'leader':
                for peer in self.peers:
                    if peer != self.node_id:
                        # request = {
                        #     'type': 'heartbeat',
                        #     'term': self.term,
                        #     'leader_id': self.node_id,
                        #     'No-response':False
                        # }
                        request = {
                            'type': 'heartbeat',
                            'term': self.term,
                            'leader_id': self.node_id,
                            'entries': [],
                            'prevLogIndex':self.cur_index[peer],
                            'prevLogTerm':self.logs[self.cur_index[peer]]['term'],
                            'LeaderCommit':self.commit_index
                        }
                        # self.send_message(peer, request)
                        dealer_socket = context.socket(zmq.DEALER)
                        dealer_socket.connect(f"tcp://localhost:555{peer}")
                        dealer_socket.send(b"", zmq.SNDMORE)
                        # dealer_socket.send_multipart([b"", zmq.SNDMORE])
                        dealer_socket.send_json(request)
                        dealers.append(dealer_socket)
            timeout = 2
            poller = zmq.Poller()
            for i in dealers:
                poller.register(i, zmq.POLLIN)
            socks = dict(poller.poll(timeout* 1000*1000))  # Convert timeout to seconds
            response = {"No-response":True}

            for socket in dealers:
                if socket in socks:
                    x = socket.recv(zmq.DONTWAIT)
                    if(x==b""):
                        response = socket.recv_json(zmq.DONTWAIT)
                        print(f"Response from {peer}: {response}")
                # else:
                    # print(f"No response received from {peer} within {timeout} seconds.")

                # print(f"Response from {peer}: {response}")
                socket.close()

            time.sleep(self.heartbeat_interval)


    def send_recv_message(self, peer, message): # send message and w8 2s for response
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://127.0.0.1:555{peer}")
        print("DEB:","Sending & Recv Message")
        socket.send_json(message)
            # response = socket.recv_json()
        # Timer of 
        timeout = 2 #seconds
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        socks = dict(poller.poll(timeout* 1000))  # Convert timeout to milliseconds
        response = {"No-response":True}
        
        if socket in socks:
            response = socket.recv_json()
            print(f"Response from {peer}: {response}")
        else:
            print(f"No response received from {peer} within {timeout} seconds.")

        # print(f"Response from {peer}: {response}")
        socket.close()
        return response

    
    def listen_replication_requests(self,request):
        leader_log_index = request['prevLogIndex']
        leader_log_term = request['prevLogTerm']
        # Getting the leader's term for checking the condition of denial of log replication
        leader_term = request['term']
        # Leader commit index, received as part of communication from the leader
        leader_commit_index = request['LeaderCommit']
        
        if self.term>leader_term:
            print("found error")
            logresults = {
                'type':'append_entries',
                'node_id':self.node_id,
                'success':False
            }
        else:
            curr_log_term=-1
            matching_index=-1

            for i in range(len(self.logs)-1,-1,-1):
                if self.logs[i]['term']==leader_log_term:
                    curr_log_term=self.logs[i]['term']
                    if i==leader_log_index:
                        matching_index=i
                        break
                    else:
                        matching_index=i-1
                        break

            if matching_index==leader_log_index:
                logresults={
                    'type':'append_entries',
                    'node_id':self.node_id,
                    'success':True
                }
                self.logs=self.logs[:matching_index]
                for i in request['entries']:
                    self.logs.append(i)
                print(self.logs)
                # Checking the commit index of the leader is higher than the commit index of the current node
                # Appending the entries starting from the commit index
                if leader_commit_index>self.commit_index:
                    for i in range(self.commit_index,leader_commit_index+1):
                        self.key_value_store[self.logs[i]['key']] = self.logs[i]['value']

                print(f"Node id = {self.node_id}")
                print(f"Key value store = {self.key_value_store}")
                # print(self.)
                # Setting the commit index equal to the commit index of the leader
                self.commit_index = leader_commit_index
            
            else:
                logresults = {
                    'type':'append_entries',
                    'node_id':self.node_id,
                    'term':self.term,
                    'success':False
                }
        self.socket.send_json(logresults)
        print("sent response to leader")

    def replicate_log_entries(self):
        dealers=[]
        context = zmq.Context()
        majority=0
        for i in range(len(self.peers)):
                peer=self.peers[i]
                if peer != self.node_id:
                    # self.store_log_entries()
                    print("Current Index = ",self.cur_index)
                    request = {
                        'type': 'append_entries',
                        'term': self.term,
                        'leader_id': self.node_id,
                        'entries': self.logs[self.cur_index[peer]:],
                        'prevLogIndex':self.cur_index[peer],
                        'prevLogTerm':self.logs[self.cur_index[peer]]['term'],
                        'LeaderCommit':self.commit_index
                    }
                    dealer_socket = context.socket(zmq.DEALER)
                    dealer_socket.connect(f"tcp://localhost:555{peer}")
                    dealer_socket.send(b"", zmq.SNDMORE)
                    # dealer_socket.send_multipart([b"", zmq.SNDMORE])
                    dealer_socket.send_json(request)
                    dealers.append(dealer_socket)

        poller = zmq.Poller()
        for i in dealers:
            poller.register(i, zmq.POLLIN)
        timeout=2
        while(majority<((len(self.peers)-1)//2+1)):
            socks = dict(poller.poll(timeout* 1000))
            if not socks:
                print("Timeout occurred, no incoming messages.")
                for sock in dealers:
                    sock.close()
                break
            for socket in dealers:
                if socket in socks and socks[socket] == zmq.POLLIN:
                    message1 = socket.recv(zmq.DONTWAIT)
                    if message1 == b"":
                        message2=socket.recv_json(zmq.DONTWAIT)
                        print("Received message from:", message2)
                        print("Message content:", message2)
                        if message2['success']==False:
                            # Added the condition of term of the node being greater than the term of the leader
                            node_term = message2['term']
                            # term of the node is greater than the term of the leader
                            if node_term>self.term:
                                self.status="folllower"
                                self.voted_for=None
                                self.reset_election_timeout()
                                self.term = node_term
                                break
                            else:
                                self.cur_index[message2['node_id']]-=1
                                socket.send(b"", zmq.SNDMORE)
                                request = {
                                        'type': 'append_entries',
                                        'term': self.term,
                                        'leader_id': self.node_id,
                                        'entries': self.logs[self.cur_index[peer]:],
                                        'prevLogIndex':self.cur_index[peer],
                                        'prevLogTerm':self.logs[self.cur_index[peer]]['term'],
                                        'LeaderCommit':self.commit_index
                                }
                                # socket.send_json(request)
                        else: # If success was received
                            # peer_index = message2['ack']  
                            # self.cur_index[message2['node_id']] = peer_index
                            majority+=1
            print("Majority:",majority)
        # Checking for the role and the term number
        # if self.state=='leader' and self.term==
        print("logs updated")
        return majority
       
                
    def run(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(self.address)

        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()
       
        # if(self.node_id == 0): #TEMp
        #     self.state = 'leader'
        #     heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        #     heartbeat_thread.start()

        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.start()

        while True:
            print("DEB:","Listening State")
            message = self.socket.recv_json()
            print(message)
            try:
                if message['type'] == 'heartbeat':
                    self.handle_heartbeat(message)
                    self.socket.send_json({"response": "SUC", "address": self.address})
                elif message['type'] == 'request_vote':
                    self.handle_vote_request(self.socket,message)
                    # self.socket.send_json({"response": "SUC", "address": self.address})
                elif message['type'] == 'client_request':
                    # self.handle_client_request(self.socket,message)
                    self.cur_index={i:len(self.logs)-1 for i in self.peers}
                    self.handle_client_request(self.socket,message)
                elif message['type'] == 'leader_message':
                    self.handle_leader_message(self.socket,message) 
                
                elif message['type'] == 'append_entries':
                        print("got")
                        self.listen_replication_requests(message)   
            except zmq.ZMQError as e:
                print(e,message['type'])
               

if __name__ == "__main__":
    node_id = int(input("Enter Node ID: "))
    server_address = f"tcp://127.0.0.1:555{node_id}"
    print(f"Node Listening at {server_address}")
    peers = [0, 1,2]  # Assuming 5 nodes
    node = RaftNode(node_id, server_address, peers)
    node.run()