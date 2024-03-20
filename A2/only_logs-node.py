import zmq
import json
import threading
import time
import sys
import signal
import os
import json


signal.signal(signal.SIGINT, signal.SIG_DFL)

class RaftNode:
    def __init__(self, node_id, address, peers):
        self.node_id = node_id
        self.address = address
        self.peers = peers
        self.state = 'follower'
        self.leader_id = 1
        self.leader_address=f"tcp://127.0.0.1:555{self.leader_id}"
        self.term = 0
        self.vote_count = 0
        self.voted_for = None
        self.socket = None
        self.election_timeout = 5  # Election timeout in seconds
        #TEMP
        if(self.node_id==0):
            self.election_timeout = 5
        elif(self.node_id==1):
            self.election_timeout = 9
        elif(self.node_id==2):
            self.election_timeout = 10
        #TEMP
        self.heartbeat_interval = 1  # Heartbeat interval in seconds
        self.logs = []#List of (logterm,value)    
        # Denotes the index of the entry of the log last commited succesfully  
        self.commit_index = -1
        self.last_applied = 0
        self.key_value_store = {}
        self.prevLogIndex=0
        self.prevLogTerm=0
        self.leasetime = 40 #Lease time in sec
        self.logs_path = os.path.join(os.getcwd(),f'logs_node_{self.node_id}')
        # self.cur_index={}
        
        if self.node_id == 0:
            # self.state = 'leader'
            # self.leader_id = self.node_id
            self.logs = [{'term': 0, 'command': "SET",'key':"0",'value':"M1"},{'term':0,'command':'SET','key':"1",'value':"M2"},{'term':1,'command':'SET','key':"2",'value':"M3"}]
            # self.logs=[(0,"hello"),(0,"world"),(1,"gg")]
            self.key_value_store = {"0":"M1","1":"M2","2":"M3"}
            self.commit_index=0
            self.term = 1

        elif self.node_id ==1:
            self.logs = [{'term': 0, 'command': "SET",'key':'0','value':"M1"},{'term':0,'command':'SET','key':'1','value':"M2"}]
            self.key_value_store={'0':"M1",'1':"M2"}
            self.term  = 0
            self.commit_index=-1
        
        self.cur_index={i:len(self.logs)-1 for i in self.peers}

    def dump_data(self,data):
        # with open(f"{self.logs_path}/dump.txt","w") as f:
        #     f.write(data)
        pass#TEMP

    def write_metadata(self):
        # with open(self.logs_path+"/metadata.json","w") as f:
        #     metadata={
        #         'Commit-Length':self.commit_index+1,
        #         'Term':self.term,
        #         'Voted For':self.voted_for
        #     }
        #     json_object = json.dumps(metadata,indent=4)
        #     f.write(json_object)
        pass#TEMP

    def write_logs(self):
        # with open(self.logs_path+"/logs.json","w") as f:
        #     json_object = json.dumps(self.logs,indent=4)
        #     f.write(json_object)
        # Function for handling the commiting entries at each heartbeat
        pass#Temp
    def handle_commit_requests(self,leader_commit_index):
        # leaderIndex = message["LeaderCommit"]
        print("Logs = ",self.logs)
        if leader_commit_index >self.commit_index:
            for i in range(self.commit_index+1,leader_commit_index):
                print("Value of i  = ",i)
                self.key_value_store[self.logs[i]['key']] = self.logs[i]['value']
                # Dump Point-7 
                self.dump_data(f"Node {self.node_id} (follower) commited the entry {self.logs[i]['command']} {self.logs[i]['key']}  : {self.logs[i]['value']} to the state machine.")
    
            self.write_logs()
            self.commit_index = leader_commit_index
            print(f"Commit Index of the node {self.node_id} is:{self.commit_index}")
            self.write_metadata()
        # Else clause just for the sake of debugging
        else:
            print("No change in commit index needed")

    def commit_log_entries(self):
        if self.state == 'leader':
            print("Commiting log entries in the leader")
            for i in range(self.commit_index+1,len(self.logs)):
                self.key_value_store[self.logs[i]['key']] = self.logs[i]['value']
                # Dump Point-9
                self.dump_data(f"Node {self.node_id} (leader) commited the entry {self.logs[i]['command']} {self.logs[i]['key']} {self.logs[i]['value']} to the state machine")
            
            self.write_logs()
            self.commit_index=len(self.logs)-1
            print(f"Leader {self.leader_id} with commit index : {self.commit_index}")
            self.write_metadata()

    def handle_client_request(self, client_socket,request):
        if self.state != 'leader':
            response={
                    'status':'failure',
                    'leaderId':self.leader_id,
                    'No-response':False
            }
            print("DEH",response)
            self.socket.send_json(response)
        else:
            request_type = request.get('sub-type')
            key = request.get('key')
            value = request.get('value')

            if request_type == 'SET':
                print(f"Received SET request for key '{key}' with value '{value}'")
                #TEMP COMMENT
                # Dump Point-8
                self.dump_data(f"Node {self.node_id}(leader) received an {request_type} {key} {value} request")
                #TEMP COMMENT

                if self.state=='leader':     
                    self.key_value_store[key] = value
                # self.key_value_store[key] = value
                
                self.logs.append({'term': self.term, 'command': 'SET','key': key, 'value': f'{value}'})
                replicate_majority = self.replicate_log_entries()
                # print("SET REQ",replicate_majority)
                if replicate_majority>=((len(self.peers)-1)//2+1):
                    # print("DEB","sucess set value")
                    self.commit_log_entries()
                    response = {
                        'status': 'success',
                        'message': f"Value for key '{key}': {value}",
                        'No-response':False
                    }
                    self.socket.send_json(response)
                else:
                    # print("DEB","failure set value")
                    response = {
                        'type':'client_response',
                        'status':'failure',
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
                    self.socket.send_json(response)

    # def reset_election_timeout(self):
    #     self.election_timer.cancel()
    #     self.vote_count=0
    #     self.voted_for = None
    #     self.election_timer = threading.Timer(self.election_timeout, self.start_election)
    #     self.election_timer.start()

    # def reset_lease(self):
    #     self.lease_timer.cancel()
    #     self.vote_count=0
    #     self.lease_timer = threading.Timer(self.lease, self.end_lease)
    #     self.lease_timer.start()
    
    def broadcast_leader(self,leader):
        for peer in self.peers:
            if peer != self.node_id:
                request = {
                    'type': 'leader_message',
                    'term': self.term,
                    'leader_id': leader,
                    'No-response':False
                }
                self.send_recv_message(peer, request)


    def handle_leader_message(self, client_socket,request):
        # print("GOT LEADER HEADER -- \n\n\n\n")
        self.leader_id = request.get('leader_id')
        if(self.state == 'candidate'):
            self.state = 'follower'
        print(f"Node {self.node_id} got leader id:{self.leader_id}")
        client_socket.send_json({"response": "Leader Broadcast ACK", "address": self.address})
        #implement term handling from leader
  
    def send_recv_message(self, peer, message): # send message and w8 2s for response
        context = zmq.Context()
        context.setsockopt(zmq.LINGER, 0)
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

        # print(f"Response from out {peer}: {response}")
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
            print("Leader Term Issue")
            logresults = {
                'type':'append_entries',
                'node_id':self.node_id,
                'term':self.term,
                'success':False
            }
        else:
            curr_log_term=-1
            matching_index=-1

            for i in range(len(self.logs)-1,-1,-1):
                if self.logs[i]['term']==leader_log_term:
                    # {'term': 0, 'command': 'SET', 'key': 0, 'value': 'hello'}
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
                    'term':self.term,
                    'success':True
                }
                if matching_index!=-1:
                    self.logs=self.logs[:matching_index]
                else:
                    self.logs=[]
                for i in request['entries']:
                    self.logs.append(i)

                # Dump Point-10
                self.dump_data(f"Node {self.node_id} accepted AppendEntries RPC from {self.leader_id}")
                print(self.logs)
            else:
                logresults = {
                    'type':'append_entries',
                    'node_id':self.node_id,
                    'term':self.term,
                    'success':False
                }
                # Dump Point-11
                self.dump_data(f"Node {self.node_id} rejected AppendEntries RPC from {self.leader_id}")
        self.socket.send_json(logresults)
        print("sent response to leader")

    def replicate_log_entries(self):
        dealers=[]
        context = zmq.Context()
        context.setsockopt(zmq.LINGER, 0)
        majority=0
        for i in range(len(self.peers)):
                peer=self.peers[i]
                if peer != self.node_id:
                    # self.store_log_entries()
                    print("Current Index = ",self.cur_index)
                    if len(self.logs)>0:
                        request = {
                            'type': 'append_entries',
                            'term': self.term,
                            'leader_id': self.node_id,
                            'entries': self.logs[self.cur_index[peer]:],
                            'prevLogIndex':self.cur_index[peer],
                            'prevLogTerm':self.logs[self.cur_index[peer]]['term'],
                            'LeaderCommit':self.commit_index
                        }
                    else:
                        request = {
                            'type': 'append_entries',
                            'term': self.term,
                            'leader_id': self.node_id,
                            'entries': self.logs,
                            'prevLogIndex':-1,
                            'prevLogTerm':-1,
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
                            node_id = message2['node_id']
                            # term of the node is greater than the term of the leader
                            if node_term>self.term:
                                self.status="follower"
                                self.voted_for=None#Chk Voted for in current term for logs
                                # self.reset_election_timeout()
                                self.broadcast_leader(node_id)
                                self.term = node_term
                                # Dump Point-14
                                self.dump_data(f"{self.node_id} Stepping down")
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
                                socket.send_json(request)
                        else: # If success was received
                            # peer_index = message2['ack']  
                            # self.cur_index[message2['node_id']] = peer_index
                            majority+=1
            print("Majority:",majority)
        # Checking for the role and the term number
        # if self.state=='leader' and self.term==
        print("logs updated")
        return majority
    def print_node_logs(self):
        print("Current Logs:")
        i=1
        for log in self.logs:
            print(i,log)
            i+=1
       
                
    def run(self):
        context = zmq.Context()
        context.setsockopt(zmq.LINGER, 0)
        self.socket = context.socket(zmq.REP)
        self.socket.bind(self.address)
        
       
        if(self.node_id == 1): #TEMp
            self.state = 'leader'
           
        while True:
            print("DEB:","Listening State")
            self.print_node_logs()
            message = self.socket.recv_json()
            print(message)
            try:
                if message['type'] == 'heartbeat':
                    self.handle_heartbeat(message)
                    self.socket.send_json({"response": "HEARTbeat ACK", "address": self.address})
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
    peers = [0, 1]  # Assuming 5 nodes
    node = RaftNode(node_id, server_address, peers)
    node.run()