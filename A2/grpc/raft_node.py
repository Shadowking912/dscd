import threading
import time
import sys
import os
import json

import concurrent
import grpc
import raft_pb2
import raft_pb2_grpc

class NodeCommunicationService(raft_pb2_grpc.NodeCommunicationServicer):
    # def serve_client():
    # def AppendEntr
    # def AppendEntries():

    def RequestVote(self,request,context):
        print(f"Received vote request from candidate {request.candidateAddress}")
        node.leader_address=-1
        # if self.leader
        vote_response = raft_pb2.VoteResponse()
        vote_response.nodeAddress = node.address
        vote_response.term = node.term

        if node.state=='leader':
            vote_response.voteGranted = False
        else:
            vote_response.voteGranted=True
        return vote_response
        
    def AppendEntries(self, request, context):
        print(f"Received append entries request from candidate {request.leaderAddress}")
        node.leader_address = request.leaderAddress
        response = raft_pb2.AppendEntriesResponse()
        response.term = node.term
        response.success=True
        response.nodeAddress = node.address
        return response

        # await 
        
        # if self.state == 'follower':
        #     candidate_term = message['term']
        #     print(self.term,candidate_term)
        #     if self.voted_for is None or self.voted_for == message['candidate_id'] and candidate_term>self.term:
        #         self.voted_for = message['candidate_id']

        #         #TEMP COMMENT
        #         # Dump Point-12
        #         self.dump_data(f"Vote granted for Node {self.voted_for} in term {message['term']}")
        #         #TEMP COMMENT
                
        #         self.reset_election_timeout()

        #         socket.send_json({"Vote":"True",'No-response':False,'node_id':self.node_id})
        #     else:
        #         #TEMP COMMENT
        #         # Dump Point-13
        #         self.dump_data(f"Vote denied for Node {message['candidate_id']} in term {message['term']}")
        #         #TEMP COMMENT
        #         socket.send_json({"Vote":"False",'No-response':False})
        # else:
        #     #TEMP COMMENT
        #     # Dump Point-13
        #     self.dump_data(f"Vote denied for Node {message['candidate_id']} in term {message['term']}")
        #     #TEMP COMMENT
        #     socket.send_json({"Vote":"False",'No-response':False})
            
        
        # self.vote_count = self.broadcast_leader()
        #         # print("DEB",res)
        #         # if(res['No-response']==True):
        #         #     print(f"No Response from {peer} in voting")
        #         # elif(res['Vote']=='True'):
        #         #     self.vote_count+=1
        # print(f"Node {self.node_id}, vote_cnt {self.vote_count}")
        # if(self.vote_count >= (len(peers)-1)//2 +1):
        #     #TEMP COMMENT
        #     # Dump Point-5
        #     self.dump_data(f"Node {self.node_id} became the leader for term {self.term}")
        #     #TEMP COMMENT

        #     self.state = 'leader'
        #     self.leader_id = self.node_id
        #     print(f"New Leader is {self.node_id}")
        #     self.broadcast()

        #     # self.lease_timer = threading.Timer(self.leasetime,self.end_lease)
        #     # self.lease_timer.start()
            
        #     #sending NO_OP
            
        #     #TEMP COMMENT
        #     self.logs.append({'term': self.term, 'command': "NO-OP",'key':None,'value':None})
        #     # Dump Point-1
        #     self.dump_data(f"Leader {self.node_id} sending heartbeat and Renewing Lease")
            
            
        #     heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        #     heartbeat_thread.start()
        # else:
        #     #If not elected Leader due to less votes
        #     print(f"Node {self.node_id} lost election")
        #     self.vote_count=0
        #     self.voted_for=None
        #     self.state = 'follower'
        #     self.term-=1
        #     self.reset_election_timeout()
       
        
    
class ClientCommunicationService(raft_pb2_grpc.ClientCommunicationServicer):
    def __init__(self):
        pass
        # self.client_socket = zmq.Context().socket(zmq.REP)

class RaftNode:
    def __init__(self, node_id, address, peers):
        self.node_id = node_id
        self.address = address
        self.peers = peers
        self.state = 'follower'
        self.leader_id = -1
        self.leader_address=-1
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
        self.leasetime = 2 #Lease time in sec
        self.logs_path = os.path.join(os.getcwd(),f'logs_node_{self.node_id}')
        self.cur_index={i:len(self.logs)-1 for i in self.peers}
        self.lease_timer= -1
        self.node_address=address
        # self.cur_index={}
        
        # if self.node_id == 0:
        #     self.state = 'leader'
        #     self.leader_id = self.node_id
        #     self.logs = [{'term': 0, 'command': "SET",'key':"0",'value':"hello"},{'term':0,'command':'SET','key':"1",'value':"world"},{'term':1,'command':'SET','key':"2",'value':"gg"}]
        #     # self.logs=[(0,"hello"),(0,"world"),(1,"gg")]
        #     self.key_value_store = {"0":"hello","1":"world","2":"gg"}
        #     self.commit_index=0
        #     self.term = 1

        # elif self.node_id ==1:
        #     self.logs = [{'term': 0, 'command': "SET",'key':'0','value':"hello"},{'term':0,'command':'SET','key':'1','value':"world"}]
        #     self.key_value_store={'0':"hello",'1':"world"}
        #     self.term  = 0
        #     self.commit_index=-1
        # elif self.node_id==2:
        #     # self.logs=[(0,"hello"),(1,"y")]
        #     self.logs=[{'term': 0, 'command': "SET",'key':'0','value':"hello"},{'term':1,'command':'SET','key':'1','value':"y"}]
        #     self.key_value_store={'0':"hello",'1':"y"}
        #     self.term = 1
        #     self.commit_index=-1
        
        
        # Creating a directory for the node if it does not already exist for the logs and dumps
        # if os.path.isdir(os.path.join(self.logs_path))==False:
        #     os.makedirs(os.path.join(self.logs_path))
        #     with open(self.logs_path+"/logs.json","w"):
        #         pass
        #     with open(self.logs_path+"/metadata.json","w"):
        #         pass
        #     with open(self.logs_path+"/dump.txt","w"):
        #         pass
        # else:
        # #   Write the functionality for loading the logs list from the text file (TO BE COMPLETED)
        #     # Reading the logs
        #     with open(self.logs_path+"/logs.json","r") as f:
        #         self.logs = json.load(f)
            
        #     # Reading the metadata
        #     with open(self.logs_path+"/metadata.json","r") as f:
        #         metadata  = json.load(f)    
        #         self.commit_index=metadata["Commit-Length"]-1
        #         # self.term = metadata["Term"]
        #         # self.voted_for=metadata["Voted For"]

        #         print("Commit index = ",self.commit_index)
        #         # print("Term = ",self.term)
        #         # print("Voted For = ",self.voted_for)

    def start_election(self):
        print("DEG:",f"Current Leader id: {self.leader_address}")
        if self.leader_address!=-1:
            time.sleep(self.election_timeout)
            self.start_election()
        elif self.state != 'leader':
            # Dump Point 4
            # self.dump_data(f"Node {self.node_id} election timer timed out, Starting election.")
            print(f"Node:{self.node_id} started election")
            self.state = 'candidate'
            self.term += 1
            self.voted_for = self.node_id
            self.vote_count += 1  # Vote for self
                # self.reset_election_timeout()    
            self.send_vote_requests()    
             
    def send_heartbeat(self):
        #TODO: Replicate Leader LOG to each Node
        #ReplicateLog(nodeId, follower )(From Pseudocode)
        # self.dump_data(f"Node {leader} is the New Leader.")
        #TODO: append the record (msg : msg, term : currentTerm) to log (from Pseudocode)
        majority=0
        timeout=self.heartbeat_interval
        def callback_function(response):
            nonlocal majority,timeout
            try:
                response_received = response.result(timeout=timeout)
                print(f"Heartbeat ACK received from {response_received.nodeAddress} with term {response_received.term}")
                if response_received.success==True:
                    majority+=1          
                    print(f"Node {self.node_id} got votes {majority} till now")
            except grpc.FutureTimeoutError:
                print("Timeout occured received no response ")
            except grpc.RpcError as e:
                print("Node Crashed")

        #TODO: Replicate Leader LOG to each Node
        #ReplicateLog(nodeId, follower )(From Pseudocode)
        # self.dump_data(f"Node {leader} is the New Leader.")
        #TODO: append the record (msg : msg, term : currentTerm) to log (from Pseudocode)
        for peer in self.peers:
            if peer!=self.address:
                node_channel=grpc.insecure_channel(peer)
                stub=raft_pb2_grpc.NodeCommunicationStub(node_channel)
                request = raft_pb2.AppendEntriesRequest()
                request.term=self.term
                request.leaderAddress=self.node_address

                logentries=raft_pb2.entry()
                for log in self.logs:
                    logentries.term=log['term']
                    logentries.command=log['command']
                    logentries.key=log['key']
                    logentries.value=log['value']
                    request.entries.append(logentries)

                request.prevLogIndex=(self.cur_index[peer] if len(self.logs)>0 else -1)
                request.prevLogTerm= (self.logs[self.cur_index[peer]['term']] if len(self.logs)>0 else -1)
                request.leaderCommit=self.commit_index
                response=stub.AppendEntries.future(request)
                response.add_done_callback(callback_function)
        
        start_time = time.time()
        while time.time() - start_time < timeout:  # Poll for a maximum of 5 seconds
            if(majority>= (len(peers)-1)//2 +1):

                # self.dump_data(f"Node {self.node_id} became the leader for term {self.term}")

                self.state = 'leader'
                self.leader_id = self.node_id
                print(f"New Leader is {self.node_id}")
                self.broadcast()
                break
            time.sleep(1) # Poll every 0.1 second

        if majority>= (len(peers)-1)//2 +1:
            time.sleep(timeout) 
            self.send_heartbeat()

        else:#lease
            return False

    def send_vote_requests(self):
        self.vote_count=0
        responses=[]
        def callback_function(response):
            try:
                response_received = response.result(timeout=self.election_timeout)
                print(response_received.nodeAddress,response_received.voteGranted)
                if response_received.voteGranted==True:
                    self.vote_count+=1           
                    print(f"Node {self.node_id} got votes {self.vote_count} till now")
            except grpc.FutureTimeoutError:
                print("Timeout occured received no response ")
            except grpc.RpcError as e:
                print("Node Crashed")
                
        for peer in self.peers:
            if peer!=self.address:
                node_channel=grpc.insecure_channel(peer)
                stub=raft_pb2_grpc.NodeCommunicationStub(node_channel)
                vote_request = raft_pb2.VoteRequest()
                vote_request.term = self.term
                vote_request.candidateAddress = self.node_address
                vote_request.lastLogIndex = self.prevLogIndex
                vote_request.lastLogTerm = self.prevLogTerm
                response=stub.RequestVote.future(vote_request)
                response.add_done_callback(callback_function)
        
        start_time = time.time()
        while time.time() - start_time < self.election_timeout:  # Poll for a maximum of 5 seconds
            if(self.vote_count >= (len(peers)-1)//2 +1):
            #TEMP COMMENT
            # Dump Point-5
                # self.dump_data(f"Node {self.node_id} became the leader for term {self.term}")
                #TEMP COMMENT
                self.state = 'leader'
                self.leader_id = self.node_id
                print(f"New Leader is {self.node_id}")
                if not self.send_heartbeat():
                    self.state='follower'
                    break

            time.sleep(1) # Poll every 0.1 second
        if self.state != 'leader':
            print("election failure")
            self.vote_count=0
            self.voted_for = None
            # self.term+=1
            # time.sleep(self.election_timeout)
            self.start_election()
        
        
node = None
def serve():
    global node
    node = RaftNode(node_id, server_address, peers)
    server=grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    client_communication_server=ClientCommunicationService()
    node_communication_server = NodeCommunicationService()
    raft_pb2_grpc.add_NodeCommunicationServicer_to_server(node_communication_server,server)
    raft_pb2_grpc.add_ClientCommunicationServicer_to_server(client_communication_server,server)
    server.add_insecure_port(node.address)
    node.election_timer = threading.Timer(node.election_timeout,node.start_election)
    node.election_timer.daemon = True
    node.election_timer.start()
    
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    node_id = int(input("Enter Node ID: "))
    server_address = f"127.0.0.1:555{node_id}"
    print(f"Node Listening at {server_address}")
    peers = ["127.0.0.1:5550", "127.0.0.1:5551","127.0.0.1:5552"]  # Assuming 5 nodes
    serve()




