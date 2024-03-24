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
        node.leader_address="-1"
        # if self.leader
        vote_response = raft_pb2.VoteResponse()
        vote_response.nodeAddress = node.address
        vote_response.term = node.term
        
        if node.term>request.term:
            vote_response.voteGranted=False
            return vote_response

        elif request.term==node.term:
            if node.prevLogIndex<=request.lastLogIndex:
                if node.voted_for==request.candidateAddress or node.voted_for==None :
                    node.voted_for=request.candidateAddress
                    vote_response.voteGranted = True
                    
                else:
                    vote_response.voteGranted=False
                return vote_response
            else:
                vote_response.voteGranted=False
                return vote_response
        else:
            node.term=request.term
            node.voted_for=request.candidateAddress
            vote_response.voteGranted=True
            return vote_response

       
            
        # return vote_response
        
    def AppendEntries(self, request, context):
        # if len(request.entries)==0:
            node.voted_for=None#handle this
            node.leader_address = request.leaderAddress

            print(f"Received heartbeat from leader {request.leaderAddress}")
            
            node.election_time=1

        # else:
            print("Received entries")
            print("leaderlogindex",request.prevLogIndex)
            print("leaderlogterm",request.prevLogTerm)
            leader_log_index = request.prevLogIndex
            leader_log_term = request.prevLogTerm
            # Getting the leader's term for checking the condition of denial of log replication
            leader_term = request.term
            # Leader commit index, received as part of communication from the leader
            leader_commit_index = request.leaderCommit
            
            if node.term>leader_term:
                print("found error")
                # logresults = {
                #     'type':'append_entries',
                #     'node_id':self.node_id,
                #     'term':self.term,
                #     'success':False
                # }
                response=raft_pb2.AppendEntriesResponse()
                response.term = node.term
                response.success=False
                response.nodeAddress = node.address
                return response
                
            else:
                print("here")
                curr_log_term=node.prevLogTerm
                matching_index=node.prevLogIndex
                if len(node.logs)>0:
                    print(node.logs)
                for i in range(len(node.logs)-1,-1,-1):
                    if node.logs[i]['term']==leader_log_term:
                        # {'term': 0, 'command': 'SET', 'key': 0, 'value': 'hello'}
                        curr_log_term=node.logs[i]['term']
                        if i==leader_log_index:
                            matching_index=i
                            break
                        else:
                            matching_index=i-1
                            break

                if matching_index==leader_log_index:
                    # print("matching")
                    # logresults={
                    #     'type':'append_entries',
                    #     'node_id':node.node_id,
                    #     'term':node.term,
                    #     'success':True
                    # }
                    response=raft_pb2.AppendEntriesResponse()
                    response.term = node.term
                    response.success=True
                    response.nodeAddress = node.address
 
                    if matching_index!=-1:
                        node.logs=node.logs[:matching_index+1]
                    else:
                        node.logs=[]
                    # entries =request.entries
                    for i in request.entries:
                        node.logs.append({"term":i.term,"command":i.operation,"key":i.key,"value":i.value})
                    print("appended: ",node.logs)
                    if len(node.logs)>0:
                        node.prevLogIndex = len(node.logs)-1
                        node.prevLogTerm = node.logs[-1]['term']
                    
                    if len(request.entries)==0:
                        node.handle_commit_requests(leader_commit_index)
                    # Dump Point-10
                    # self.dump_data(f"Node {node.node_id} accepted AppendEntries RPC from {node.leader_id}")
                    # print("Logs after appending = ",self.logs)
                else:
                    #logresults = {
                    # 'type':'append_entries',
                    # 'node_id':self.node_id,
                    # 'term':self.term,
                    # 'success':False
                        
                    response=raft_pb2.AppendEntriesResponse()
                    response.term = node.term
                    response.success=False
                    response.nodeAddress = node.address

                    # Dump Point-11
                    # self.dump_data(f"Node {node.node_id} rejected AppendEntries RPC from {node.leader_id}")
            
            print("sent response to leader")
            return response
        
class ClientCommunicationService(raft_pb2_grpc.ClientCommunicationServicer):
    def ServeClient(self,request,context):
        if node.state != 'leader':
            print("error",node.leader_address)
            response=raft_pb2.ServeClientReply()
            response.Data=""
            response.leaderAddress=node.leader_address
            response.Success=False
            print(f"In the leader area with id = ")
            return response
        
        else:
            request=json.loads(request.Request)
            request_type = request.get('sub-type')
            key = request.get('key')
            value = request.get('value')
            print(node.logs)
            print("after")
            if request_type == 'SET':
                node.logs.append({'term': node.term, 'command': 'SET','key': key, 'value': f'{value}'})
                print(node.logs)
                node.heartbeat_pause==1
                node.cur_index={i:len(node.logs)-1 for i in node.peers}
                time.sleep(node.heartbeat_interval)
                while node.majority<((len(node.peers)-1)//2+1):
                    time.sleep(node.heartbeat_interval)
                print("append1")
                response=raft_pb2.ServeClientReply()
                response.Data=""
                response.leaderAddress=node.leader_address
                response.Success=True
                
                # else:
                #     print("append2")
                #     response=raft_pb2.ServeClientReply()
                #     response.Data=""
                #     response.leaderAddress=node.leader_address
                #     response.Success=False

                print("response returned to client")
                return response
    
            elif request_type == 'GET':
                # print(f"Received GET request for key '{key}'")
                # print(key,self.key_value_store.keys(),key in self.key_value_store.keys())
                if key in node.key_value_store.keys():
                    value = node.key_value_store[key]
                    data={
                        'key':key,
                        'value':value,
                    
                    }
                    response=raft_pb2.ServeClientReply()
                    response.Data=json.dumps(data)
                    print("HERE HELLo")
                    response.leaderAddress=node.leader_address
                    response.Success=True

                else:
                    response=raft_pb2.ServeClientReply()
                    response.Data=""
                    response.leaderAddress=node.leader_address
                    response.Success=True
                return response
                
class RaftNode:
    def __init__(self, node_id, address, peers):
        self.majority=1
        self.node_id = node_id
        self.address = address
        self.peers = peers
        self.state = 'follower'
        self.leader_id = -1
        self.leader_address="-1"
        self.client_majority=1
        self.term = 0
        self.vote_count = 0
        self.voted_for = None
        self.socket = None
        self.election_time=0
        self.election_timeout = 5  # Election timeout in seconds
        #TEMP
        self.heartbeat_pause=0
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
        self.prevLogIndex=-1
        self.prevLogTerm=-1
        self.leasetime = 2 #Lease time in sec
        self.logs_path = os.path.join(os.getcwd(),f'logs_node_{self.node_id}')
        self.cur_index={i:len(self.logs) for i in self.peers}
        self.lease_timer= -1
        self.node_address=address

    def handle_commit_requests(self,leader_commit_index):
        print("Logs = ",self.logs)
        commit_index = self.commit_index
        if leader_commit_index>self.commit_index:
            for i in range(commit_index+1,leader_commit_index+1):
                print("Value of i = ",i)
                self.commit_index+=1
                self.key_value_store[self.logs[i]['key']] = self.logs[i]['value']
                self.last_applied+=1
            print(f"Commit Index of the node {self.node_id} {self.commit_index}")
        else:
            print("No change in commit index needed")
        print(self.key_value_store)

    def commit_log_entries(self):
        commit_index = self.commit_index
        for i in range(commit_index+1,len(self.logs)):
            self.commit_index+=1
            self.key_value_store[self.logs[i]['key']] = self.logs[i]['value']
            self.last_applied+=1

        # self.write_logs()   
        print(f"Commit index of the leader with id {self.leader_id} {self.commit_index}")
        # self.write_metadata(
        print(self.key_value_store)
    
    def start_election(self):
        print("DEG:",f"Current Leader id: {self.leader_address}")
        
        if self.election_time==1:#reset election timer
            self.election_time=0
            time.sleep(self.election_timeout)
            self.start_election()

        else:#start election
            # Dump Point 4
            # self.dump_data(f"Node {self.node_id} election timer timed out, Starting election.")
            print(f"Node:{self.node_id} started election")
            self.leader_address="-1"
            self.vote_count=0
            self.state = 'candidate'
            self.term += 1
            self.voted_for = self.node_id
            self.vote_count += 1  # Vote for self  
            self.send_vote_requests()
            if self.vote_count>= (len(peers))//2 +1:
                self.state = 'leader'
                self.leader_id = self.node_id
                self.leader_address=self.node_address
                self.appendthread=threading.Thread(target=self.append_entries())
                self.appendthread.daemon=True
                self.appendthread.start()   
            else:
                self.state=='follower'
                time.sleep(self.election_timeout)
                self.start_election()
                                
    def send_vote_requests(self):
        
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
                print("Node Crashed",e)
                
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
        while time.time() - start_time < self.election_timeout:  # Poll for a maximum of timeout seconds
            if(self.vote_count >= (len(peers))//2 +1):
            # Dump Point-5
                # self.dump_data(f"Node {self.node_id} became the leader for term {self.term}") 
                self.state = 'leader'
                self.leader_id = self.node_id
                self.leader_address=self.node_address
                print(f"New Leader is {self.node_id}")
                break

            time.sleep(0.1) # Poll every 0.1 second
        print("votes",self.vote_count)
        return

    def append_entries(self):
        if self.state != 'leader':
            return 
        self.majority=1
        timeout=self.heartbeat_interval
        nodecheck={i:0 for i in self.peers}
        def callback(response):
            nonlocal nodecheck
            try:#detect hearbeat ack
                response_received = response.result(timeout=timeout)
                print(response_received)
                print(f"append entries ACK received from {response_received.nodeAddress} with term {response_received.term}")
                if response_received.success==True:
                    self.cur_index[response_received.nodeAddress]=len(self.logs)
                else:
                    self.cur_index[response_received.nodeAddress]-=1
                    if self.cur_index[response_received.nodeAddress]<0:
                        self.cur_index[response_received.nodeAddress]=0
                nodecheck[response_received.nodeAddress]=1
                self.majority+=1  
                return True

            except grpc.FutureTimeoutError:
                print("Timeout occured received no response ")
                return False

            except grpc.RpcError as e:#detect node failure
                print("Node Crashed",e)
                return False

        # cur_index2={i:len(self.logs)-1 for i in self.peers}
        print("curs: ",self.cur_index)
        for peer in self.peers:
            if peer!=self.address:
                node_channel=grpc.insecure_channel(peer)
                stub=raft_pb2_grpc.NodeCommunicationStub(node_channel)
                request = raft_pb2.AppendEntriesRequest()
                request.term=self.term
                request.leaderAddress=self.node_address

                logentries=raft_pb2.entry()

                for i in range(self.cur_index[peer],len(self.logs)):
                    log=self.logs[i]
                    logentries.term=log['term']
                    logentries.operation=log['command']
                    logentries.key=log['key']
                    logentries.value=log['value']
                    request.entries.append(logentries)

                print("sending ",request.entries,"to",peer)
                request.prevLogIndex=(self.cur_index[peer]-1)
                request.prevLogTerm= (self.logs[self.cur_index[peer]-1]['term'] if request.prevLogIndex>=0 else -1)
                print("error her",request.prevLogIndex,request.prevLogTerm)
                request.leaderCommit=self.commit_index
                response=stub.AppendEntries.future(request)
                response.add_done_callback(callback)
                
        
        start_time = time.time()
        while time.time() - start_time < timeout:  # Poll for timeout seconds
            time.sleep(0.1) # Poll every 0.1 second

        if self.majority>= (len(peers))//2 +1:#leader remains
            print(f"Node {self.node_id} got heartbeats {self.majority} till now")

        self.append_entries()    

        
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
    server.start()
    node.election_timer = threading.Timer(node.election_timeout,node.start_election)
    node.election_timer.daemon = True
    node.election_timer.start()
    
    
    server.wait_for_termination()

if __name__ == "__main__":
    node_id = int(input("Enter Node ID: "))
    server_address = f"127.0.0.1:555{node_id}"
    print(f"Node Listening at {server_address}")
    peers = ["127.0.0.1:5550", "127.0.0.1:5551","127.0.0.1:5552"]  # Assuming 5 nodes
    serve()




