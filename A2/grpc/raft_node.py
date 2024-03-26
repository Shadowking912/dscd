import threading
import time
import sys
import os
import json

import concurrent
import grpc
import raft_pb2
import raft_pb2_grpc
from functools import partial


class NodeCommunicationService(raft_pb2_grpc.NodeCommunicationServicer):
    # def serve_client():
    # def AppendEntr
    # def AppendEntries():
    def RequestVote(self,request,context):
        print(f"Received vote request from candidate {request.candidateAddress}")
        node.leader_address="-1"
        node.leader_id = -1
        # if self.leader
        vote_response = raft_pb2.VoteResponse()
        vote_response.nodeAddress = node.address
        vote_response.term = node.term

        print("received leasee time:",time.monotonic()-node.followerleasestart)
        # Calculating the longestRemainingDuration of the lease known to the node
        if node.longestRemainingLease>0:
            vote_response.longestRemainingDuration = node.longestRemainingLease - (time.monotonic()-node.followerleasestart)
        else:
            vote_response.longestRemainingDuration = 0

        if node.term>request.term:
            
            # Dump POINT-13
            node.dump_data(f"Vote denied for Node {peer_dict[request.candidateAddress]} in term {request.term}")

            vote_response.voteGranted=False
            return vote_response

        elif request.term==node.term:
            if node.prevLogIndex<=request.lastLogIndex:
                if node.voted_for==request.candidateAddress or node.voted_for==None :
                    node.voted_for=request.candidateAddress
                    
                    # Dump POINT-12
                    node.dump_data(f"Vote granted for Node {peer_dict[request.candidateAddress]} in term {request.term}")
                    
                    vote_response.voteGranted = True
                    
                else:
                    # Dump POINT-13
                    node.dump_data(f"Vote denied for Node {peer_dict[request.candidateAddress]} in term {request.term}")

                    vote_response.voteGranted=False
                return vote_response
            else:
                # Dump POINT-13
                node.dump_data(f"Vote denied for Node {peer_dict[request.candidateAddress]} in term {request.term}")
                
                vote_response.voteGranted=False
                return vote_response
        else:
            node.term=request.term
            node.voted_for=request.candidateAddress
            
            # Dump POINT-12
            node.dump_data(f"Vote granted for Node {peer_dict[request.candidateAddress]} in term {request.term}")

            vote_response.voteGranted=True
            return vote_response

       
            
        # return vote_response
        
    def AppendEntries(self, request, context):
        # if len(request.entries)==0:
            # node.voted_for=None #handle this
            node.leader_address = request.leaderAddress
            node.leader_id = peer_dict[node.leader_address]
            print(f"Received heartbeat from leader {request.leaderAddress}")
            node.longestRemainingLease=request.leaseTimer
            node.followerleasestart = time.monotonic()
            print("received leader lease remaining:",node.longestRemainingLease)
            # node.longestRemainingLease - node.followerleasestart-time.time()
            node.election_timer.cancel()
            node.election_timer=threading.Timer(node.election_timeout,node.start_election)
            node.election_timer.daemon=True
            node.election_timer.start()
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
                # print("found error")
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
                # print("here")
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
                    response=raft_pb2.AppendEntriesResponse()
                    response.term = node.term
                    response.success=True
                    response.nodeAddress = node.address
                    if matching_index!=-1:
                        node.logs=node.logs[:matching_index+1]
                    else:
                        node.logs=[]
                    for i in request.entries:
                        log ={"term":i.term,"command":i.operation,"key":i.key,"value":i.value}
                        node.logs.append(log)

                        # Adding log to the log file
                        node.write_logs()
                
                    # print("appended: ",node.logs)
                    if len(node.logs)>0:
                        node.prevLogIndex = len(node.logs)-1
                        node.prevLogTerm = node.logs[-1]['term']
                    if len(request.entries)==0:
                        print("In this if condition") 
                        node.handle_commit_requests(leader_commit_index)
                else:
                    # Dump POINT-11
                    node.dump_data(f"Node {node.node_id} rejected AppendEntries RPC from {node.leader_id}")

                    response=raft_pb2.AppendEntriesResponse()
                    response.term = node.term
                    response.success=False
                    response.nodeAddress = node.address
            print("sent response to leader")
            return response
        
class ClientCommunicationService(raft_pb2_grpc.ClientCommunicationServicer):
    def ServeClient(self,request,context):
        if node.state != 'leader':
            # print("error",node.leader_address)
            response=raft_pb2.ServeClientReply()
            response.Data=""
            response.leaderAddress=node.leader_address
            response.Success=False
            return response
        
        else:
            request=json.loads(request.Request)
            request_type = request.get('sub-type')
            key = request.get('key')
            value = request.get('value')
            # print(node.logs)
            # print("after")
            if request_type == 'SET':

                # Dump POINT-8
                node.dump_data(f"Node {node.node_id} (leader) received an {request_type} for {key} : {value} request")

                log = {'term': node.term, 'command': 'SET','key': key, 'value': f'{value}'}

                node.logs.append(log)
               
                # Writing the logs to the logs file
                node.write_logs()
               
                print(node.logs)
                node.heartbeat_pause==1
                node.cur_index={i:len(node.logs)-1 for i in node.peers}
                time.sleep(node.heartbeat_interval)
                while node.majority<((len(node.peers)-1)//2+1):
                    time.sleep(node.heartbeat_interval)
                # print("append1")
                response=raft_pb2.ServeClientReply()
                response.Data=""
                response.leaderAddress=node.leader_address
                response.Success=True

                # print("response returned to client")
                return response
    
            elif request_type == 'GET':

                 # Dump POINT-8
                node.dump_data(f"Node {node.node_id} (leader) received a {request_type} for {key} request")
               
                print(f"Received GET request for key '{key}'")
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
        self.election_timeout = 5  # Election timeout in seconds
        #TEMP
        self.lease_timer= -1
        self.heartbeat_pause=0
        if(self.node_id==0):
            self.election_timeout = 3
        elif(self.node_id==1):
            self.election_timeout = 5
        elif(self.node_id==2):
            self.election_timeout = 6
        #TEMP
        self.leasetime = 7
        self.heartbeat_interval = 1  # Heartbeat interval in seconds
        self.logs = []#List of (logterm,value)    
        # Denotes the index of the entry of the log last commited succesfully  
        self.commit_index = -1
        self.last_applied = 0
        self.key_value_store = {}
        self.prevLogIndex=-1
        self.prevLogTerm=-1
         #Lease time in sec
        self.logs_path = os.path.join(os.getcwd(),f'logs_node_{self.node_id}')
        self.cur_index={i:len(self.logs) for i in self.peers}
        self.lease_timer= -1
        self.node_address=address
        self.isLeaseCancel=0
        self.leasestart=0
        self.followerleasestart=0
        self.longestRemainingLease = 0
        self.election_timer=None
        # Creating files for storing metadata and the logs
        if os.path.isdir(os.path.join(self.logs_path))==False:
            os.makedirs(os.path.join(self.logs_path))
            with open(self.logs_path + "/logs.json","w"):
                pass
            with open(self.logs_path+"/metadata.json","w"):
                pass
            with open(self.logs_path+"/dump.txt","w"):
                pass

        else:
        # Write functionlaity for loading the logs list from the text file
            with open(self.logs_path+"/logs.json","r") as f:
                self.logs = json.load(f)

            with open(self.logs_path+"/metadata.json","r") as f:
                try:
                    metadata = json.load(f)
                    self.commit_index = metadata['Commit-Length']-1
                    self.term = metadata["Term"]
                except json.JSONDecodeError as e:
                    pass


    def dump_data(self,data):
        with open(f"{self.logs_path}/dump.txt","a") as f:
            f.write(data)
            f.write("\n")

    def write_metadata(self):
        with open(f"{self.logs_path}/metadata.json","w") as f:
            metadata={
                'Commit-Length':self.commit_index+1,
                'Term':self.term,
                'Voted-For':self.voted_for
            }
            json_object = json.dumps(metadata,indent=4)
            f.write(json_object)

    def write_logs(self):
        with open(self.logs_path+"/logs.json","w") as f:
            json_object = json.dumps(self.logs,indent=4)
            f.write(json_object)
        


    def end_lease(self):
        if(self.state == 'leader'):
            print(f"Ended Lease for Leader {self.node_id}")
            # DUMP Point-14
            self.dump_data(f"{self.node_id} Stepping down")
            
            self.state = 'follower'
            self.voted_for = None
            self.leader_address = "-1"
            self.leader_id=-1
            self.vote_count = 0


    def handle_commit_requests(self,leader_commit_index):
        # Dump POINT-10
        self.dump_data(f"Node {self.node_id} accepted AppendEntries RPC from {self.leader_id}")

        print("Logs = ",self.logs)
        print("Commit Index of the node = ",self.commit_index)
        print("Commit Index of the leader = ",leader_commit_index)
        commit_index = self.commit_index
        if leader_commit_index>self.commit_index:
            for i in range(commit_index+1,leader_commit_index+1):
                self.last_applied+=1
                self.commit_index+=1
                if self.logs[i]['key']!='None':
                    self.key_value_store[self.logs[i]['key']] = self.logs[i]['value']

                    # Dump POINT-7
                    self.dump_data(f"Node {self.node_id} (follower) committed the entry {self.logs[i]['command']} {self.logs[i]['key']} : {self.logs[i]['value']} with term {self.logs[i]['term']} to the state machine")
                else:
                    # Dump POINT-7
                    self.dump_data(f"Node {self.node_id} (follower) committed the entry {self.logs[i]['command']} with term {self.logs[i]['term']} to the state machine.")

            print(f"Commit Index of the node {self.node_id} {self.commit_index}")

            # Writing the metadata to the JSON file
            self.write_metadata()
        else:
            print("No change in commit index needed")
        print(self.key_value_store)

    def commit_log_entries(self):
        print("Inside the commit log entries function")
        commit_index = self.commit_index
        for i in range(commit_index+1,len(self.logs)):
            self.last_applied+=1
            self.commit_index+=1

            if self.logs[i]['key']!='None':
                self.key_value_store[self.logs[i]['key']] = self.logs[i]['value']
                
                # Dump POINT-9
                self.dump_data(f"Node {self.node_id} (leader) committed the entry {self.logs[i]['command']} {self.logs[i]['key']} : {self.logs[i]['value']} with term {self.logs[i]['term']} to the state machine")

            else:
                # Dump POINT-9
                self.dump_data(f"Node {self.node_id} (leader) committed the entry {self.logs[i]['command']} with term {self.logs[i]['term']} to the state machine.")

        # Writing the metadata to the JSON file
        self.write_metadata()    
            
        print(f"Commit index of the leader with id {self.leader_id} {self.commit_index}")
        print(self.key_value_store)
    
    def start_election(self):
        print("DEG:",f"Current Leader id: {self.leader_address}")
        self.longestRemainingLease-=(time.monotonic()-self.leasestart)
        # Dump POINT-4
        self.dump_data(f"Node {self.node_id} election timer timed out, Starting election.")

        print(f"Node:{self.node_id} started election")
        self.leader_address="-1"
        self.leader_id = -1
        self.vote_count=0
        self.state = 'candidate'
        self.term += 1
        self.voted_for = self.node_id
        self.vote_count += 1  # Vote for self  

        # Writing the metadata
        self.write_metadata()
        self.send_vote_requests()
        if self.vote_count>= (len(peers))//2 +1:

            # Dump POINT-5
            self.dump_data(f"Node {self.node_id} became the leader for term {self.term}.")

            self.state = 'leader'
            self.leader_id = self.node_id
            self.leader_address=self.node_address
            print("vote lease time received: ",self.longestRemainingLease)
            time.sleep(self.longestRemainingLease)

            # DUMP Point-3
            self.dump_data(f"New Leader waiting for Old Leader lease to timeout.")

            self.lease_timer = threading.Timer(self.leasetime,self.end_lease)
            self.lease_timer.daemon = True
            self.lease_timer.start()
            self.leasestart=time.monotonic()
            
            # Appending NO-OP entry to the log
            log = {'term': self.term, 'command': 'NO-OP','key':"None",'value':"None"}
            self.logs.append(log)
            print("Logs of leader = ",self.logs)
            self.write_logs()
            
            self.appendthread=threading.Thread(target=self.append_entries)
            self.appendthread.daemon=True
            self.appendthread.start()

            self.election_timer = threading.Timer(self.election_timeout,self.start_election)
            self.election_timer.daemon = True
            self.election_timer.start()

        else:
            # DUMP point-2
            self.dump_data(f"Leader {self.node_id} lease renewal failed. Stepping Down.")

            self.state='follower' # ----------------> Issue, == instead of = and also not checking if a leader has been created directly starting election
            self.election_timer = threading.Timer(self.election_timeout,self.start_election)
            self.election_timer.daemon = True
            self.election_timer.start()                       
    
    def send_vote_requests(self):
        
        def callback_function(response):
            try:
                response_received = response.result(timeout=self.election_timeout)
                print(response_received.nodeAddress,response_received.voteGranted)
                if response_received.voteGranted==True:
                    self.vote_count+=1           
                    print("received leasetime: ",response_received.longestRemainingDuration)
                    # Calculating the longestRemaining Lease received as response to the response of the vote
                    self.longestRemainingLease = max(self.longestRemainingLease,response_received.longestRemainingDuration)
                    print(f"Node {self.node_id} got votes {self.vote_count} till now")
            except grpc.FutureTimeoutError:
                print("Timeout occured received no response ")
            except grpc.RpcError as e:
                # DUMP Point-6
                # self.dump_data(f"Error occurred while sending RPC to Node {peer_id}")
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

                # creating a partial function with arguments
                response.add_done_callback(callback_function)

        
        start_time = time.time()
        while time.time() - start_time < self.election_timeout:  # Poll for a maximum of timeout seconds
            if(self.vote_count >= (len(peers))//2 +1):
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
                # print(response_received)
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

                # DUMP POINT-1
                self.dump_data(f"Leader {self.leader_id} sending heartbeat & Renewing Lease.")

                print("sending ",request.entries,"to",peer)
                request.prevLogIndex=(self.cur_index[peer]-1)
                request.prevLogTerm= (self.logs[self.cur_index[peer]-1]['term'] if request.prevLogIndex>=0 else -1)
                print("error her",request.prevLogIndex,request.prevLogTerm)
                request.leaderCommit=self.commit_index
                request.leaseTimer=self.leasetime-(time.monotonic()-self.leasestart)
                print("sent lease timer: ",request.leaseTimer)
                response=stub.AppendEntries.future(request)
                response.add_done_callback(callback(response))
                
        
        start_time = time.time()
        while time.time() - start_time < timeout:  # Poll for timeout seconds
            time.sleep(0.1) # Poll every 0.1 second
        
        print("majority",self.majority)
        if self.majority>= (len(peers))//2 +1:#leader remains
            print(f"Node {self.node_id} got heartbeats {self.majority} till now")
            self.isLeaseCancel=0
            self.election_timer.cancel()
            self.election_timer=threading.Timer(self.election_timeout,self.start_election)
            self.election_timer.daemon=True
            self.election_timer.start()
            node.lease_timer.cancel()
            node.lease_timer=threading.Timer(self.leasetime,self.end_lease)
            node.lease_timer.daemon=True
            node.lease_timer.start()
            self.leasestart=time.monotonic()
            self.commit_log_entries()
        else:
            print("node didnt get majority")
            self.isLeaseCancel=1  

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
    peer_dict = {"127.0.0.1:5550":0,"127.0.0.1:5551":1,"127.0.0.1:5552":2}
    serve()




