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
import signal

class NodeCommunicationService(raft_pb2_grpc.NodeCommunicationServicer):
    # def serve_client():
    # def AppendEntr
    # def AppendEntries():
    def RequestVote(self,request,context):

        print(f"Received vote request from candidate {request.candidateAddress}")
        vote_response = raft_pb2.VoteResponse()
        lock=threading.Lock()
        lock.acquire()
        if node.leader_address!='-1':
            vote_response.voteGranted = False
            vote_response.term = node.term
            vote_response.nodeAddress = node.address
            node.voted_for = None
        else:
            # Dump POINT-13
            # node.dump_data(f"Vote denied for Node {request.candidateAddress} in term {request.term}")
            # return vote_response
            print("received leasee time:",time.monotonic()-node.followerleasestart)
            # Calculating the longestRemainingDuration of the lease known to the node
            if node.longestRemainingLease>0:
                vote_response.longestRemainingDuration = node.longestRemainingLease - (time.monotonic()-node.followerleasestart)
            else:
                vote_response.longestRemainingDuration = 0


            print(f"Term of the node {node.address} is {node.term}")
            print(f"Term of the candidate {request.candidateAddress} is {request.term}")
            print(f"lastlogindex of candidate {request.lastLogIndex}, lastlogindex of node {node.prevLogIndex}, lastlogterm of candidate {request.lastLogTerm}, lastlogterm of node {node.prevLogTerm}")
            if request.term>node.term:
                
                # Dump POINT-13
                # node.dump_data(f"Vote denied for Node {request.candidateAddress} in term {request.term}")

                node.term = request.term
                node.voted_for = None
                node.state="follower"
                
                # vote_response.voteGranted=False
                # return vote_response
            lastTerm = -1
            if len(node.logs)>0:
                lastTerm = node.logs[-1]['term']

            logOk = ((request.lastLogTerm>lastTerm) or ((request.lastLogTerm==lastTerm) and (request.lastLogIndex+1>=len(node.logs))))
            print(f"LogOk : {logOk}, LastTerm : {lastTerm}, candidateTerm : {request.term}, nodeTerm : {node.term}, Length of logs : {len(node.logs)}, Voted For : {node.voted_for}")
            if request.term==node.term and logOk and (node.voted_for==request.candidateAddress or node.voted_for==None):
                node.voted_for = request.candidateAddress
        
                # Dump POINT-12
                node.dump_data(f"Vote granted for Node {request.candidateAddress} in term {request.term}")
                vote_response.voteGranted = True
            
            else:
                # Dump POINT-13
                node.dump_data(f"Vote denied for Node {request.candidateAddress} in term {request.term}")

                vote_response.voteGranted = False

            vote_response.nodeAddress = node.address
            vote_response.term = node.term
            
            if vote_response.voteGranted==True:
                print("election timer reseted")
                node.election_timer.cancel()
                node.election_timer=threading.Timer(node.election_timeout,node.start_election)
                node.election_timer.daemon=True
                node.election_timer.start()
        lock.release()
        return vote_response


        #     if node.prevLogIndex<=request.lastLogIndex:
        #         if node.voted_for==request.candidateAddress or node.voted_for==None :
        #             node.voted_for=request.candidateAddress
                    
                  
                    
        #             vote_response.voteGranted = True
                    
        #         else:
        #             # Dump POINT-13
        #             node.dump_data(f"Vote denied for Node {request.candidateAddress} in term {request.term}")

        #             vote_response.voteGranted=False
        #         return vote_response
        #     else:
      
                
        #         vote_response.voteGranted=False
        #         return vote_response
        # else:
        #     node.term=request.term
        #     node.voted_for=request.candidateAddress
            
            # Dump POINT-12
            # node.dump_data(f"Vote granted for Node {request.candidateAddress} in term {request.term}")

            # vote_response.voteGranted=True
            # return vote_response


        
    def AppendEntries(self, request, context):
        # if len(request.entries)==0:
            # node.voted_for=None #handle this
            
            lock=threading.Lock()
            lock.acquire()
            #=========================TEMPRORAY DUMP=============================================
            node.dump_data(f"GOT Request Term : {request.term} prevLogIndex : {request.prevLogIndex} prevLogTerm : {request.prevLogTerm}")
            leader_log_index = request.prevLogIndex
            leader_log_term = request.prevLogTerm
            # Getting the leader's term for checking the condition of denial of log replication
            leader_term = request.term
            # Leader commit index, received as part of communication from the leader
            leader_commit_index = request.leaderCommit
            
            # if node.term>leader_term:
            #     print("found error")
            #     response=raft_pb2.AppendEntriesResponse()
            #     response.term = node.term
            #     response.success=False
            #     response.nodeAddress = node.address
            #     # Dump POINT-11
            #     node.dump_data(f"Node {node.node_address} rejected AppendEntries RPC from {node.leader_address}")
            #     return response
            
            
            # print(f"Received heartbeat from leader {request.leaderAddress}")
            node.longestRemainingLease=request.leaseTimer
            node.followerleasestart = time.monotonic()
            print("received leader lease remaining:",node.longestRemainingLease)
            # node.longestRemainingLease - node.followerleasestart-time.time()
            
            if node.lease_timer!=-1:
                node.lease_timer.cancel()
            
            node.election_timer.cancel()
            node.election_timer=threading.Timer(node.election_timeout,node.start_election)
            node.election_timer.daemon=True
            node.election_timer.start()

            
        # else:
            print("Received entries : ",request.entries)
            print("leaderlogindex",request.prevLogIndex)
            print("leaderlogterm",request.prevLogTerm)
            

            if leader_term>node.term:
                node.term = leader_term
                node.voted_for=None

                node.write_metadata()


            if leader_term==node.term:
                node.state='follower'
                node.leader_address = request.leaderAddress

                curr_log_term=node.prevLogTerm
                matching_index=node.prevLogIndex
                
                if len(node.logs)>0:
                    print(node.logs)
                for i in range(len(node.logs)-1,-1,-1):
                    if node.logs[i]['term']==leader_log_term:
                        # {'term': 0, 'command': 'SET', 'key': 0, 'value': 'hello'}-
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

                    node.dump_data(f"Node {node.node_address} accepted AppendEntries RPC from {request.leaderAddress}")
                
                    # print("appended: ",node.logs)
                    if len(node.logs)>0:
                        node.prevLogIndex = len(node.logs)-1
                        node.prevLogTerm = node.logs[-1]['term']
                    if len(request.entries)==0:
                        print("In this if condition") 
                        node.handle_commit_requests(leader_commit_index)
                else:
                    # Dump POINT-11
                    node.dump_data(f"Node {node.node_address} rejected AppendEntries RPC from {node.leader_address}")

                    response=raft_pb2.AppendEntriesResponse()
                    response.term = node.term
                    response.success=False
                    response.nodeAddress = node.address
            print("sent response to leader")
            lock.release()
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
                node.dump_data(f"Node {node.node_address} (leader) received an {request_type} for {key} : {value} request")

                log = {'term': node.term, 'command': 'SET','key': key, 'value': f'{value}'}

                node.logs.append(log)
                node.lastLogIndex = len(node.logs)-1
                node.lastLogTerm = node.logs[-1]['term']
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
                node.dump_data(f"Node {node.node_address} (leader) received a {request_type} for {key} request")
               
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
        self.leader_address="-1"
        self.term = 0
        self.voted_for = None
        self.election_timeout = 5  # Election timeout in seconds
        #TEMP
        self.lease_timer= -1
        self.heartbeat_pause=0
        if(self.node_id==0):
            self.election_timeout = 5
        elif(self.node_id==1):
            self.election_timeout = 6
        elif(self.node_id==2):
            self.election_timeout = 7

        elif(self.node_id)==3:
            self.election_timeout = 8

        elif(self.node_id)==4:
            self.election_timeout = 9
        #TEMP
        self.leasetime = 9
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
        self.leasestart=0
        self.followerleasestart=0
        self.longestRemainingLease = 0
        self.election_timer=None
        self.vote_receieved = set()

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
            with open(self.logs_path+"/logs.json","r+") as f:
                try:
                    self.logs = json.load(f)
                except json.JSONEncoder as e:
                    pass

            with open(self.logs_path+"/metadata.json","r") as f:
                try:
                    metadata = json.load(f)
                    self.commit_index = metadata['Commit-Length']-1
                    self.term = metadata["Term"]
                    self.voted_for = metadata["Voted-For"]
                    for i in range(metadata['Commit-Length']):
                        self.last_applied+=1
                        if self.logs[i]['command']!='NO-OP':
                            self.key_value_store[self.logs[i]['key']] = self.logs[i]['value']
                    

                except json.JSONDecodeError as e:
                    pass

            with open(self.logs_path+"/dump.txt","w") as f:
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
          
            
            print(f"Ended Lease for Leader {self.node_address}")
            # DUMP Point-14
            self.dump_data(f"{self.node_address} Stepping down")
            
            self.state = 'follower'
            self.voted_for = None
            self.leader_address = "-1"
            self.leader_id=-1
            self.vote_receieved = set()


    def handle_commit_requests(self,leader_commit_index):
        # Dump POINT-10
        self.dump_data(f"Node {self.node_address} accepted AppendEntries RPC from {self.leader_address}")

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
                    self.dump_data(f"Node {self.node_address} (follower) committed the entry {self.logs[i]['command']} {self.logs[i]['key']} : {self.logs[i]['value']} with term {self.logs[i]['term']} to the state machine")
                else:
                    # Dump POINT-7
                    self.dump_data(f"Node {self.node_address} (follower) committed the entry {self.logs[i]['command']} with term {self.logs[i]['term']} to the state machine.")

            print(f"Commit Index of the node {self.node_address} {self.commit_index}")

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
                self.dump_data(f"Node {self.node_address} (leader) committed the entry {self.logs[i]['command']} {self.logs[i]['key']} : {self.logs[i]['value']} with term {self.logs[i]['term']} to the state machine")

            else:
                # Dump POINT-9
                self.dump_data(f"Node {self.node_address} (leader) committed the entry {self.logs[i]['command']} with term {self.logs[i]['term']} to the state machine.")

        # Writing the metadata to the JSON file
        self.write_metadata()    
            
        print(f"Commit index of the leader with id {self.leader_address} {self.commit_index}")
        print(self.key_value_store)
    
    def start_election(self):
        print("DEG:",f"Current Leader id: {self.leader_address}")
        self.longestRemainingLease-=(time.monotonic()-self.leasestart)
        # Dump POINT-4
        self.dump_data(f"Node {self.node_address} election timer timed out, Starting election.")

        print(f"Node:{self.node_address} started election")
        self.leader_address="-1"
        self.leader_id = -1
        # self.vote_count=0
        self.vote_receieved = set()
        self.vote_receieved.add(self.node_address)
        self.state = 'candidate'
        self.term += 1
        self.voted_for = self.node_address
       

        # self.lastLogTerm = 0
        if len(self.logs)>0:
            self.prevLogTerm = self.logs[-1]["term"]
            self.prevLogIndex = len(self.logs)-1

        # Writing the metadata
        self.write_metadata()

        self.send_vote_requests()

        #===================TEMPORARY DUMP==================
        self.dump_data(f"Received Votes From : {self.vote_receieved}")
        if len(self.vote_receieved)>= (len(peers))//2 +1:


            self.state = 'leader'
            self.leader_id = self.node_id
            self.leader_address=self.node_address
            print("vote lease time received: ",self.longestRemainingLease)

            # Dump POINT-5
            self.dump_data(f"Node {self.node_address} became the leader for term {self.term}.")
            
            if self.longestRemainingLease<self.election_timeout and self.longestRemainingLease>=0:
                time.sleep(self.longestRemainingLease)
          
            # DUMP Point-3
            self.dump_data(f"New Leader waiting for Old Leader lease {self.longestRemainingLease} to timeout.")

            # print("")

            self.lease_timer = threading.Timer(self.leasetime,self.end_lease)
            self.lease_timer.daemon = True
            self.lease_timer.start()
            self.leasestart=time.monotonic()
            
            # Appending NO-OP entry to the log
            log = {'term': self.term, 'command': 'NO-OP','key':"None",'value':"None"}
            self.logs.append(log)
            self.prevLogIndex = len(self.logs)-1
            self.prevLogTerm = self.logs[-1]['term']

            node.cur_index={i:len(node.logs)-1 for i in node.peers}
            
            time.sleep(self.heartbeat_interval)
            # ============================TEMPORARY DUMP===========================================================
            self.dump_data(f"{json.dumps(node.cur_index)}")
            
            print("Logs of leader = ",self.logs)
            self.write_logs()
            
            self.appendthread=threading.Thread(target=self.append_entries)
            self.appendthread.daemon=True
            self.appendthread.start()

            self.election_timer.cancel()
            self.election_timer = threading.Timer(self.election_timeout,self.start_election)
            self.election_timer.daemon = True
            self.election_timer.start()

        else:
            #=========================RANDOM DUMP===============================

            self.dump_data(f"===============ERROR===================================")

            self.state='follower' # ----------------> Issue, == instead of = and also not checking if a leader has been created directly starting election
            self.election_timer.cancel()
            self.election_timer = threading.Timer(self.election_timeout,self.start_election)
            self.election_timer.daemon = True
            self.election_timer.start()                       
    
    def send_vote(self,request,peer_address,stub):
        try:
            response_received = stub.RequestVote(request,timeout=self.election_timeout)
            print(response_received.nodeAddress,response_received.voteGranted)
            if response_received.voteGranted==True and self.state=='candidate' and self.term==response_received.term:
                # self.vote_count+=1      
                self.vote_receieved.add(response_received.nodeAddress)     
                print("received leasetime: ",response_received.longestRemainingDuration)
                # Calculating the longestRemaining Lease received as response to the response of the vote
                self.longestRemainingLease = max(self.longestRemainingLease,response_received.longestRemainingDuration)
                print(f"Node {self.node_address} got votes {len(self.vote_receieved)} till now")
            
            elif response_received.term>self.term:
                self.term = response_received.term
                self.state='follower'
                self.voted_for=None
                self.back=True
            
        except:
            print("node crashed ",peer_address)
            self.dump_data(f"Error occurred while sending RPC to Node {peer_address}")
        
    def send_vote_requests(self):
        self.back=False
                        
        for peer in self.peers:
            if peer!=self.address:
                node_channel=grpc.insecure_channel(peer)
                stub=raft_pb2_grpc.NodeCommunicationStub(node_channel)
                vote_request = raft_pb2.VoteRequest()
                vote_request.term = self.term
                vote_request.candidateAddress = self.node_address
                vote_request.lastLogIndex = self.prevLogIndex
                vote_request.lastLogTerm = self.prevLogTerm
                send_vote_thread=threading.Thread(target=self.send_vote,args=(vote_request,peer,stub))
                send_vote_thread.daemon=True
                send_vote_thread.start()

        start_time = time.time()
        while time.time() - start_time < self.election_timeout:  # Poll for a maximum of timeout seconds
            if(len(self.vote_receieved)>= (len(peers))//2 +1):
                self.state = 'leader'
                self.leader_id = self.node_id
                self.leader_address=self.node_address
                print(f"New Leader is {self.node_id}")
                break
          # Poll for a maximum of timeout seconds
        print(f"Votes Received : {self.vote_receieved}")
        if self.back==True:
            # self.vote_count=1
            self.vote_receieved=set()
            self.vote_receieved.add(self.node_address)
            return
        
        if(len(self.vote_receieved) >= (len(peers))//2 +1):
            self.state = 'leader'
            self.leader_id = self.node_id
            self.leader_address=self.node_address
            print(f"New Leader is {self.node_address}")
            return

    def send_entry(self,request,peer_address,stub):
        try:
            response_received = stub.AppendEntries(request,timeout=self.heartbeat_interval)
            print(f"append entries ACK received from {response_received.nodeAddress}")
            if response_received.success==True:
                print("success",response_received.nodeAddress)
                self.cur_index[response_received.nodeAddress]=len(self.logs)
            else:
                if response_received.term>self.term:
                    print("somebodey else is leader")
                    self.term=response_received.term
                    self.state='follower'
                    self.voted_for=None
                    self.back=True
                else:
                    self.cur_index[response_received.nodeAddress]-=1            
                    if self.cur_index[response_received.nodeAddress]<0:
                        self.cur_index[response_received.nodeAddress]=0        
            
            self.majority+=1
            return True

        except Exception as e:
            print("ERROR2 = ",e)
            # DUMP Point-6
            self.dump_data(f"Error occurred while sending RPC to Node {peer_address}")
            return False

        
        
        
    def append_entries(self):
        if self.state != 'leader':
            return 
        self.majority=1
        lock = threading.Lock()
        lock.acquire()
        # cur_index2={i:len(self.logs)-1 for i in self.peers}
        print("curs: ",self.cur_index)

        #===============================================================TEMPORARY DUMP=====================================
        self.dump_data(f"Cur_Index : {self.cur_index}")
        
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
                self.dump_data(f"Leader {self.leader_address} sending heartbeat & Renewing Lease.")

                print("sending ",request.entries,"to",peer)
                request.prevLogIndex=(self.cur_index[peer]-1)
                request.prevLogTerm= (self.logs[self.cur_index[peer]-1]['term'] if request.prevLogIndex>=0 else -1)
                request.leaderCommit=self.commit_index
                request.leaseTimer=self.leasetime-(time.monotonic()-self.leasestart)
                append_send_thread=threading.Thread(target=self.send_entry,args=(request,peer,stub))
                append_send_thread.daemon=True
                append_send_thread.start()
                
                
                                        
        time.sleep(self.heartbeat_interval)
        lock.release()         

        print("majority",self.majority)
        if self.majority>= (len(self.peers))//2 +1:#leader remains
            print(f"Node {self.node_address} got heartbeats {self.majority} till now")
            self.isLeaseCancel=0
            self.election_timer.cancel()
            self.election_timer=threading.Timer(self.election_timeout,self.start_election)
            self.election_timer.daemon=True
            self.election_timer.start()

            self.lease_timer.cancel()
            self.lease_timer=threading.Timer(self.leasetime,self.end_lease)
            self.lease_timer.daemon=True
            self.lease_timer.start()
            self.leasestart=time.monotonic()

            self.commit_log_entries()
        else:
            # DUMP point-2
            self.dump_data(f"Leader {self.node_address} lease renewal failed.")                
        self.append_entries()

server=None        
node = None

def serve():
    global node,server
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
    print("Server started")
    server.wait_for_termination()

def signal_handler(sig, frame):
    print('Ctrl+C pressed. Closing all sockets...')
    server.stop(0)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    node_id = int(input("Enter Node ID: "))
    server_address = f"127.0.0.1:5555{node_id}"
    print(f"Node Listening at {server_address}")
    peers = ["127.0.0.1:55550", "127.0.0.1:55551","127.0.0.1:55552"]  # Assuming 5 nodes
    
    # peers = ["127.0.0.1:55550", "127.0.0.1:55551","127.0.0.1:55552","127.0.0.1:55553","127.0.0.1:55554"]  # Assuming 5 nodes
    # peer_dict = {"127.0.0.1:5550":0,"127.0.0.1:5551":1,"127.0.0.1:5552":2,"127.0.0.1:5553":3,"127.0.0.1:5554":4}
    # ,"127.0.0.1:5554":4,"127.0.0.1:5555":555}
    serve()



