import threading
import time
import sys
import os
import json

import concurrent
import grpc
import raft_pb2
import raft_pb2_grpc

class RaftClient:
    def __init__(self, client_id, server_address):
        self.client_id = client_id
        self.server_address = server_address
        
        self.nodes = ['127.0.0.1:55550','127.0.0.1:55551','127.0.0.1:55552','127.0.0.1:55553','127.0.0.1:55554']


        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = raft_pb2_grpc.ClientCommunicationStub(self.channel)

    def change_socket_connection(self,address=None):
        self.channel.close()
        self.channel=(grpc.insecure_channel(address) if address!=None else grpc.insecure_channel(self.server_address))
        self.stub = raft_pb2_grpc.ClientCommunicationStub(self.channel)

    def check_every_node(self,request_type,key,value=None):
        request = raft_pb2.ServeClientArgs()
        if request_type=='SET':
            request_dict = {
                'client_id':self.client_id,
                'sub-type':request_type,
                'key':key,
                'value':value
            }
            request.Request = json.dumps(request_dict)
            i=0
            while i<len(self.nodes):
                try:
                    print(f"Contacting the node with address : {self.nodes[i]}")
                    self.change_socket_connection(self.nodes[i])
                    response = self.stub.ServeClient(request)
                    status = response.Success
                    if status==True:
                        print(f"{response.Data}")
                        return True
                    else:
                        if response.leaderAddress=="-1":
                            print("There is no leader elected in the database and hence the operation failed")
                            return True
                        else:
                            print(f"SET Command failed, retrying and contacting the new leader with id : {response.leaderAddress}....")
                            self.leaderAddress=response.leaderAddress
                            self.send_set_request(key,value)
                            return True
                except grpc.RpcError as e :
                    print(f"The node {self.nodes[i]} is down hence contacting the next node")
                    i+=1
            return False
    
        else:
            request_dict = {
                'client_id':self.client_id,
                'sub-type':request_type,
                'key':key
            }
            request.Request = json.dumps(request_dict)
            i=0
            while i<len(self.nodes):
                try:
                    print(f"Contacting the node with address : {self.nodes[i]}")
                    response = self.stub.ServeClient(request)
                    status = response.Success
                    if status==True:
                        if response.Data!="":
                            print(f"The key {key} does not exist in the key value store")
                            return True
                        else:
                            if response.Data=="":
                                print(f"The key {key} does not exist in the key value store")
                            else:
                                response = json.loads(response.Data)
                                print("Response = ",response)
                                print(f"Value for key {response['key']} = value {response['value']}")
                            return True
                    else:
                        if response.leaderAddress=="-1":
                            print("There is no leader elected in the database and hence the operation failed")
                            return True
                        else:
                            print(f"GET Command failed, retrying and contacting the new leader with id : {response.leaderAddress}....")
                            self.leaderAddress=response.leaderAddress
                            self.send_get_request(key)
                            return True
                except grpc.RpcError as e:
                    print(f"The node {self.nodes[i]} is down hence contacting the next node")
                    i+=1
                    if i<len(self.nodes):
                        self.change_socket_connection(self.nodes[i])
            return False

        


    def send_set_request(self,key=None,value=None):
        request=raft_pb2.ServeClientArgs()
        request_dict={
            'client_id':self.client_id,
            'sub-type':'SET',
            'key':key,
            'value':value
        }
        request.Request = json.dumps(request_dict)
        found = True
        try:
            response = self.stub.ServeClient(request)
            status = response.Success
            while status!=True:
                leaderAddress = response.leaderAddress
                if leaderAddress=="-1":
                    found = False 
                    break
                else:
                    print(f"SET Command failed, retrying and contacting the new leader with id : {leaderAddress}....")
                    self.server_address = response.leaderAddress
                    self.change_socket_connection()
                    response = self.stub.ServeClient(request)
                    status = response.Success
            
            if found==True:
                 print(f"{response.Data}")
            else:
                print(f"There is no leader elected in the database and hence the operation failed")

        except grpc.RpcError as e:
            print(f"Error occured : ")
            return False
       
    # def send_set_request
    def send_get_request(self,key=None):
        request=raft_pb2.ServeClientArgs()
        request_dict={
            'client_id':self.client_id,
            'type':"client_request",
            'sub-type':'GET',
            'key':key
        }
        request.Request = json.dumps(request_dict)
        found = True
        try:
            response = self.stub.ServeClient(request)
            status = response.Success
            while status!=True:
                leaderAddress = response.leaderAddress
                if leaderAddress=="-1":
                    found = False 
                    break
                else:
                    print(f"GET Command failed, retrying and contacting the new leader with id : {leaderAddress}....")
                    self.server_address = response.leaderAddress
                    self.change_socket_connection()
                    response = self.stub.ServeClient(request)
                    status = response.Success
            if found==True:
                if response.Data=="":
                    print(f"The key {key} does not exist in the key value store")
                else:
                    response = json.loads(response.Data)
                    print("Response = ",response)
                    print(f"Value for key {response['key']} = value {response['value']}")
            else:
                print(f"There is no leader elected in the database and hence the operation failed")
              
        except grpc.RpcError as e:
            print(f"Error occured : ")
            return False

    def run(self):
        while True:
            print("\nOptions:")
            print("1. SET")
            print("2. GET")
            print("3. Exit")

            
            choice = input("Enter your choice: ")

            if choice == '1':
                key = input("Enter key: ")
                value = input("Enter value: ")
                success = self.send_set_request(key, value)
                if success==False:
                    success_every = self.check_every_node('SET',key,value)
                    if success_every==False:
                        print("All the nodes are crashed and the system is down hence the operation cannot be completed")
            elif choice == '2':
                key = input("Enter key: ")
                success = self.send_get_request(key)
                if success==False:
                    success_every = self.check_every_node('GET',key)
                    if success_every==False:
                        print("All the nodes are crashed and the system is down hence the operation cannot be completed")
            elif choice == '3':
                client.close_connection()
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
       

    def close_connection(self):
        self.channel.close()
        
if __name__ == "__main__":
    client_id = int(input("Enter Client ID: "))
    server_address = "127.0.0.1:55551"
    client = RaftClient(client_id, server_address)
    client.run()