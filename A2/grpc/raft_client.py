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

        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = raft_pb2_grpc.ClientCommunicationStub(self.channel)

    def change_socket_connection(self):
        self.channel.close()
        self.channel=grpc.insecure_channel(self.server_address)
        self.stub = raft_pb2_grpc.ClientCommunicationStub(self.channel)

      
    def send_set_request(self,request_type,key=None,value=None):
        request=raft_pb2.ServeClientArgs()
        request_dict={
            'client_id':self.client_id,
            'sub-type':request_type,
            'key':key,
            'value':value
        }
        request.Request = json.dumps(request_dict)
        found = True

        try:
            response = self.stub.ServeClient(request)
            print("here")
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
            print(f"Error occured : {str(e)}")
       
    def send_get_request(self,request_type,key=None):
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
                response = json.loads(response.Data)
                print(f"Value for {response.key} = {response.value}")
            else:
                print(f"There is no leader elected in the database and hence the operation failed")
    
        except grpc.RpcError as e:
            print(f"Error occured : {str(e)}")

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
                self.send_set_request('SET', key, value)
            elif choice == '2':
                key = input("Enter key: ")
                self.send_get_request('GET', key)
            elif choice == '3':
                client.close_connection()
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
       

    def close_connection(self):
        self.socket.close()
        self.context.term()
        
if __name__ == "__main__":
    client_id = int(input("Enter Client ID: "))
    server_address = "127.0.0.1:5550"
    client = RaftClient(client_id, server_address)
    client.run()