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

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.server_address)

    def change_socket_connection(self):
        self.close_connection() 
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.server_address)
      
    def send_set_request(self,request_type,key=None,value=None):
        request={
            'client_id':self.client_id,
            'type':'client_request',
            'sub-type':request_type,
            'key':key,
            'value':value
        }
        found = True
        try:
            self.socket.send_json(request)
            response = self.socket.recv_json()
            status = response['status']
            while status!='success':
                if "leaderid" in response.keys():
                    leader =  response['leaderid']
                    if leader==-1:
                        found = False 
                        break
                    else:
                        print(f"SET Command failed, retrying and contacting the new leader with id : {leader}....")
                        self.server_address = f"tcp://127.0.0.1:555{leader}"
                        self.change_socket_connection()
                        
                else:
                    break

                self.socket.send_json(request)
                response = self.socket.recv_json()
                status = response['status']
            
            if found==True:
                print(f"{response['message']}")
            else:
                print(f"There is no leader elected in the database and hence the operation failed")

        
        except zmq.ZMQError as e:
            print(f"Error occured : {str(e)}")
       
    def send_get_request(self,request_type,key=None):
        request={
            'client_id':self.client_id,
            'type':"client_request",
            'sub-type':'GET',
            'key':key
        }
        found = True
        try:
            self.socket.send_json(request)
            response = self.socket.recv_json()
            status = response['status']

            while status!='success':
                if "leaderid" in response.keys():
                    leader = response['leaderid']
                    if leader==-1:
                        found = False 
                        break
                    else:
                        print(f"GET Command failed, retrying and contacting the new leader with id : {leader}....")
                        self.server_address = f"tcp://127.0.0.1:555{leader}"
                        self.change_socket_connection()
                else:
                    break
            
                self.socket.send_json(request)
                response = self.socket.recv_json()
                status = response['status']
               
            if found==True: 
                print(f"{response['message']}")
            else:
                print(f"There is no leader elected in the database and hence the operation failed")
        
        except zmq.ZMQError as e:
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
    server_address = "tcp://127.0.0.1:5550"
    client = RaftClient(client_id, server_address)
    client.run()