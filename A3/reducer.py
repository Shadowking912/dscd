import sys
import grpc
import master_pb2
import master_pb2_grpc
from concurrent import futures
from itertools import groupby
from operator import itemgetter
import numpy as np
import os
import json
import random
import time 

server=None
flag=None
folder=None
logfile=None
port_number=None
dumpfile=open("Data/dump.txt","a")
class MasterReducerCommunication(master_pb2_grpc.MasterReducerCommunicationServicer):
    
    def reduce(self,responses):
        sum_x=0
        sum_y=0
        number_of_points=0
        final_response={}
        with open(f"{folder}reducer{port_number}.txt","w") as f:
            print("reduce responses",responses,file=logfile,flush=True)
            for i in responses.keys():
                sum_x=0
                sum_y=0
                number_of_points=0
                for j in responses[i]:
                    sum_x+=j[0][0]
                    sum_y+=j[0][1]
                    number_of_points+=j[1]
                final_response[i]=(sum_x/number_of_points,sum_y/number_of_points)
                f.write(f'{i},{final_response[i][0]},{final_response[i][1]}\n')
                print(f'New Centroid {i},{final_response[i][0]},{final_response[i][1]}\n',file=logfile,flush=True)
        return final_response
    
    
    def ReducerParameters(self, request, context):
        #print("Reducer Running",file=logfile,flush=True)
        #responses={}
        # os.mkdir(os.path.join(os.getcwd(),f"Data/Reducer_{sys.argv[1]}"))
        response = master_pb2.ReducerResponse()
        r=random.random()
        if r>flag:
            response.success=2
            return response

        responses={}
        mappers=request.mapperportnumbers
        #print(mappers,file=logfile,flush=True)
        
        response.success=1
        for j in mappers:
            try:
                channel = grpc.insecure_channel(f'localhost:{j}')
                stub = master_pb2_grpc.MapperReducerCommunicationStub(channel)
                request=master_pb2.PartionRequest()
                request.reducerid=int(port_number[-1])
                mapresponse=stub.PartitionParameters(request)
                mapresponse=mapresponse.mappartion
                if mapresponse is not None:
                    for i in mapresponse:
                        if i.key in responses:
                            responses[i.key].append([tuple([i.point.x,i.point.y]),i.freq])
                        else:
                            responses[i.key]=[[tuple([i.point.x,i.point.y]),i.freq]]
                    
            except:
                print(f"Mapper {j} not available",file=logfile,flush=True)
                response.mapper_id = int(j[-1])
                response.success=3
                return response
        for i in responses:
            for k in responses[i]:
                print(f'Centroid {i}: Point {str(k)}',file=logfile,flush=True)
        #print(f"HERE AFTER FLUSHING THE RESPONSES",file=logfile,flush=True)
        response.centroids=json.dumps(self.reduce(responses))
        response.mapper_id=-1
        return response

def stopserver():
    global server
    #print("stopping server")
    server.stop(1)
    sys.exit(0)

def shuffleAndSort(intermediate_data):
    sorted_data = sorted(intermediate_data, key=itemgetter(0))

    grouped_data = {}
    for key, group in groupby(sorted_data, key=itemgetter(0)):
        grouped_data[key] = [value for _, value in group]

    for key, values in grouped_data.items():
        print(f'{key}: {values}',file=logfile,flush=True)

def serve():
    #print("Reducer Running")
    global server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_pb2_grpc.add_MasterReducerCommunicationServicer_to_server(MasterReducerCommunication(),server)
    server.add_insecure_port(f"localhost:{port_number}")
    server.start()
    #print("Server Reducer started")
    server.wait_for_termination()

def run_reducer(port):
    global folder,logfile,port_number,flag
    flag=0.5
    port_number = port
    print("reducer starting",port_number,file=dumpfile,flush=True)
    folder=os.path.join(os.getcwd(),f"Data/Reducers/")
    logfile=open(f"{folder}/reducer{port_number}log.txt","w")
    serve()




