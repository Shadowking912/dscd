import sys
import grpc
import master_pb2
import master_pb2_grpc
from concurrent import futures
from itertools import groupby
from operator import itemgetter
import numpy as np
import os

server=None
logfile=open(f"reducer{sys.argv[1]}log.txt","w")
folder=os.path.join(os.getcwd(),f"Data/Reducers/")

class MasterReducerCommunication(master_pb2_grpc.MasterReducerCommunicationServicer):
    
    def reduce(self,responses):
        sum_x=0
        sum_y=0
        number_of_points=0
        final_response={}
        with open(f"{folder}reducer{sys.argv[1]}.txt","w") as f:
            for i in responses.keys():
                for j in responses[i]:
                    sum_x+=j[0][0]
                    sum_y+=j[0][1]
                    number_of_points+=j[1]
                final_response[i]=(sum_x/number_of_points,sum_y/number_of_points)
                f.write(f'{i},{final_response[i][0]},{final_response[i][1]}\n')
    
    
    print("Reducer Running",file=logfile,flush=True)
    def ReducerParameters(self, request, context):
        #responses={}
        # os.mkdir(os.path.join(os.getcwd(),f"Data/Reducer_{sys.argv[1]}"))
        responses={}
        mappers=request.mapperportnumbers
        print(mappers,file=logfile,flush=True)
        response = master_pb2.ReducerResponse()
        response.success=True
        for i in mappers:
            channel = grpc.insecure_channel(f'localhost:{i}')
            stub = master_pb2_grpc.MapperReducerCommunicationStub(channel)
            request=master_pb2.PartionRequest()
            request.reducerid=int(port_number[-1])
            mapresponse=stub.PartitionParameters(request)
            mapresponse=mapresponse.mappartion
            for i in mapresponse:
                if i.key in responses:
                    responses[i.key].append([tuple([i.point.x,i.point.y]),i.freq])
                else:
                    responses[i.key]=[[tuple([i.point.x,i.point.y]),i.freq]]
        
        for i in responses:
            for j in responses[i]:
                print(f'{i}: {str(j)}',file=logfile,flush=True)

        print(f"HERE AFTER FLUSHING THE RESPONSES",file=logfile,flush=True)
        self.reduce(responses)
        return response

def stopserver():
    global server
    print("stopping server")
    server.stop(1)
    sys.exit(0)

def shuffleAndSort(intermediate_data):
    sorted_data = sorted(intermediate_data, key=itemgetter(0))

    grouped_data = {}
    for key, group in groupby(sorted_data, key=itemgetter(0)):
        grouped_data[key] = [value for _, value in group]

    for key, values in grouped_data.items():
        print(f'{key}: {values}')

def serve():
    print("Reducer Running")
    global server
    print("Arguuments = ",sys.argv)
    print("Port Number= ",port_number)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_pb2_grpc.add_MasterReducerCommunicationServicer_to_server(MasterReducerCommunication(),server)
    server.add_insecure_port(f"localhost:{port_number}")
    server.start()
    print("Server Reducer started")
    server.wait_for_termination()

port_number = sys.argv[1]
serve()




