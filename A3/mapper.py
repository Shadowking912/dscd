import master_pb2
import master_pb2_grpc
import grpc
from concurrent import futures
import sys
import threading
import os
import time
import numpy as np
import random
from decimal import Decimal, getcontext

# Set the desired precision
getcontext().prec = 50  # Set the precision to 50 decimal places

# Calculate the exponential function

server=None
folder=None
logfile=None
flag=None
port_number=None
eps=1e-8
def stopserver():
    global server
    #print("stopping server")
    server.stop(1)
    sys.exit(0)

class MapperReducerCommunication(master_pb2_grpc.MapperReducerCommunication):
    def PartitionParameters(self,request,context):
        reducer_id = request.reducerid
        mappartion = master_pb2.mappartion()
        datapoint = master_pb2.DataPoint()
        # datapoint=master_pb2.DataPoint()
        PartitionResponse = master_pb2.PartionResponse()
        #print(f"Reducer ID = {reducer_id}",file=logfile,flush=True)
        with open(f"{folder}/partition_{reducer_id}.txt","r") as f:
            points = f.readlines()
            #print("DEB ",points,file=logfile,flush=True)
            points_final =[]
            for point in points:

                point = point.strip().strip("(").strip(")")
                # point = point[1:-1]
                points = point.split(",")
                mappartion.key=int(points[0])
                x=points[1].strip().strip("(").strip(")")
                y=points[2].strip().strip("(").strip(")")
                if x==0:
                    x+=eps
                
                if y==0:
                    y+=eps
                datapoint.x = float(x)
                datapoint.y = float(y)

                mappartion.point.CopyFrom(datapoint)
                mappartion.freq = 1
                PartitionResponse.mappartion.append(mappartion)
        return PartitionResponse
    
class MasterMapperCommunication(master_pb2_grpc.MasterMapperCommunicationServicer):

    def calculate_keys(self,points,centroids):#Tested code.. Do not Touch
        points = np.array(points)
        centroids = np.array(centroids)
        distances = np.linalg.norm(points[:, np.newaxis, :] - centroids, axis=2)
        nearest_centroid_indices = np.argmin(distances, axis=1)
        assigned_points = [(centroid_idx, tuple(coord_point),1) for centroid_idx, coord_point in zip(nearest_centroid_indices, points)]
        return assigned_points
    
    def read_points(self,indices):
        points = []
        with open(os.path.join(os.getcwd(),f'Data/Input/points.txt'),"r") as file:
            r=file.readlines()
            #print(r,file=logfile,flush=True)
            for i in indices:
                for j in range(i[0]-1,i[1]):
                    point_x,point_y = r[j].strip().split(',')
                    points.append(tuple([float(point_x), float(point_y)]))
        return points

    def partition(self,keys,rno):
        reducers = {k:() for k in range(rno)}

        for i in range(len(keys)):
            index = keys[i][0]
            reducer_id = index%rno
            reducers[reducer_id] = [i for i in keys if i[0]%rno==reducer_id]
    
        for reducer_id in reducers.keys():
            #print(f"REDUCER ID = {reducer_id}",file=logfile,flush=True)
            # with 
            with open(f"{folder}/partition_{reducer_id}.txt","w") as f:
                for j in reducers[reducer_id]:
                    #print(f"VALUE WRITTEN = {j}",file=logfile,flush=True)
                    f.write(f"{j}\n")    
    
    def MapperParameters(self, request, context):
        response = master_pb2.MapResponse()
        r=random.random()
        if r>flag:
            response.success=False
            return response
        
        global server
        
        print("received request from master",file=logfile,flush=True)
        
        lengths = request.Lengths
        centroid_coordinates=request.CentroidCoordinates
        reducers = request.reducers
        for reducer_id in range(reducers):
            with open(f"{folder}/partition_{reducer_id}.txt","w") as f:
                pass
        if not lengths:
            print("Lengths are empty",file=logfile,flush=True)
            response.success=True
            return response
        centroids=[]
        coordinates=[]
        for i in lengths:
            coordinates.append((i.startLength,i.endLength))
        for i in centroid_coordinates:
            centroids.append((i.x,i.y))

        #print("Lengths = ",coordinates,file=logfile,flush=True)
        #print("Centroids = ",centroid_coordinates,file=logfile,flush=True)
        
        if len(lengths)==0:
            response.success=True
            return response

        points = self.read_points(coordinates)
        print("Points = ",points,file=logfile,flush=True)
        keys = self.calculate_keys(points,centroids)#centroids,points
        print("Keys = ",keys,file=logfile,flush=True)
    
        self.partition(keys,reducers)

       
        response.success=True
        # time.sleep(5)
        return response
    
def serve():
    global server
    #print("Port Number= ",port_number)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_pb2_grpc.add_MasterMapperCommunicationServicer_to_server(MasterMapperCommunication(),server)
    master_pb2_grpc.add_MapperReducerCommunicationServicer_to_server(MapperReducerCommunication(),server)
    server.add_insecure_port(f"localhost:{port_number}")
    server.start()
    #print("SERVER STARTED")
    server.wait_for_termination()

def run_mapper(port):    
    
    global folder,logfile,flag,port_number
    port_number=port
    #print("mapper starting",port_number)
    folder=os.path.join(os.getcwd(),f"Data/Mappers/Mapper_{port_number}")
    os.mkdir(folder)
    flag=0.5
    logfile=open(f"{folder}/log.txt","w")
    serve()