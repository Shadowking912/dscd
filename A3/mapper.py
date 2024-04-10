import master_pb2
import master_pb2_grpc
import grpc
from concurrent import futures
import sys
import threading
import os
import time
import numpy as np
server=None

def stopserver():
    global server
    print("stopping server")
    server.stop(1)
    sys.exit(0)

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
            print(r,file=logfile,flush=True)
            for i in indices:
                for j in range(i[0]-1,i[1]):
                    point_x,point_y = r[j].strip().split(',')
                    # point_y = r[j].strip()
                    # print(point_x,point_y,file=logfile,flush=True)
                    points.append(tuple([float(point_x), float(point_y)]))
        return points
    
    # keys = (index,((coordinate),1))
    def partition(self,keys,partitions):
        # print(keys)
        # reducers = self.
        # print()
        # print(f"")
        reducers = {}
        for i in range(len(keys)):
            index = keys[i][0]
            print(f"Index = {index}",file=logfile,flush=True)
            reducer_id = index%partitions
            reducers[reducer_id] = keys
        
        for reducer_id in reducers.keys():
            with open(f"{folder}\partition_{reducer_id}.txt","w") as f:
                for j in reducers[reducer_id]:
                    f.write(f"{j}\n")
        # with open(f"partition_{reducer_id}.txt") as f:
            # f.write()

    def MapperParameters(self, request, context):
        global server
        print("received request from master",file=logfile,flush=True)
        
        lengths = request.Lengths
        centroid_coordinates=request.CentroidCoordinates
        reducers = request.reducers
        
        centroids=[]
        coordinates=[]
        for i in lengths:
            coordinates.append((i.startLength,i.endLength))
        for i in centroid_coordinates:
            centroids.append((i.x,i.y))

        print("Lengths = ",coordinates,file=logfile,flush=True)
        print("Centroids = ",centroid_coordinates,file=logfile,flush=True)


        points = self.read_points(coordinates)
        keys = self.calculate_keys(points,centroids)

        print("Points = ",points,file=logfile,flush=True)
        print("Centroids = ",centroids,file=logfile,flush=True)
        print("Keys = ",keys,file=logfile,flush=True)
        
     
     

        if os.path.isdir(os.path.join(os.getcwd(),f"Mapper_{sys.argv[1]}"))==True:
            os.rmdir(os.path.join(os.getcwd(),f"Mapper_{sys.argv[1]}"))
        else:
            os.mkdir(os.path.join(os.getcwd(),f"Mapper_{sys.argv[1]}"))

        self.partition(keys,reducers)
        # if os.path.exists(os.path.join(os.getcwd(),f"/output_mapper_{sys.argv[1]}.txt"))==True:
        #     os.remove(os.path.join(os.getcwd(),f"/output_mapper_{sys.argv[1]}.txt"))
        # with open(f"output_mapper_{sys.argv[1]}.txt","w") as f:
        #     f.write(f"Indices : {str(lengths)}\nCentroids: {str(centroid_coordinates)}\n")
    

        
        
        response = master_pb2.MapResponse()
        response.success=True
        # print(response)
        x=threading.Timer(1,stopserver)
        x.start()
        return response
    
def serve():
    global server
    
    
    print("Arguuments = ",sys.argv)
    port_number = sys.argv[1]
    print("Port Number= ",port_number)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_pb2_grpc.add_MasterMapperCommunicationServicer_to_server(MasterMapperCommunication(),server)
    server.add_insecure_port(f"localhost:{port_number}")
    server.start()
    print("SERVER STARTED")
    server.wait_for_termination()
    
folder=os.path.join(os.getcwd(),f"Mapper_{sys.argv[1]}")
print("mapper starting")
logfile=open(f"mapper{sys.argv[1]}log.txt","w")
serve()