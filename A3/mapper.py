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
    def calculate_keys(self,points,centroids):
        print(points[:, np.newaxis, :])
        distances = np.linalg.norm(points[:, np.newaxis, :] - centroids, axis=2)
        labels = np.argmin(distances, axis=1)
        return labels
    
    def read_points(self,indices):
        points = []
        with open(os.path.join(os.getcwd(),f'Data/Input/points.txt')) as file:
            r=file.readlines()
            print(r,file=logfile,flush=True)
            for i in indices:
                for j in range(i[0]-1,i[1]):
                    point_x,point_y = r[j].strip().split(',')
                    # point_y = r[j].strip()
                    print(point_x,point_y,file=logfile,flush=True)
                    points.append(tuple([float(point_x), float(point_y)]))
        return points
    
    def MapperParameters(self, request, context):
        global server
        print("received request from master",file=logfile,flush=True)
        
        lengths = request.Lengths
        coordinates=[]
        for i in lengths:
            coordinates.append((i.startLength,i.endLength))
        print("Lengths = ",coordinates,file=logfile,flush=True)
        
        centroid_coordinates=request.CentroidCoordinates
        points = self.read_points(coordinates)
        
        # self.calculate_keys()
        if os.path.exists(os.path.join(os.getcwd(),f"/output_mapper_{sys.argv[1]}.txt"))==True:
            os.remove(os.path.join(os.getcwd(),f"/output_mapper_{sys.argv[1]}.txt"))
        with open(f"output_mapper_{sys.argv[1]}.txt","w") as f:
            f.write(f"Indices : {str(lengths)}\nCentroids: {str(centroid_coordinates)}\n")
        
        print("ID  = ",sys.argv[1])
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

print("mapper starting")
logfile=open(f"mapper{sys.argv[1]}log.txt","w")
serve()