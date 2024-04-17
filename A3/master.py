import grpc
import master_pb2
import master_pb2_grpc
import os
import sys
import multiprocessing
import signal
import threading
import time
import math
import random
import shutil
import json
from mapper import run_mapper
from reducer import run_reducer
import matplotlib.pyplot as plt
import numpy as np


points_file="Data/Input/points.txt"
final_centroid_file="final_centroids.txt"
logfile = "master_log.txt"
centroid_file="centroids_new.txt"
folder=os.path.join(os.getcwd(),f"Data")
eps=1e-8
temp_centroids = {}
succesmappers=set()
succesreducers=set()
restartmappers=set()

def exithandler():
    active = multiprocessing.active_children()
    for child in active:
        child.terminate()

signal.signal(signal.SIGINT, exithandler)

class custom_exeception(Exception):
    def __init__(self,message):
        super().__init__(message)
        self.message = message


def send_to_mapper(mapper_id,mapper_partitions,num_centroids,centroids_list,num_reducers):
    global succesmappers
    try:
        channel = grpc.insecure_channel(f'localhost:5555{mapper_id}')
        stub = master_pb2_grpc.MasterMapperCommunicationStub(channel)
        request=master_pb2.MapRequest()
        lengths=master_pb2.length()
        for j in mapper_partitions[mapper_id]:
            if(j[0]!=j[1]):
                lengths.startLength=j[0]+1
                lengths.endLength=j[1]
                request.Lengths.append(lengths)
        
        centroids=master_pb2.DataPoint()

        for j in range(num_centroids):
            centroids.x=centroids_list[j][0]
            centroids.y=centroids_list[j][1]
            request.CentroidCoordinates.append(centroids)

        request.reducers = num_reducers
        response = stub.MapperParameters(request)
        if response.success==False:
            with open(logfile,"a") as f:
                f.write(f"Response from Mapper id (in Master) = {mapper_id} {response.success}\n")
            send_to_mapper(mapper_id,mapper_partitions,num_centroids,centroids_list,num_reducers)
       
        elif response.success==True:
            with open(logfile,"a") as f:
                f.write(f"Response from Mapper id (in Master) = {mapper_id} {response.success}\n")
            lock = threading.Lock()
            lock.acquire()
            succesmappers.add(mapper_id)
            # print(succesmappers)
            lock.release()

    except grpc.RpcError as e:
        pass
    channel.close()
        
def send_to_reducer(reducer_id,num_mappers):
    global temp_centroids,succesreducers,restartmappers
    try:
        channel = grpc.insecure_channel(f'localhost:6666{reducer_id}')   
        stub = master_pb2_grpc.MasterReducerCommunicationStub(channel)
        request=master_pb2.ReducerRequest()
        for i in range(num_mappers):
            request.mapperportnumbers.append(f"5555{i}")
        response = stub.ReducerParameters(request)
        if response.success==2:
            with open(logfile,"a") as f:
                f.write(f"Response from Reducer id (in Master)= {reducer_id} {response.success}\n")

            send_to_reducer(reducer_id,num_mappers)
    
        elif response.success==1:
            lock = threading.Lock()
            with open(logfile,"a") as f:
                f.write(f"Response from Reducer id (in Master)= {reducer_id} {response.success}\n")
            lock.acquire()
            temp_centroids.update(json.loads(response.centroids))
            succesreducers.add(reducer_id)
            # print("Temporary Centroids = ",response.centroids,file=logfile,flush=True)
            lock.release()

        elif response.success==3:
            with open(logfile,"a") as f:
                f.write(f"Response from Reducer id (in Master)= {reducer_id} {response.success}\n")

            lock = threading.Lock()
            lock.acquire()
            restartmappers.add(response.mapper_id)
            lock.release()
    
    except Exception as e:
       print(e)
    channel.close()
    
    # return json.loads(response.centroids)
    # print(f"Response from id  = {reducer_id} {response.success}")
    
def delete_folder_recursive(folder):
    # Recursively delete all files and subdirectories within the folder
    for item in os.listdir(folder):
        item_path = os.path.join(folder, item)
        if os.path.isdir(item_path):
            shutil.rmtree(item_path)
        else:
            os.remove(item_path)

def get_response(response):
    response = response.result()
    # print("Response in Master",response)
    
def plotter():
    points=[]
    with open(points_file, "r") as f:
        for i in f.readlines():
            points.append(tuple(map(float,i.strip().split(','))))
        # Sample data points
    points = np.array(points)
    # Sample centroids
    centroids=[]
    with open("final_centroids.txt", "r") as f:
        for i in f.readlines():
            centroids.append(tuple(map(float,i.strip().split(','))))
    centroids = np.array(centroids)


    cluster_points = [[] for _ in range(len(centroids))]  # Initialize an empty list for each cluster

    for point in points:
        distances = np.linalg.norm(point - centroids, axis=1)  # Calculate Euclidean distance to each centroid
        closest_centroid_index = np.argmin(distances)  # Find the index of the closest centroid
        cluster_points[closest_centroid_index].append(point)  # Assign the point to the closest cluster

    # Plotting the data points with different colors for each cluster
    for i, cluster in enumerate(cluster_points):
        cluster = np.array(cluster)
        plt.scatter(cluster[:,0], cluster[:,1], label=f'Cluster {i+1}')
    
    for i, centroid in enumerate(centroids):
        plt.scatter(centroid[0], centroid[1], color='red', s=100, label='Centroids')
        plt.text(round(centroid[0],6), round(centroid[1],6), f'({round(centroid[0],6)}, {round(centroid[1],6)})', fontsize=8, ha='center', va='bottom')
    # Plotting the centroids

    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('Cluster Visualization')
    plt.legend()
    plt.grid(True)
    plt.show()

def read_file():
    with open(os.path.join(os.getcwd(),points_file),"r") as f:
        points = f.readlines()

        
    final_points = []
    for point in points:
        fpoint = point.strip()
        x,y = fpoint.split(",")
        final_points.append(tuple([float(x),float(y)]))
    return final_points

def create_partitions(points,shard_size,num_mappers):
    shard_size=math.ceil(len(points)/num_mappers)
    num_partitions = num_mappers
    print("Num Partitions = ",num_partitions,"shard_size = ",shard_size )
    start_index = 0
    partitions=[]
    for i in range(num_partitions):
        if start_index+shard_size<=len(points):
            partitions.append(tuple([start_index,start_index+shard_size]))
            start_index+=shard_size
    if start_index<len(points):
        partitions.append(tuple([start_index,len(points)]))
    print(partitions)
    mapper_partitions={}
    for i in range(num_mappers):
        mapper_partitions[i]=[]

    for i in range(len(partitions)):
        if i%num_mappers not in mapper_partitions.keys():
            mapper_partitions[i%num_mappers]=[]
        mapper_partitions[i%num_mappers].append(partitions[i])
    print("deb",mapper_partitions) 
    return mapper_partitions
# split_number = 0
    # shard_size = len(points) // num_mappers
    # num_partitions = num_mappers
    # if len(points)%shard_size!=0:
    #     remaining_size = len(points)-num_partitions*shard_size
    #     num_partitions+=1

    # start_index = 0
    # partitions=[]
    # for i in range(num_partitions):
    #     if start_index+shard_size<=len(points):
    #         partitions.append(tuple([start_index,start_index+shard_size]))
    #         start_index+=shard_size
    #     else:
    #         partitions.append(tuple([start_index,start_index+remaining_size]))
    #         start_index+=remaining_size

    # mapper_partitions={}
    # for i in range(len(partitions)):
    #     if i%num_mappers not in mapper_partitions.keys():
    #         mapper_partitions[i%num_mappers]=[]
    #     mapper_partitions[i%num_mappers].append(partitions[i]) 
    # return mapper_partitions

def killers_of_doom(stop_event,mapperpids, reducerpids):
    print("Killers of doom started")
    print("mapperpids: ",mapperpids)
    print("reducerpids: ",reducerpids)
    while stop_event.is_set()==False:
        x=input("Enter to kill process (m/r/e,index)").split()
        if x[0]=='m':
            mapperpids[int(x[1])].terminate()
            mapperpids[int(x[1])].close()
        
        elif x[0]=='r':
            reducerpids[int(x[1])].terminate()
            reducerpids[int(x[1])].close()
        
        elif x[0]=='e':
            break

def update_centroids(centroids_list):
    global temp_centroids
    # print(temp_centroids,type(temp_centroids))
    new_centroids = centroids_list.copy()
    x=sorted(temp_centroids.items())

    with open(logfile,"a") as f:
        f.write(f"Temp Centroids = {x}\n")

    for i in x:
        new_centroids[int(i[0])]=(float(i[1][0]),float(i[1][1]))
    
    with open(centroid_file,"a") as f:
        f.write(f"New Centroids = {new_centroids}\n")
    
    return new_centroids
    
def main():
    global succesmappers,succesreducers,restartmappers
    with open(logfile,"w") as f:
        pass
    with open(centroid_file,"w") as f:
        pass
    with open("Data/dump.txt","w") as f:
        pass
    if len(sys.argv) != 5:
        print("Usage: python master.py <number_of_mappers> <number_of_reducers> <number_of_centroids> <number_of_iterations>")
        sys.exit(1)
    
    # delete_folders("Mappers")
    # delete_folders("Reducers")
    try:
        shutil.rmtree(f'Data/Mappers')
        shutil.rmtree(f'Data/Reducers')
    except:
        pass
    
    num_mappers = int(sys.argv[1])
    num_reducers = int(sys.argv[2])
    num_centroids = int(sys.argv[3])
    num_iterations = int(sys.argv[4])
    
    shard_size = num_mappers
    # sys.exit(0)
    if os.path.isdir(os.path.join(folder,"Mappers"))==False:
        os.mkdir(os.path.join(folder,"Mappers"))
    else:
        os.mkdir(os.path.join(folder,"Mappers"))

    if os.path.isdir(os.path.join(folder,"Reducers"))==False:
        os.mkdir(os.path.join(folder,"Reducers"))
    else:
        os.mkdir(os.path.join(folder,"Reducers"))
        # sys.exit(0)
    

    print("Master started.")
    print(f"Number of mappers: {num_mappers}")
    print(f"Number of reducers: {num_reducers}")
    print(f"Number of centroids: {num_centroids}")
    print(f"Number of iterations: {num_iterations}")

    # Read the points from the file
    points = read_file()
    print(f"Points{points}, len(points)={len(points)} ")    

    centroids_list = random.sample(points,num_centroids)
    print("Centroids = ",centroids_list)
    mapper_partitions=create_partitions(points,shard_size,num_mappers)
    print(f"Partitions = {mapper_partitions}")

    # Fork for the number of mappers
    print("Mappers started")
    pidListMappers = []
    #start mappers
    for mapper_id in range(num_mappers):
        process = multiprocessing.Process(target=run_mapper, args=(f"5555{mapper_id}",points_file))
        pidListMappers.append(process)
        process.start()

    pidListReducers = []
    for reducer_id in range(num_reducers):
        process = multiprocessing.Process(target=run_reducer, args=(f"6666{reducer_id}",))
        pidListReducers.append(process)
        process.start()
        
    stop_event = threading.Event()
    x=threading.Thread(target=killers_of_doom,args=(stop_event,pidListMappers,pidListReducers))
    x.daemon=True
    x.start()

    time.sleep(2)
    iter=0
    while iter<num_iterations:
        
        succesreducers=set()
        succesmappers=set()
        # print(f"Iteration {iter+1}")
        # print("(Master) New Centroids = ",centroids_list,file=centroid_file,flush=True)
        channels=[]
        responses = []
        mappers=[]
        threadings=[]
        for i in range(num_mappers):
            sentrequest=threading.Thread(target=send_to_mapper,args=(i,mapper_partitions,num_centroids,centroids_list,num_reducers))
            sentrequest.daemon=True
            threadings.append(sentrequest)
            sentrequest.start()
        
        
            
        while len(succesmappers)!=num_mappers:
            for i in threadings:
                i.join()
            
            for i in range(num_mappers):
                if i not in succesmappers:
                    pidListMappers[i]=multiprocessing.Process(target=run_mapper, args=(f"5555{i}",))
                    pidListMappers[i].start()
                    threadings[i]=threading.Thread(target=send_to_mapper,args=(i,mapper_partitions,num_centroids,centroids_list,num_reducers))
                    threadings[i].start()

        # print("Responses = ",responses)
        with open(logfile,"a") as f:
            f.write("All Mapper finished\n")

        # Fork for the number of reducers
        with open(logfile,"a") as f:
            f.write("Reducers Started\n")


        threadings=[]
        for reducer_id in range(num_reducers):
            sentrequest=threading.Thread(target=send_to_reducer,args=(reducer_id,num_mappers))
            sentrequest.daemon=True
            threadings.append(sentrequest)
            sentrequest.start()

        while len(succesreducers)!=num_reducers:
            for i in threadings:
                i.join()
            
            if len(restartmappers)>0:
                break
            
            for i in range(num_reducers):
                if i not in succesreducers:
                    try:
                        pidListReducers[i]=multiprocessing.Process(target=run_reducer, args=(f"6666{i}",))
                        pidListReducers[i].start()
                        threadings[i]=threading.Thread(target=send_to_reducer,args=(i,num_mappers))
                        threadings[i].start()
                    except Exception as e:
                        print("here",e)
                        # sys.exit(0)

        with open(logfile,"a") as f:
            f.write("All Reducers are  finished\n")

        if len(restartmappers)>0:
            for i in restartmappers:
                with open(logfile,"a") as f:
                    f.write(f"Restarting Mapper {i}\n")
                pidListMappers[i]=multiprocessing.Process(target=run_mapper, args=(f"5555{i}",))
                pidListMappers[i].start()
            restartmappers=set()
            continue

        with open(logfile,"a") as f:
            f.write(f"centroids_list {centroids_list}\n")

        new_centroids=update_centroids(centroids_list)
        with open(logfile,"a") as f:
            f.write(f"New Centroids after it {iter+1}, is {new_centroids}\n")
    
        if all(abs(centroids_list[i][0]-new_centroids[i][0])<eps and abs(centroids_list[i][1]-new_centroids[i][1])<eps for i in range(num_centroids)):
            break
        else:
            centroids_list=new_centroids
            # for i in new_centroids:
            # print("Deb",centroids_list)
        # if iter<num_iterations-1:
        #     break
        iter+=1
    
    for i in pidListMappers:
        i.terminate()
        i.join()
        i.close()
    
    for i in pidListReducers:
        i.terminate()
        i.join()
        i.close()

    # delete_folder_recursive(os.path.join(folder,"Reducers"))
    # delete_folder_recursive(os.path.join(folder,"Mappers"))
    # stop_event.set()
    # x.join()
    with open("final_centroids.txt","w") as f:
        for i in centroids_list:
            f.write(f"{i[0]},{i[1]}\n")
    plotter()
    sys.exit(0)

    
if __name__ == "__main__":
    main()

