import grpc
import master_pb2
import master_pb2_grpc
import os
import sys
import multiprocessing
import signal
import threading
import time
import random
import shutil



centroid_file=open("centroids_new.txt","w")
folder=os.path.join(os.getcwd(),f"Data")
eps=1e-8
def exithandler():
    active = multiprocessing.active_children()
    for child in active:
        child.terminate()

signal.signal(signal.SIGINT, exithandler)

def send_to_mapper(request,stub,mapper_id):
    response = stub.MapperParameters(request)
    print(f"Response from id = {mapper_id} {response.success}")
    
def send_to_reducer(request,stub,reducer_id):
    response = stub.ReducerParameters(request)
    print(f"Response from id = {reducer_id} {response.success}")
    # print(f"Response from id  = {reducer_id} {response.success}")
    
# def delete_folders(prefix):
#     # Iterate through all items in the directory
#     for item in os.listdir(folder):
#         # Construct the full path to the item
#         item_path = os.path.join(folder, item)
        
#         # Check if the item is a directory
#         if os.path.isdir(item_path):
#             # If the directory starts with the specified prefix, delete it
#             if item.startswith(prefix):
#                 # Delete the directory and its contents recursively
#                 try:
#                     delete_folder_recursive(item_path)
#                     os.rmdir(folder)
#                     print(f"Deleted directory and its contents: {item_path}")
#                 except OSError as e:
#                     print(f"Error deleting directory and its contents {item_path}: {e}")


def delete_folder_recursive(folder):
    # Recursively delete all files and subdirectories within the folder
    for item in os.listdir(folder):
        item_path = os.path.join(folder, item)
        if os.path.isdir(item_path):
            shutil.rmtree(item_path)
        else:
            os.remove(item_path)
    # Finally, remove the empty folder
    # return response


def get_response(response):
    response = response.result()
    print(response)
    
def run_reducer(reducer_id):
    os.system(f"python reducer.py 6666{reducer_id}")


def run_mapper(mapper_id):
    print("mapper ",mapper_id)
    os.system(f"python mapper.py 5555{mapper_id}")

def read_file():
    with open(os.path.join(os.getcwd(),f'Data/Input/points.txt')) as f:
        points = f.readlines()

        
    final_points = []
    for point in points:
        fpoint = point.strip()
        x,y = fpoint.split(",")
        final_points.append(tuple([float(x),float(y)]))
    return final_points

def create_partitions(points,shard_size,num_mappers):

    split_number = 0
    num_partitions = len(points)//shard_size
    if len(points)%shard_size!=0:
        remaining_size = len(points)-num_partitions*shard_size
        num_partitions+=1
    print("Num Partitions = ",num_partitions)
    
    print("Remaining Size = ",remaining_size)
    partition_id = 0
    start_index = 0
    partitions=[]
    for i in range(num_partitions):
        if start_index+shard_size<=len(points):
            partitions.append(tuple([start_index,start_index+shard_size]))
            start_index+=shard_size
        else:
            print("Start Index = ",start_index)
            print("Ending Index = ",start_index+remaining_size)
            partitions.append(tuple([start_index,start_index+remaining_size]))
            start_index+=remaining_size

    mapper_partitions={}
    for i in range(len(partitions)):
        if i%num_mappers not in mapper_partitions.keys():
            mapper_partitions[i%num_mappers]=[]
        mapper_partitions[i%num_mappers].append(partitions[i]) 
    return mapper_partitions

def update_centroids():
    new_centroids = {}
    for item in os.listdir(os.path.join(folder, "Reducers")):
        with open(os.path.join(folder, "Reducers",item), "r") as f:
            centroid = f.readline().strip().split(",")
            print(centroid)
            new_centroids[int(centroid[0])]=(float(centroid[1]),float(centroid[2]))
    
    print("New Centroids = ",new_centroids,file=centroid_file,flush=True)
    return list(new_centroids.values())
    

def main():
    if len(sys.argv) != 6:
        print("Usage: python master.py <shard_size> <number_of_mappers> <number_of_reducers> <number_of_centroids> <number_of_iterations>")
        sys.exit(1)
    
    # delete_folders("Mappers")
    # delete_folders("Reducers")
    shutil.rmtree(f'Data/Mappers')
    shutil.rmtree(f'Data/Reducers')
    

    shard_size = int(sys.argv[1])
    num_mappers = int(sys.argv[2])
    num_reducers = int(sys.argv[3])
    num_centroids = int(sys.argv[4])
    num_iterations = int(sys.argv[5])

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
    print(points)    
    print(len(points))

    centroids_list = random.sample(points,num_centroids)
    
    mapper_partitions=create_partitions(points,shard_size,num_mappers)
    print(f"Partitions = ")
    print(mapper_partitions)
    print(f"Partitions = {mapper_partitions}")

    # Fork for the number of mappers
    print("Mappers started")
    pidListMappers = []
    
    #start mappers
    for mapper_id in range(num_mappers):
        process = multiprocessing.Process(target=run_mapper, args=(mapper_id,))
        pidListMappers.append(process)
        process.start()

    pidListReducers = []
    for reducer_id in range(num_reducers):
        process = multiprocessing.Process(target=run_reducer, args=(reducer_id,))
        pidListReducers.append(process)
        process.start()
    
    time.sleep(2)
    for iter in range(num_iterations):
        print(f"Iteration {iter+1}")
        print("centroids",centroids_list)
        channels=[]
        responses = []
        mappers=[]
        threadings=[]
        for i in range(num_mappers):
            channel = grpc.insecure_channel(f'localhost:5555{i}')
            channels.append(channel)
            stub = master_pb2_grpc.MasterMapperCommunicationStub(channel)
            request=master_pb2.MapRequest()
            lengths=master_pb2.length()
            
            for j in mapper_partitions[i]:
                lengths.startLength=j[0]+1
                lengths.endLength=j[1]
                request.Lengths.append(lengths)
            
            centroids=master_pb2.DataPoint()
            
            for j in range(num_centroids):
                centroids.x=centroids_list[j][0]
                centroids.y=centroids_list[j][1]
                request.CentroidCoordinates.append(centroids)

            request.reducers = num_reducers

            sentrequest=threading.Thread(target=send_to_mapper,args=(request,stub,i))
            sentrequest.daemon=True
            threadings.append(sentrequest)
            sentrequest.start()
        
        for i in threadings:
            i.join()


        # print("Responses = ",responses)
        print("All Mapper finished.")

        # Fork for the number of reducers
        print("Reducers Started")
        

        time.sleep(2)
        channelsr=[]

        threadings=[]
        for reducer_id in range(num_reducers):
            channel = grpc.insecure_channel(f'localhost:6666{reducer_id}')
            channelsr.append(channel)         
            stub = master_pb2_grpc.MasterReducerCommunicationStub(channel)
            request=master_pb2.ReducerRequest()
            for i in range(num_mappers):
                request.mapperportnumbers.append(f"5555{i}")
            sentrequest=threading.Thread(target=send_to_reducer,args=(request,stub,reducer_id))
            sentrequest.daemon=True
            threadings.append(sentrequest)
            sentrequest.start()

        for i in threadings:
            i.join()
        print("All reducers are  finished.")
        new_centroids=update_centroids()
        if all(abs(centroids_list[i][0]-new_centroids[i][0])<eps and abs(centroids_list[i][1]-new_centroids[i][1])<eps for i in range(num_centroids)):
            break
        else:
            centroids_list=new_centroids
        if iter<num_iterations-1:
            delete_folder_recursive(os.path.join(folder,"Reducers"))
            delete_folder_recursive(os.path.join(folder,"Mappers"))
    
    for i in pidListMappers:
        i.terminate()
    for i in pidListReducers:
        i.terminate()
if __name__ == "__main__":
    main()

