import grpc
import master_pb2
import master_pb2_grpc
import os
import sys
import multiprocessing
import signal
import threading
import time
eps=1e-8
def exithandler():
    active = multiprocessing.active_children()
    for child in active:
        child.terminate()

signal.signal(signal.SIGINT, exithandler)

def send_to_mapper(request,stub,mapper_id):
    response = stub.MapperParameters(request)
    print(f"Response from id = {mapper_id} {response.success}")
    
    # return response
# class MapperReducerCommunication(master_pb2_grpc.MapperReducerCommunicationServicer):
    
#     def PartitionParameters(self, request, context):
#         return super().PartitionParameters(request, context)
    
# # class MasterMapperCommunication(master_pb2_grpc.MasterMapperCommunicationServicer):
# #     def MapperParameters(self, request, context):
# #         return super().MapperParameters(request, context)

# class MasterReducerCommunication(master_pb2_grpc.MasterReducerCommunicationServicer):
#     def ReducerParameters(self, request, context):
#         return super().ReducerParameters(request, context)
    
def get_response(response):
    response = response.result()
    print(response)
    
def run_reducer(reducer_id):
    # Function to run reducer process
    # command=f"5556{reducer_id}"
    # exec(open('reducer.py').read(),{'argv':command})
    os.system("python reducer.py")


def run_mapper(mapper_id):
    # Function to run reducer process
    # Assuming the script you want to run is named 'another_script.py'
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
    # if os.path.isdir(os.path.join(os.getcwd(),f'Data/Splits'))==False:
    #     os.makedirs(os.path.join(os.getcwd(),f'Data/Splits'))
    # else:
    #     os.rmdir(os.path.join(os.getcwd(),f'Data/Splits'))
    #     os.makedirs(os.path.join(os.getcwd(),f'Data/Splits'))

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


def main():


    if len(sys.argv) != 6:
        print("Usage: python master.py <shard_size> <number_of_mappers> <number_of_reducers> <number_of_centroids> <number_of_iterations>")
        sys.exit(1)

    shard_size = int(sys.argv[1])
    num_mappers = int(sys.argv[2])
    num_reducers = int(sys.argv[3])
    num_centroids = int(sys.argv[4])
    num_iterations = int(sys.argv[5])

    print("Master started.")
    print(f"Number of mappers: {num_mappers}")
    print(f"Number of reducers: {num_reducers}")
    print(f"Number of centroids: {num_centroids}")
    print(f"Number of iterations: {num_iterations}")

    # Read the points from the file
    points = read_file()
    print(points)    
    print(len(points))

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
    time.sleep(3)
    channels=[]
    responses = []
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
            centroids.x=0+eps
            centroids.y=0+eps
            request.CentroidCoordinates.append(centroids)

        sentrequest=threading.Thread(target=send_to_mapper,args=(request,stub,i))
        sentrequest.daemon=True
        sentrequest.start()


    for process in pidListMappers:
        process.join()

    # print("Responses = ",responses)
    print("All Mapper finished.")

    # for response in responses:
    #     print(response.result())
    # for response in responses:
    #     threading.Thread(target=get_response,args=(response,)).start()
    # Fork for the number of reducers
    # print("Reducers Started")
    # pidListReducers = []
    # for reducer_id in range(num_reducers):
    #     process = multiprocessing.Process(target=run_reducer, args=(reducer_id,))
    #     pidListReducers.append(process)
    #     process.start()
        
    # for process in pidListReducers:
    #     process.join()
    
    # print("All reducers are  finished.")

if __name__ == "__main__":
    main()

