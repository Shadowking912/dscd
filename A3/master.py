import grpc
import master_pb2
import master_pb2_grpc
import os
import sys
import multiprocessing



class MapperReducerCommunication(master_pb2_grpc.MapperReducerCommunicationServicer):
    
    def PartitionParameters(self, request, context):
        return super().PartitionParameters(request, context)
    
class MasterMapperCommunication(master_pb2_grpc.MasterMapperCommunicationServicer):
    def MapperParameters(self, request, context):
        return super().MapperParameters(request, context)

class MasterReducerCommunication(master_pb2_grpc.MasterReducerCommunicationServicer):
    def ReducerParameters(self, request, context):
        return super().ReducerParameters(request, context)
    








def run_reducer(reducer_id):
    # Function to run reducer process
    print(f"Reducer {reducer_id} started.")
    # Add your reducer logic here
    return
def run_mapper(mapper_id):
    # Function to run reducer process
    print(f"Mapper {mapper_id} started.")
    # Add your reducer logic here
    return


def main():
    if len(sys.argv) != 5:
        print("Usage: python master.py <number_of_mappers> <number_of_reducers> <number_of_centroids> <number_of_iterations>")
        sys.exit(1)

    num_mappers = int(sys.argv[1])
    num_reducers = int(sys.argv[2])
    num_centroids = int(sys.argv[3])
    num_iterations = int(sys.argv[4])

    print("Master started.")
    print(f"Number of mappers: {num_mappers}")
    print(f"Number of reducers: {num_reducers}")
    print(f"Number of centroids: {num_centroids}")
    print(f"Number of iterations: {num_iterations}")

    # Fork for the number of mappers
    pidListMappers = []
    for mapper_id in range(num_mappers):
        process = multiprocessing.Process(target=run_mapper, args=(mapper_id,))
        pidListMappers.append(process)
        process.start()
        
    
    for process in pidListMappers:
        process.join()

    print("All Mapper finished.")

    # Fork for the number of reducers
    pidListReducers = []
    for reducer_id in range(num_reducers):
        process = multiprocessing.Process(target=run_reducer, args=(reducer_id,))
        pidListReducers.append(process)
        process.start()
        
    for process in pidListReducers:
        process.join()

    print("All reducers are finished.")

if __name__ == "__main__":
    main()

