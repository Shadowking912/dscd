import master_pb2
import master_pb2_grpc
import grpc
from concurrent import futures
import sys
class MasterMapperCommunication(master_pb2_grpc.MasterMapperCommunicationServicer):
    def MapperParameters(self, request, context):
        print("received request from master")
        indices = request.Indices
        centroid_coordinates=request.CentroidCoordinates
        print("indices = ",indices)
        print("centroid coordinates = ",centroid_coordinates)
        
        # response = master_pb2.MapResponse()
        # key = 
        # response = master_pb2.MapResponse() 

        with open(f"output_mapper_{sys.argv[1]}.txt","w") as f:
            f.write(f"Indices : {str(indices)}\nCentroids: {str(centroid_coordinates)}\n")
                
        # return response

def serve():
    print("Arguuments = ",sys.argv)
    port_number = sys.argv[1]
    print("Port Number= ",port_number)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_pb2_grpc.add_MasterMapperCommunicationServicer_to_server(MasterMapperCommunication(),server)
    server.add_insecure_port(f"localhost:{port_number}")
    server.start()
    server.wait_for_termination()


print("mapper starting")

serve()