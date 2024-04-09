import sys
import grpc
import master_pb2_grpc
from master_pb2_grpc import MapperReducerCommunication
from concurrent import futures
from itertools import groupby
from operator import itemgetter
import numpy as np

server=None

def kmeans_worker(data, centroids, max_iterations=100):
    k = centroids.shape[0]

    for _ in range(max_iterations):
        # Assign points to the nearest centroid
        distances = np.linalg.norm(data[:, np.newaxis, :] - centroids, axis=2)
        labels = np.argmin(distances, axis=1)

        # Compute the new centroids
        new_centroids = np.empty_like(centroids)
        for i in range(k):
            cluster_points = data[labels == i]
            if len(cluster_points) > 0:
                new_centroids[i] = np.mean(cluster_points, axis=0)
            else:
                new_centroids[i] = centroids[i]

        centroids = new_centroids

    return centroids

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

def main():
    print("Reducer Running")
    global server
    print("Arguuments = ",sys.argv)
    port_number = sys.argv[1]
    print("Port Number= ",port_number)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_pb2_grpc.add_MapperReducerCommunicationServicer_to_server(MapperReducerCommunication(),server)
    server.add_insecure_port(f"localhost:{port_number}")
    server.start()
    print("Server Reducer started")

main()





# if __name__ == "__main__":
#     # Example usage
#     # Assuming data and centroids are provided to this worker
#     data = np.random.rand(100, 2)  # Example data (already partitioned)
#     centroids = np.random.rand(3, 2)  # Example centroids
#     max_iterations = 100

#     new_centroids = kmeans_worker(data, centroids, max_iterations)
#     print("New centroids:", new_centroids)
