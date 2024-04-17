import numpy as np
import matplotlib.pyplot as plt

# Function to generate random centers far apart from each other
def generate_random_centers(num_centers, min_distance, max_distance):
    centers = []
    for _ in range(num_centers):
        while True:
            # Generate a random center
            center = np.random.uniform(-max_distance, max_distance, size=2)
            # Check if the distance to existing centers is greater than min_distance
            if all(np.linalg.norm(center - c) > min_distance for c in centers):
                centers.append(center)
                break
    return np.array(centers)

# Generate random centers that are far apart
num_centers = 2
min_distance = 3
max_distance = 5
centers = generate_random_centers(num_centers, min_distance, max_distance)

# Generate cluster points around these centers
num_points_per_center = 20
cluster_points = np.concatenate([center + np.random.randn(num_points_per_center, 2) for center in centers])

with open("Data/Input/points4.txt", "w") as file:
    for point in cluster_points:
        file.write(f"{point[0]}, {point[1]}\n")
        
with open("center_points.txt", "w") as file:
    for center in centers:
        file.write(f"{center[0]}, {center[1]}\n")       
# Plotting the generated points
plt.scatter(cluster_points[:, 0], cluster_points[:, 1], s=50)
plt.scatter(centers[:, 0], centers[:, 1], color='red', marker='*', s=300, label='Centers')
plt.xlabel('X')
plt.ylabel('Y')
plt.title('Cluster Points with Random Centers')
plt.legend()
plt.grid(True)
plt.show()