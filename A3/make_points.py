import numpy as np
from sklearn.datasets import make_blobs
# Generating cluster points with large standard deviation to make them far apart
X, _ = make_blobs(n_samples=30, centers=3, cluster_std=10.0, random_state=0)

# Writing cluster points to a file
with open("Data/Input/points4.txt", "w") as file:
    for point in X:
        file.write(f"{point[0]},{point[1]}\n")