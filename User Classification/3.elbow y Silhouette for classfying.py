import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import copy
from sklearn.cluster import KMeans
from yellowbrick.cluster import KElbowVisualizer
from scipy.stats import norm
from sklearn.metrics import silhouette_score
from yellowbrick.cluster import SilhouetteVisualizer
import sklearn.metrics as metrics

df = pd.read_csv('./users/csv/users_comprehensive_profiles_2024-02-01_to_2025-02-01.csv')
print(df)
columnas_utilizadas= ['followers_count',	'following_count',	'posts_count_total',	'total_reposts_received',		'total_likes_received']

data= df[columnas_utilizadas]

integers_columns= ['followers_count',	'following_count',	'posts_count_total',	'total_reposts_received',		'total_likes_received']

for atributo_integer in integers_columns:
    data[atributo_integer]= data[atributo_integer].astype(int)

print(data.dtypes)

dataset_inicial= copy.deepcopy(data)
ids_users = df["user_id"]
dataset_inicial["id"] = ids_users

dataset_inicial.head()

# Elbow Method
def plot_results_method_elbow(inertials):
    print("\n")
    x, y = zip(*[inertia for inertia in inertials])
    plt.plot(x, y, 'ro-', markersize=8, lw=2)
    plt.grid(True)
    plt.xlabel('Number of Clusters')
    plt.ylabel('Inertia')
    plt.title("Elbow method for choosing the number of clusters")
    plt.show()
def select_clusters(data_tmp):
    X= data_tmp.to_numpy()

    # Instantiate the clustering model and visualizer
    model = KMeans()
    visualizer = KElbowVisualizer(model, k=(2,20), timings=False)

    visualizer.fit(X)        # Fit the data to the visualizer
    visualizer.show()        # Finalize and render the figure

select_clusters(data)


def print_results_kmm(centroids, num_cluster_points):
    print ('\n\nFINAL RESULT:')
    for i, c in enumerate(centroids):
        print('\n\tCluster %d' % (i + 1))
        print('\t\tNumber Points in Cluster %d' % num_cluster_points.count(i))
        print('\t\tCentroid: %s' % str(centroids[i]))

def k_means(data, num_clusters, max_iterations, init_cluster, tolerance, dataset_inicial):
    # Read data set
    X= data.to_numpy()

    # Object KMeans
    kmeans = KMeans(n_clusters=num_clusters, max_iter=max_iterations,
                    init=init_cluster, tol=tolerance)

    # Calculate Kmeans
    kmeans.fit(X)

    # Obtain centroids and number Cluster of each point
    centroides = kmeans.cluster_centers_
    etiquetas = kmeans.labels_

    df_labels = dataset_inicial.assign(Cluster = etiquetas)

    # Print final result
    print_results_kmm(centroides, etiquetas.tolist())


    return df_labels

NUM_CLUSTERS= 5
MAX_ITERATIONS = 50000
INITIALIZE_CLUSTERS = 'k-means++'
CONVERGENCE_TOLERANCE = 0.0000001

print(f"\n\n------------------------ KMeans with {NUM_CLUSTERS} clusters ------------------------")
df_labels= k_means(data, NUM_CLUSTERS, MAX_ITERATIONS, INITIALIZE_CLUSTERS,
           CONVERGENCE_TOLERANCE, dataset_inicial)
            
            
df_labels["Cluster"].value_counts()



# Silhouette Score
columnas_utilizadas= ['followers_count',	'following_count',	'posts_count_total',	'total_reposts_received',		'total_likes_received']
data= df[columnas_utilizadas]

sil_score = []
SK = list(range(2, 20))  

for i in SK:
    labels = KMeans(n_clusters=i, init="k-means++", n_init=20, max_iter=50000, random_state=42).fit(data).labels_
    score = metrics.silhouette_score(data, labels, metric="euclidean", sample_size=10000)
    sil_score.append(score)
    print(f"Silhouette score for k = {i} is {score}")
    