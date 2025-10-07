import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from scipy.stats import norm
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from yellowbrick.cluster import KElbowVisualizer
import matplotlib.colors as mcolors

users_df = pd.read_csv('./5_clustered_users.csv',encoding='ISO-8859-1')

users_df["Cluster"].value_counts()
print(users_df.dtypes)
print(users_df.head())


def plot_violin_distribution_integer_variables_cluster(list_df_clusters, integers_columns, N, clusters_name, zoom_values_integers):
  print("\n\nObserve the distribution of the numerical attributes of each cluster from the violin graph\n")
  
  colors = ['#FFB6C1', '#FFDAB9', '#E6E6FA', '#ADD8E6', '#B0E0E6', '#87CEEB', '#FFD700']
  fig, ((ax1, ax2, ax3), (ax4, ax5, ax6)) = plt.subplots(2, 3, figsize=(22, 12))

  axis= [ax1, ax2, ax3, ax4, ax5, ax6]


  xloc = np.arange(N)

  contador = 0
  for ax, c in zip(axis, integers_columns):
    data_clusters= []
    for count, df in enumerate(list_df_clusters):
      column = pd.to_numeric(df[c], errors='coerce').dropna().tolist()

      data_clusters.append(column)

    _vp= ax.violinplot(data_clusters)

    index= 0
    for patch, color in zip(_vp["bodies"], colors):
      patch.set_facecolor(list(plt.cm.tab10(index)))
      index+=1


    ax.set_xticks(np.arange(1, len(clusters_name) + 1), labels=clusters_name)
    ax.set_ylim(zoom_values_integers[contador])

    ax.set_ylabel('Count', fontsize=12)
    ax.set_xlabel('Clusters', fontsize=12)
    ax.yaxis.grid(True)

    # Adding title
    ax.set_title(f"{c}")

    contador+= 1

  fig.suptitle('Distribution of the attributes in the different groups obtained', fontsize= 18)
  plt.show()


def addlabels(ax, x,y):

  for i in range(len(x)):
      desplazamiento= -0.04
      if round(abs(y[i]),2) < 0.1:
        desplazamiento= 0.02

      ax.text(i, y[i]+ desplazamiento, round(abs(y[i]),2), ha = 'center')


def create_percent_data(list_df_clusters, c):
  list_percent_true= []
  list_percent_false= []

  for count, df in enumerate(list_df_clusters):

    count_true = df.apply(lambda x: x[c] ==  1, axis=1).sum()
    count_false = df.apply(lambda x: x[c] == 0, axis=1).sum()

    total = (count_true + count_false)

    percent_count_true = round(count_true / total, 2)
    percent_count_false= round(count_false / total, 2)

    list_percent_true.append(percent_count_true)
    list_percent_false.append(percent_count_false)

  return list_percent_true, list_percent_false





def representation_distribution_clusters(users_df, number_clusters, clusters_name):
  list_df_clusters= []
  # Sanitize numeric columns in each cluster
  integers_columns = ['followers_count', 'following_count', 'posts_count_total', 'total_reposts_received', 'total_likes_received']
  for i in range(len(list_df_clusters)):
      for col in integers_columns:
          list_df_clusters[i][col] = pd.to_numeric(list_df_clusters[i][col], errors='coerce')

  
  zoom_values_integers = [(0, 3000), (0, 5000), (0, 1500), (0, 250000), (0, 1000000)]


  for n in range(number_clusters):
    list_df_clusters.append(users_df.loc[users_df['Cluster'] == n])


  plot_violin_distribution_integer_variables_cluster(list_df_clusters, integers_columns, number_clusters, clusters_name, zoom_values_integers)
  
number_clusters= 5
clusters_name= ["0", "1", "2", "3", "4"]

representation_distribution_clusters(users_df, number_clusters, clusters_name)