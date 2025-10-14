import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import numpy as np
from sklearn import decomposition


data = pd.read_csv('./users/csv/users_comprehensive_profiles_2024-02-01_to_2025-02-01.csv')
print(data)
selected_columns = ['followers_count', 'following_count', 'posts_count_total', 'reposts_count_total',	'likes_given_count', 'posting_frequency_total', 'total_likes_received',	'total_reposts_received']
data[selected_columns]

data=data[selected_columns]
scaler = StandardScaler()
scaled_data = scaler.fit_transform(data)
# Applying PCA
pca = PCA(n_components=2)  #choose the number of components
pca_result = pca.fit_transform(scaled_data)
variances = pca.explained_variance_

pca = PCA(n_components=3)  #choose the number of components
pca_result = pca.fit_transform(scaled_data)

pca = PCA(n_components=6)
pca_result = pca.fit_transform(scaled_data)
explained_variance_ratio = pca.explained_variance_ratio_

# Print the explained variance ratio for each principal component
for i, evr in enumerate(explained_variance_ratio):
    print(f"Variance explained by PC{i+1}: {evr:.2f}")

# Total variance explained
total_variance_explained = np.sum(explained_variance_ratio)
print(f"Total variance explained: {total_variance_explained:.2f}")


# Applying PCA
pca = decomposition.PCA(n_components=4)
pca.fit(data)

features=['followers_count',	'following_count',	'posts_count_total',	'reposts_count_total',	'likes_given_count',	'posting_frequency_total',		'total_likes_received',	'total_reposts_received']
loadings = pd.DataFrame(pca.components_.T, columns=['PC1', 'PC2', 'PC3','PC4'], index=features)
loadings
# Show which features contribute most to each component
for component in loadings.columns:
    print(f"\n{component} - Features ranked by absolute loading:")
    sorted_loadings = loadings[component].abs().sort_values(ascending=False)
    for feature, loading in sorted_loadings.items():
        actual_loading = loadings.loc[feature, component]
        print(f"  {feature}: {actual_loading:.3f}")
# Get summary statistics for loadings
print("Loading Statistics Summary:")
print("-" * 30)
print(f"Highest positive loading: {loadings.max().max():.3f}")
print(f"Lowest negative loading: {loadings.min().min():.3f}")
print(f"Most influential features per component:")
for component in loadings.columns:
    max_feature = loadings[component].abs().idxmax()
    max_value = loadings.loc[max_feature, component]
    print(f"  {component}: {max_feature} ({max_value:.3f})")