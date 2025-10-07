import tensorflow as tf
import optuna
from tensorflow import keras
from tensorflow.keras import layers, optimizers
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from scipy.stats import truncnorm
import matplotlib.pyplot as plt
from tensorflow.keras.callbacks import Callback
import seaborn as sns
import matplotlib.pyplot as plt
import itertools

import pandas as pd
from scipy.stats import spearmanr, pearsonr, chi2_contingency, normaltest, mstats
from sklearn.metrics import mutual_info_score
import warnings
from utils.utils import optuna_objective, plot_pairwise_distributions
from utils.models import VAE

warnings.filterwarnings("ignore")



cluster_name = "cluster0"

df = pd.read_csv(f"/home/haoyuan/influencer/cluster0/{cluster_name}_attributes.csv",encoding='UTF-8')

df = df.drop("user_id", axis=1)

# Load and preprocess the data
input_dim = len(df.columns)

data = pd.read_csv(f"/home/haoyuan/influencer/cluster0/{cluster_name}_attributes.csv",encoding='UTF-8')
data.drop('user_id', axis=1, inplace=True)
column_names = data.columns

data = data.to_numpy(dtype=np.float32)
log_data = np.log1p(data + 1e-10)
scaler = MinMaxScaler()
normalized_data = scaler.fit_transform(log_data)

input_dim = normalized_data.shape[1]

# Split the data once (outside the objective function)
train_data, test_data = train_test_split(normalized_data, test_size=0.2, random_state=42)
train_data = tf.convert_to_tensor(train_data, dtype=tf.float32)
test_data = tf.convert_to_tensor(test_data, dtype=tf.float32)

reconstruction_param = 0.8
epochs=500

current_folder = "/home/haoyuan/influencer/cluster0/"
cluster_name = "cluster0"  # Update if needed
study_name = f"{cluster_name}_VAE"  # Study name does not need the full path
storage_path = f"sqlite:///{current_folder}{cluster_name}_VAE.db"  # Full path for SQLite file


study = optuna.create_study(
    study_name=study_name,  # Use only the name, not the full path
    direction="minimize",  # Set the optimization direction
    storage=storage_path,  # Full path to the SQLite database
    load_if_exists=True    # Load existing study if it exists
)
study.optimize(
    lambda trial: optuna_objective(
        trial,
        train_data,
        epochs=epochs,
        model_type="VAE",
        input_dim=input_dim,
        reconstruction_param=reconstruction_param
    ),
    n_trials=60,  # Number of trials
    n_jobs=-1     # Parallelize across all available CPU cores
)
# Load the study (optional, if needed again later)
study = optuna.load_study(
    study_name=study_name,  # Use only the name, not the full path
    storage=storage_path    # Full path to the SQLite database
)
print("Best hyperparameters:", study.best_params)


best_hyperparams = study.best_params


model = VAE(
    input_dim=input_dim,
    latent_dim=best_hyperparams['latent_dim'],
    reconstruction_param=reconstruction_param,
    encoder_units=best_hyperparams['encoder_units'],
    decoder_units=best_hyperparams['encoder_units']  # Symmetrical
)

model.compile(optimizer=optimizers.Adam(learning_rate=best_hyperparams["learning_rate"]))

history = model.fit(
    train_data,
    epochs=epochs,
    batch_size=best_hyperparams['batch_size'],
    verbose=0
)

model.save(f"/home/haoyuan/influencer/cluster0/{cluster_name}_users.keras")

test_reconstructed = model.reconstruct_agents(test_data)

# Display original and reconstructed samples for comparison
print("Original test sample:\n", np.round(test_data[:3], 2))
print("Reconstructed test sample:\n", np.round(test_reconstructed[:3], 2))

# Plot a subset of the probability distributions
plot_pairwise_distributions("Test Data", test_data[:, :6], test_reconstructed[:, :6], column_names)

new_agents = model.generate_new_agents(num_samples=test_data.shape[0])
plot_pairwise_distributions("Generated Agents", test_data[:, :6], new_agents[:, :6], column_names)

new_agents = model.generate_new_agents(num_samples=normalized_data.shape[0]).numpy()
plot_pairwise_distributions("Generated Agents", normalized_data[:, :6], new_agents[:, :6], column_names)
numpy_array = new_agents
log_data_reverted = scaler.inverse_transform(numpy_array)
users_atributes = np.expm1(log_data_reverted) - 1e-10

df = pd.DataFrame(users_atributes, columns=column_names)
df.index.name = 'user_id'
df = df.astype({'Followers (Millions)':'int64','Following':'int64','QRT':'int64','RT':'int64','tweet':'int64'})
df.to_csv(f"/home/haoyuan/influencer/cluster0/synthetic_{cluster_name}_atributes.csv")
