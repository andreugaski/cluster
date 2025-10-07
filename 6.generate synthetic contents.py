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

from utils.utils import optuna_objective, plot_single_distributions
from utils.models import CVAE

warnings.filterwarnings("ignore")

cluster_name = "cluster0"
users_df = pd.read_csv(f"/home/haoyuan/influencer/cluster0/{cluster_name}_atributes.csv")

tweets_df = pd.read_csv(f"/home/haoyuan/influencer/cluster0/{cluster_name}_tweets_probabilities.csv")
column_names = tweets_df.columns.to_list()[2::]
tweets_n_columns = len(column_names)

merged_df = tweets_df.merge(users_df, on="user_id", suffixes=['_tweet', '_user'])
merged_df.drop(['user_id', 'tweet_tweet'], axis=1, inplace=True)

log_merged_df = np.log1p(merged_df + 1e-10)
tweet_data = log_merged_df.iloc[:, :tweets_n_columns].to_numpy(dtype=np.float32)
user_data = log_merged_df.iloc[:, tweets_n_columns:].to_numpy(dtype=np.float32)

tweet_scaler = MinMaxScaler()
tweet_data = tweet_scaler.fit_transform(tweet_data)
user_data = MinMaxScaler().fit_transform(user_data)

# Get the input dimensions
tweet_dim = tweet_data.shape[1]
user_dim = user_data.shape[1]

# Create a dataset
dataset = tf.data.Dataset.from_tensor_slices(((tweet_data, user_data), tweet_data))
dataset = dataset.shuffle(buffer_size=100)

train_samples = int(0.8 * len(tweet_data))
train_dataset = dataset.take(train_samples).batch(32)
test_dataset = dataset.skip(train_samples).batch(32)

print(len(train_dataset))
print(len(test_dataset))

reconstruction_param = 0.8
epochs = 100

current_folder = "/home/haoyuan/influencer/cluster0/"
cluster_name = "cluster0"  
study_name = f"{cluster_name}_CVAE"  # Study name
storage_path = f"sqlite:///{current_folder}{study_name}.db"  # Full path for SQLite database

# Create RDBStorage with engine arguments
storage = optuna.storages.RDBStorage(
    url=storage_path,
    engine_kwargs={"connect_args": {"timeout": 30}}
)

# Create or load the Optuna study
study = optuna.create_study(
    direction="minimize",      # Set the optimization direction
    study_name=study_name,     # Use the study name
    storage=storage,           # Use the RDBStorage instance
    load_if_exists=True        # Load existing study if it exists
)

# Optimize the study
study.optimize(
    lambda trial: optuna_objective(
        trial,
        train_dataset,
        model_type="CVAE",
        epochs=epochs,
        tweet_dim=tweet_dim,
        user_dim=user_dim,
        reconstruction_param=reconstruction_param
    ),
    n_trials=60,  # Number of trials
    n_jobs=-1     # Parallelize across all available CPU cores
)

current_folder = "/home/haoyuan/influencer/cluster0/"
cluster_name = "cluster0"  
study_name = f"{cluster_name}_CVAE"  # Study name
storage_path = f"sqlite:///{current_folder}{study_name}.db"  # Full path for SQLite database

# Load the Optuna study
study = optuna.load_study(
    study_name=study_name,  # Use the study name
    storage=storage_path    # Full path to the SQLite database
)

# Print the best hyperparameters
print("Best hyperparameters:", study.best_params)

best_hyperparams = study.best_params
#best_hyperparams = {'latent_dim': 5, 'learning_rate': 8.236940517619239e-05, 'batch_size': 128, 'encoder_units': 256}

model = CVAE(
    tweet_dim=tweet_dim,
    user_dim=user_dim,
    latent_dim=best_hyperparams['latent_dim'],
    reconstruction_param=reconstruction_param,
    encoder_units=best_hyperparams['encoder_units'],
    decoder_units=best_hyperparams['encoder_units']
)

model.compile(optimizer=optimizers.Adam(learning_rate=best_hyperparams["learning_rate"]))

history = model.fit(
    train_dataset,
    epochs=epochs,
    batch_size=best_hyperparams['batch_size'],
    verbose=0
)

plt.plot(history.history['loss'],'b', label="Total Loss")
plt.xlabel("Epochs")
plt.ylabel("Loss")
plt.legend()
plt.title("Training Losses Over Epochs")
plt.show()

model.save(f"/home/haoyuan/influencer/cluster0/{cluster_name}_tweets.keras")

model = tf.keras.models.load_model(f"/home/haoyuan/influencer/cluster0/{cluster_name}_tweets.keras", custom_objects={"CVAE": CVAE})

original_test_users, original_test_tweets, reconstruction_test_tweets = model.reconstruct_tweets(test_dataset)
print(f"Original: {np.round(original_test_tweets[:3],2)}")
print(f"Reconstructed: {np.round(reconstruction_test_tweets[:3], 2)}")

validate_user = original_test_users[1]
matching_indices = np.where((original_test_users == validate_user).all(axis=1))[0]

print('User Statistics: \n', validate_user[::5])
print('User Preferences: \n', validate_user[5::])
synthetic_tweets = model.generate_tweets(validate_user, num_samples=int(len(matching_indices)*2))
        
plot_single_distributions(synthetic_tweets, original_test_tweets[matching_indices], user_prefs=validate_user[5::], column_names=column_names)

def generate_tweets_dataset(model, user_data):
    user_data = tf.convert_to_tensor(user_data)
    z = tf.keras.backend.random_normal(shape=(len(user_data), model.latent_dim))
    synthetic_tweets_dataset = model.decoder([z, user_data])
    return synthetic_tweets_dataset.numpy()

print(original_test_users.shape)
synthetic_tweets_dataset = generate_tweets_dataset(model, original_test_users)

plot_single_distributions(synthetic_tweets_dataset, original_test_tweets, column_names=column_names)

synthetic_users_df = pd.read_csv(f"/home/haoyuan/influencer/cluster0/synthetic_{cluster_name}_atributes.csv")
synthetic_users_df = synthetic_users_df.loc[synthetic_users_df.index.repeat(synthetic_users_df['tweet']/2)].assign(fifo_qty=1).reset_index(drop=True)
synthetic_users_df_column = synthetic_users_df['user_id']
synthetic_users_df.drop(['user_id', 'fifo_qty'], axis=1, inplace=True)
log_synthetic_users_df = np.log1p(synthetic_users_df + 1e-10).to_numpy(dtype=np.float32)
synthetic_users_data = tweet_scaler.fit_transform(log_synthetic_users_df)

def generate_tweets_dataset(model, user_data):
    user_data = tf.convert_to_tensor(user_data)
    z = tf.keras.backend.random_normal(shape=(len(user_data), model.latent_dim))
    synthetic_tweets_dataset = model.decoder([z, user_data])
    return synthetic_tweets_dataset.numpy()

synthetic_tweets_data = generate_tweets_dataset(model, synthetic_users_data)
df = pd.DataFrame(synthetic_tweets_data, columns=column_names)
df.insert(0, 'user_id', synthetic_users_df_column)
df.to_csv(f"/home/haoyuan/influencer/cluster0/synthetic_{cluster_name}_tweets.csv", index=False)
plot_single_distributions(synthetic_tweets_dataset, tweet_data, column_names=column_names)

