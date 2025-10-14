from tweetnlp import TopicClassification, Sentiment, Irony, Hate, Offensive, Emotion, NER
import pandas as pd
import numpy as np
import logging
import os
import pickle
import re

topic_model = TopicClassification()
sentiment_model = Sentiment()
irony_model = Irony()
hate_model = Hate()
offensive_model = Offensive()
emotion_model = Emotion()
entity_model = NER()

def setup_logger(cluster_name):
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    fhandler = logging.FileHandler(filename=f'/home/haoyuan/influencer/cluster0/{cluster_name}_tweet_classification.log', mode='w')
    formatter = logging.Formatter('%(asctime)s %(message)s')
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.DEBUG)
    return logger

class TweetAnalysis:
    def __init__(self, cluster_name):
        self.data = []
        self.cluster_name = cluster_name
        self.checkpoint_file = f"/home/haoyuan/influencer/cluster0/{cluster_name}_checkpoint.pkl"
        self.last_processed_index = 0

    def extract_probabilities(self, tweet):
        cleaned_tweet = self.clean_text(tweet)
        logger.info(f"Cleaned Tweet: {cleaned_tweet}")

        topic_probs = topic_model.topic(cleaned_tweet, return_probability=True)['probability']
        logger.info(f"Topic Probabilities: {topic_probs}")

        sentiment_probs = sentiment_model.sentiment(cleaned_tweet, return_probability=True)['probability']
        logger.info(f"Sentiment Probabilities: {sentiment_probs}")

        emotion_probs = emotion_model.emotion(cleaned_tweet, return_probability=True)['probability']
        logger.info(f"Emotion Probabilities: {emotion_probs}")

        irony_prob = irony_model.irony(cleaned_tweet, return_probability=True)['probability']['irony']
        logger.info(f"Irony Probability: {irony_prob}")

        hate_prob = hate_model.hate(cleaned_tweet, return_probability=True)['probability']['HATE']
        logger.info(f"Hate Probability: {hate_prob}")

        offensive_prob = offensive_model.offensive(cleaned_tweet, return_probability=True)['probability']['offensive']
        logger.info(f"Offensive Probability: {offensive_prob}")

        return {
        'topic_probs': topic_probs,
        'sentiment_probs': sentiment_probs,
        'emotion_probs': emotion_probs,
        'irony_prob': irony_prob,
        'hate_prob': hate_prob,
        'offensive_prob': offensive_prob,
        }


    def clean_text(self, text):
        """Clean text by removing special characters and normalizing spaces"""
        text = re.sub(r'[^\x00-\x7F]+', '', text)  
        text = re.sub(r'http\S+|www.\S+', '', text)  
        text = ' '.join(text.split())  
        return text.strip()

    def save_checkpoint(self):
        checkpoint_data = {
            'data': self.data,
            'last_processed_index': self.last_processed_index,
        }
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(checkpoint_data, f)
        logger.info(f"Checkpoint saved at index {self.last_processed_index}")

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'rb') as f:
                checkpoint_data = pickle.load(f)
                self.data = checkpoint_data['data']
                self.last_processed_index = checkpoint_data['last_processed_index']
            logger.info(f"Checkpoint loaded. Resuming from index {self.last_processed_index}")
        else:
            logger.info("No checkpoint found. Starting from scratch.")

    def analyze_tweets(self, tweets):
        self.load_checkpoint()

        for i, (user_id, tweet) in enumerate(tweets[self.last_processed_index:], start=self.last_processed_index):
            if i % 1000 == 0 and i >= self.last_processed_index:
                self.save_checkpoint()
            try:
                # Validate and convert user_id to integer
                user_id = int(float(user_id))  # Handle scientific notation or numeric strings
                probabilities = self.extract_probabilities(tweet)
                probabilities['user_id'] = user_id
                probabilities['tweet'] = tweet
                self.data.append(probabilities)
            except ValueError:
                logger.error(f"Invalid user_id: {user_id} at index {i}. Skipping.")
            except Exception as e:
                logger.error(f"Error processing tweet {i}: {tweet}\n{str(e)}")
            self.last_processed_index = i + 1

        self.save_checkpoint()

    def flatten_data(self):
        flat_data = []
        for entry in self.data:
            flat_entry = {}
            for key, value in entry.items():
                if 'probs' in key:
                    for sub_key, sub_value in value.items():
                        flat_entry[f"{key}_{sub_key}"] = sub_value
                else:
                    flat_entry[key] = value
            flat_data.append(flat_entry)

        df = pd.DataFrame(flat_data)
        df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce')
        df = df.dropna(subset=['user_id']).astype({'user_id': 'int64'})
        
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        df[numeric_columns] = df[numeric_columns].applymap(lambda x: 0 if x < 0.2 else x)
        df = round(df, 2)
        df.set_index(["user_id", "tweet"]).to_csv(f"/home/haoyuan/influencer/cluster0/{cluster_name}_tweets_probabilities.csv")
        return df

    def aggregate_by_user(self):
        df = self.flatten_data()
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        grouped_mean = df.groupby('user_id')[numeric_columns].mean()
        grouped_mean = round(grouped_mean, 5)

    # Ensure no duplicate 'user_id' column when resetting the index
        if 'user_id' in grouped_mean.columns:
            grouped_mean = grouped_mean.drop(columns=['user_id'])
    
        grouped_mean = grouped_mean.reset_index()  # Reset index to include 'user_id' as a column
        logger.info("Aggregation by user successful")
        return grouped_mean


# Main execution
cluster_name = "cluster0"
logger = setup_logger(cluster_name)

# Load data and ensure proper types
tweets = pd.read_csv(f"/home/haoyuan/influencer/cluster0/{cluster_name}_tweets.csv", encoding='utf-8')
tweets['user_id'] = pd.to_numeric(tweets['user_id'], errors='coerce')
tweets = tweets.dropna(subset=['user_id']).astype({'user_id': 'int64'})

users_stats = pd.read_csv(f"/home/haoyuan/influencer/cluster0/{cluster_name}_statistics.csv")

# Run analysis
tweet_analysis = TweetAnalysis(cluster_name)
tweet_analysis.analyze_tweets(tweets[['user_id', 'text']].values.tolist())
aggregated_probabilities = tweet_analysis.aggregate_by_user()

# Join with user stats and save
users_attributes = users_stats.join(aggregated_probabilities.set_index('user_id'), on='user_id')
users_attributes.to_csv(f"/home/haoyuan/influencer/cluster0/{cluster_name}_attributes.csv")


