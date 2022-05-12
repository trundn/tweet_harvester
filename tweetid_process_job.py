# Provides various time-related functions
import time
# Import module sys to get the type of exception
import sys
# Using library for system dependent functionalities
import os
# Implements some useful functions on pathnames
from pathlib import Path
# Provides a standard interface to extract, format and print stack traces 
import traceback
# Data manipulation tool.
import pandas as pd
# Provide a common protocol for objects that wish to execute code while they are active
from runnable import Runnable
# Provides the utility functions
from helper import Helper
# Uility to write tweet data to CounchDB
from tweet_writer import TweetWriter
# The harvest constant definitions
import constants

class TweetIdProcessJob(Runnable):
    def __init__(self, tweepy_apis, config_loader, writer, dataset_folder):
        self.own_tweepy_api = None
        self.tweepy_apis = tweepy_apis
        self.config_loader = config_loader
        self.writer = writer
        self.dataset_folder = dataset_folder
        self.helper = Helper()

    def lookup_tweets(self, tweet_ids):
        tweet_count = len(tweet_ids)

        try:
            for i in range((tweet_count // 100) + 1):
                all_tweets = []
                # Catch the last group if it is less than 100 tweets
                last_index = min((i + 1) * 100, tweet_count)
                # Sleep 2 seconds to avoid rate limit issue
                time.sleep(constants.TWO_SECONDS)
                full_tweets = self.own_tweepy_api.statuses_lookup(tweet_ids[i * 100 : last_index])

                # Check if tweet is in configured user filter locations
                processed_tweets = self.helper.filer_tweets_with_coordinates(full_tweets, True)

                # Write all tweets to counchdb
                if (processed_tweets):
                    self.writer.write_to_counchdb(processed_tweets)

            return full_tweets
        except:
            print("Failed to loopkup statuses.")
            traceback.print_exc(file = sys.stdout)

    def run(self):
        try:
            self.own_tweepy_api = self.tweepy_apis[self.handled_thread_name]
            for pth, dirs, files in os.walk(self.dataset_folder):
                for file_name in files:
                    data_folder = Path(self.dataset_folder)
                    data_path = data_folder / file_name

                    if os.path.exists(data_path):
                        print(f"Querying tweets from {data_path}")
                        data_frame = pd.read_csv(data_path, delimiter = constants.TAB_CHAR, engine='c')
                        self.lookup_tweets(data_frame[constants.TWEET_ID_COLUMN].values.tolist())
        except:
            print("Exception", sys.exc_info()[0], "occurred.")
            traceback.print_exc(file = sys.stdout)