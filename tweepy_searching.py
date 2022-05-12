# Message Passing Interface (MPI) standard library
from mpi4py import MPI
# Constructs higher-level threading interfaces on top of the lower level _thread module
import threading
# Provides various time-related functions
import time
# An easy-to-use Python library for accessing the Twitter API
import tweepy
# The harvest constant definitions
import constants
# Using class for creating Tweepy API
from api_factory import APIFactory
# Provides the utility functions
from helper import Helper
# Uility to write tweet data to CounchDB
from tweet_writer import TweetWriter

class SearchingAPIThread(threading.Thread):
    def __init__(self, config_loader, writer):
        threading.Thread.__init__(self)
        self.writer = writer
        self.tweepy_api = None
        self.config_loader = config_loader

    def run(self):
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        processor_size = comm.Get_size()

        helper = Helper()

        users = self.config_loader.get_searching_users(rank, processor_size)
        api_key, api_secret_key, access_token, access_token_secret = self.config_loader.get_searching_authen(rank)

        api_factory = APIFactory()
        self.tweepy_api = api_factory.create_api(api_key, api_secret_key,
                                access_token, access_token_secret)

        # Query time line for all configured users
        if (users is not None):
            for user_id in users:
                # Get all posible user tweets (max: 3200 tweets for each uer)
                all_tweets = helper.get_all_tweets(self.tweepy_api, user_id)
                # Write all tweets to counchdb
                self.writer.write_to_counchdb(all_tweets, True)
                # Sleep 2 seconds
                time.sleep(constants.TWO_SECONDS)

            # Query time line for all followers
            for user_id in users:
                for follower in helper.get_followers(self.tweepy_api, user_id, -1):
                    if (helper.is_match(follower.location.lower(), self.config_loader.user_location_filters)):
                        all_followers_tweets = helper.get_all_tweets(self.tweepy_api, follower.screen_name)

                        # Only get historic tweets having coordiantes
                        processed_tweets = helper.filer_tweets_with_coordinates(all_followers_tweets)
                        # Write all tweets to counchdb
                        if (processed_tweets):
                            self.writer.write_to_counchdb(processed_tweets)