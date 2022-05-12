# JSON parsing library
import json
# Provides access to some variables used or maintained by the interpreter
import sys, traceback
# Constructs higher-level threading interfaces on top of the lower level _thread module
import threading
# Provides various time-related functions
import time
# Implements pseudo-random number generators for various distributions
import random
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

# Override tweepy.StreamListener to add logic to on_status
class StreamListener(tweepy.StreamListener):
    def __init__(self, tweepy_api, config_loader, writer):
        super(StreamListener, self).__init__()
        self.writer = writer
        self.tweepy_api = tweepy_api
        self.config_loader = config_loader
        self.helper = Helper()

    def on_status(self, status):
        # Write current tweet to counchdb
        self.writer.write_to_counchdb([status])

        # Try to query time line for this user
        all_tweets = self.helper.get_all_tweets(self.tweepy_api, status.user.screen_name)

        # Write all tweets to counchdb
        if (all_tweets):
            self.writer.write_to_counchdb(all_tweets)

    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")
        sys.exit()

class StreamingAPIThread(threading.Thread):
    def __init__(self, config_loader, writer):
        threading.Thread.__init__(self)
        self.tweepy_api = None
        self.writer = writer
        self.config_loader = config_loader
        
        # The initial backoff time after a disconnection occurs, in seconds
        self.default_backoff_time = 1
        self.minimum_backoff_time = 1
        # The maximum backoff time before reseting to default value
        self.maximum_backoff_time = 32

    def run(self):
        # Create tweepy API
        api_factory = APIFactory()
        self.tweepy_api = api_factory.create_api(self.config_loader.api_key,
                                self.config_loader.api_secret_key,
                                self.config_loader.access_token,
                                self.config_loader.access_token_secret)

        while(True):
            try:
                # Instantiate the stream listener
                listener = StreamListener(self.tweepy_api, self.config_loader, self.writer)

                # Streaming and filtering tweet data
                stream = tweepy.Stream(auth = self.tweepy_api.auth,
                    listener = listener, tweet_mode = constants.TWEET_MODE)

                # Filer tweets based on configured locations
                locations = self.config_loader.get_streaming_locations()
                if (locations is not None):
                    stream.filter(locations = locations)
            except MemoryError as ex:
                print("Encountered the memory exeption. Please restart harvester process.")
                break
            except Exception as ex:
                print(f"Exception occurred during tweet streaming. {ex}")
                
                if self.minimum_backoff_time > self.maximum_backoff_time:
                    self.minimum_backoff_time = self.default_backoff_time

                delay = self.minimum_backoff_time + random.randint(0, 1000) / 1000.0

                print(f"Trying to reconect after {delay} seconds")
                time.sleep(delay)
                self.minimum_backoff_time *= 2
