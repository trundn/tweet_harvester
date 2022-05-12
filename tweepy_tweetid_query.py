# Provides various time-related functions
import time
# Implements pseudo-random number generators for various distributions
import random
# Message Passing Interface (MPI) standard library
from mpi4py import MPI
# Using library for system dependent functionalities
import os
# Constructs higher-level threading interfaces on top of the lower level _thread module
import threading
# An easy-to-use Python library for accessing the Twitter API
import tweepy
# Using class for creating Tweepy API
from api_factory import APIFactory
# Uility to write tweet data to CounchDB
from tweet_writer import TweetWriter
# Manage all runnable jobs
from job_executor import JobExecutor
# Responsible for processing tweet id dataset
from tweetid_process_job import TweetIdProcessJob
# The harvest constant definitions
import constants

class TweetIdQueryThread(threading.Thread):
    def __init__(self, config_loader, writer):
        threading.Thread.__init__(self)
        self.writer = writer
        self.config_loader = config_loader
        
         # The initial waiting time after a disconnection occurs, in seconds
        self.default_waiting_time = 1
        self.minimum_waiting_time = 1
        # The maximum waiting time before reseting to default value
        self.maximum_waiting_time = 64

    def run(self):
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        processor_size = comm.Get_size()

        dataset_folders = self.config_loader.get_tweeid_dataset(rank, processor_size)
        
        if (dataset_folders is not None and len(dataset_folders) > 0):
            # Get exectuor configuration to initialise thread pool job exectuor
            num_thread, thread_names = self.config_loader.get_tweetid_executors_config(rank)
            self.threadpool_job_executor = JobExecutor(num_thread, num_thread, 5000)
            
            # Update thread name
            for index in range(num_thread):
                self.threadpool_job_executor.set_thread_name(index, thread_names[index])

            # Get tweeter authentication keys for tweetid repo harvest mode
            authen_info = self.config_loader.get_tweetid_authentications(rank, num_thread)
            
            # Instantiate tweepy apis
            tweepy_apis = {}
            api_factory = APIFactory()
            for key, value in authen_info.items():
                tweepy_api = api_factory.create_api(value.api_key, value.api_secret_key,
                                    value.access_token, value.access_token_secret)
                tweepy_apis[key] = tweepy_api

            for folder in dataset_folders:
                print(f"Processing tweetid data set folder [{folder}]")
                job = TweetIdProcessJob(tweepy_apis, self.config_loader, self.writer, folder)
                self.threadpool_job_executor.queue(job)

            while(self.threadpool_job_executor.is_any_thread_alive() is True):
                if self.minimum_waiting_time > self.maximum_waiting_time:
                    self.minimum_waiting_time = self.default_waiting_time

                delay = self.minimum_waiting_time + random.randint(0, 1000) / 1000.0

                time.sleep(delay)
                self.minimum_waiting_time *= 2

            print("Finished harvesting from tweetid datasets.")

