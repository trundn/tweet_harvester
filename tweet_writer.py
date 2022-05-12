# Constructs higher-level threading interfaces on top of the lower level _thread module
import threading
# Provides the utility functions
from helper import Helper
# Utility for working with CouchDB
from couchdb_connection import CouchDBConnection
# Manage all runnable jobs
from job_executor import JobExecutor
# Reponsible for writer tweets to CouchDB
from writer_job import WriterJob

class TweetWriter(object):
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.lock = threading.Lock()
        self.database_connection = CouchDBConnection(
            self.config_loader.couchdb_database_name,
            self.config_loader.couchdb_connection_string, self.lock)
        self.database_connection.init_database()
        self.threadpool_job_executor = JobExecutor(-1, 50, 50000)

    def write_to_counchdb(self, all_tweets, ignore_coordinates_filter = False):
        if (all_tweets):
            job = WriterJob(all_tweets, self.database_connection, self.config_loader, ignore_coordinates_filter)
            self.threadpool_job_executor.queue(job)
