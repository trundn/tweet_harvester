# Library for working with CouchDB
import couchdb
# The harvest constant definitions
import constants

class CouchDBConnection(object):
    def __init__(self, database_name, connection_string, lock):
        self.lock = lock
        self.database_name = database_name
        if (self.database_name is None or self.database_name == ""):
            self.database_name = constants.TWEETS_DATABASE
        self.connection_string = connection_string
        self.server = couchdb.Server(self.connection_string)

    def init_database(self):
        
        if self.database_name in self.server:
            self.database = self.server[self.database_name]
        else:
            self.database = self.server.create(self.database_name)

    def write_tweet(self, tweet_content):
        if ((tweet_content is not None) and isinstance(tweet_content, dict)):
            tweet_id = tweet_content['_id']
            with self.lock:
                if ((tweet_id is not None) and (tweet_id not in self.database)):
                    self.database.save(tweet_content)