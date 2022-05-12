# Using library for system dependent functionalities
import os
# Library for command line argument parsing
import sys, getopt
# Using class for loading all needed configurations
from config_loader import ConfigurationLoader
# Thread for performing tweepy streaming API
from tweepy_streaming import StreamingAPIThread
# Thread for performing tweepy searching API
from tweepy_searching import SearchingAPIThread
# Thread for performing tweetid get status API
from tweepy_tweetid_query import TweetIdQueryThread
# Uility to write tweet data to CounchDB
from tweet_writer import TweetWriter
# The harvest constant definitions
import constants

def print_usage():
    print('Usage is: tweet_harvester.py -a <the authentication configuration file>')
    print('                             -f <the tweet filter configuration file>')
    print('                             -d <the database configuration file>')
    print('                             -m <the harvest mode: all, stream, search, tweetid>')

def parse_arguments(argv):
    # Initialise local variables
    authen_config_path = ""
    filter_config_path = ""
    database_config_path = ""
    harvest_mode = ""

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv, constants.CMD_LINE_DEFINED_ARGUMENTS)
    except getopt.GetoptError as error:
        print("Failed to parse comand line arguments. Error: %s" %error)
        print_usage()
        sys.exit(2)

    # Extract argument values
    for opt, arg in opts:
        if opt == constants.HELP_ARGUMENT:
            print_usage()
            sys.exit()
        if opt in (constants.AUTHEN_CONFIG_ARGUMENT):
            authen_config_path = arg
        elif opt in (constants.FILTER_CONFIG_ARGUMENT):
            filter_config_path = arg
        elif opt in (constants.DATABASE_CONFIG_ARGUMENT):
            database_config_path = arg
        elif opt in (constants.HARVEST_MODE_ARGUMENT):
            harvest_mode = arg.lower()
            if (harvest_mode != constants.ALL_HARVEST_MODE and 
                harvest_mode != constants.STREAM_HARVEST_MODE and
                harvest_mode != constants.SEARCH_HARVEST_MODE and
                harvest_mode != constants.TWEETID_HARVEST_MODE):
                print_usage()
                sys.exit()

    # Return all arguments
    return authen_config_path, filter_config_path, database_config_path, harvest_mode

def main(args):
    # Parse command line arguments to get the authentication and filter configuration files
    authen_config_path, filter_config_path, database_config_path, harvest_mode = parse_arguments(args)

    # Instantiate the configuration loader
    config_loader = ConfigurationLoader(authen_config_path, filter_config_path, database_config_path)
    config_loader.load_authentication_config()
    config_loader.load_filter_config()
    config_loader.load_couchdb_config()

    # Instantiate tweet writer
    writer = TweetWriter(config_loader)

    # Start tweeter streaming API thread
    if (harvest_mode == constants.ALL_HARVEST_MODE or
        harvest_mode == constants.STREAM_HARVEST_MODE):
        streaming = StreamingAPIThread(config_loader, writer)
        streaming.start()

    # Start tweeter searching API thread
    if (harvest_mode == constants.ALL_HARVEST_MODE or
        harvest_mode == constants.SEARCH_HARVEST_MODE):
        searching = SearchingAPIThread(config_loader, writer)
        searching.start()

    # Start tweetid querying thread
    if (harvest_mode == constants.ALL_HARVEST_MODE or
        harvest_mode == constants.TWEETID_HARVEST_MODE):
        querying = TweetIdQueryThread(config_loader, writer)
        querying.start()

# Run the actual program
if __name__ == "__main__":
    main(sys.argv[1:])
