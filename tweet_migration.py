# Message Passing Interface (MPI) standard library
from mpi4py import MPI
# Constructs higher-level threading interfaces on top of the lower level _thread module
import threading
# Using library for system dependent functionalities
import os
# Library for command line argument parsing
import sys, getopt
# JSON parsing library
import json
# The harvest constant definitions
import constants
# Using class for loading all needed configurations
from config_loader import ConfigurationLoader
# Utility for working with CouchDB
from couchdb_connection import CouchDBConnection
# Provides the utility functions
from helper import Helper

def print_usage():
    print('Usage is: tweet_migration.py -j <the json data file>')
    print('                             -f <the tweet filter configuration file>')
    print('                             -d <the database configuration file>')

def parse_arguments(argv):
    # Initialise local variables
    data_path = ""
    filter_config_path = ""
    database_config_path = ""

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv, constants.CMD_LINE_MIGRATION_ARGUMENTS)
    except getopt.GetoptError as error:
        print("Failed to parse comand line arguments. Error: %s" %error)
        print_usage()
        sys.exit(2)

    # Extract argument values
    for opt, arg in opts:
        if opt == constants.HELP_ARGUMENT:
            print_usage()
            sys.exit()
        if opt in (constants.DATA_CONFIG_ARGUMENT):
            data_path = arg
        elif opt in (constants.FILTER_CONFIG_ARGUMENT):
            filter_config_path = arg
        elif opt in (constants.DATABASE_CONFIG_ARGUMENT):
            database_config_path = arg

    # Return all arguments
    return data_path, filter_config_path, database_config_path

def process_twitter_data(rank, processor_size, data_path, config_loader):
    if os.path.exists(data_path):
        helper = Helper()
        lock = threading.Lock()
        db_connection = CouchDBConnection(config_loader.couchdb_database_name, config_loader.couchdb_connection_string, lock)
        db_connection.init_database()

        with open(data_path, encoding = constants.UTF8_ENCODING) as fstream:
            try:
                for i, line in enumerate(fstream):
                    if (i % processor_size == rank):
                        if (i > 0):
                            line = line.replace(constants.JSON_NEW_LINE_STRING,"")
                            
                            try:
                                # Load tweet into json document
                                tweet = json.loads(line)
                                tweet_doc = tweet[constants.JSON_DOCUMENT]

                                # Extract full text
                                tweet_text = ""
                                if (constants.JSON_FULL_TEXT_PROP in tweet_doc):
                                    tweet_text = tweet_doc[constants.JSON_FULL_TEXT_PROP]
                                else:
                                    tweet_text = tweet_doc[constants.JSON_TEXT_PROP]

                                match_track_filter = helper.is_match(tweet_text.lower(), config_loader.track)
                                # Extract coordinator
                                source = ""
                                coordinates = []

                                if tweet_doc[constants.GEO] \
                                    and (constants.COORDINATES in tweet_doc[constants.GEO]) \
                                    and tweet_doc[constants.GEO][constants.COORDINATES]:
                                        source = constants.GEO
                                        coordinates = [tweet_doc[constants.GEO][constants.COORDINATES][1], tweet_doc[constants.GEO][constants.COORDINATES][0]]
                                elif tweet_doc[constants.COORDINATES] \
                                    and (constants.COORDINATES in tweet_doc[constants.COORDINATES]) \
                                    and tweet_doc[constants.COORDINATES][constants.COORDINATES]:
                                        source = constants.COORDINATES
                                        coordinates = tweet_doc[constants.COORDINATES][constants.COORDINATES]
                                elif tweet_doc[constants.PLACE] \
                                    and (constants.BOUNDING_BOX in tweet_doc[constants.PLACE]) \
                                    and isinstance(tweet_doc[constants.PLACE][constants.BOUNDING_BOX], dict) \
                                    and (constants.COORDINATES in tweet_doc[constants.PLACE][constants.BOUNDING_BOX]) \
                                    and tweet_doc[constants.PLACE][constants.BOUNDING_BOX][constants.COORDINATES]:
                                        source = constants.PLACE
                                        temp_coordinates = tweet_doc[constants.PLACE][constants.BOUNDING_BOX][constants.COORDINATES][0]

                                        lng = (temp_coordinates[0][0] + temp_coordinates[2][0])/2
                                        lat = (temp_coordinates[0][1] + temp_coordinates[2][1])/2

                                        coordinates = [lng, lat]

                                inside_polygon = False
                                if (coordinates):
                                    inside_polygon = config_loader.within_geometry_filters(coordinates)
                                else:
                                    user_location = tweet_doc['user']['location']
                                    if (user_location is not None):
                                        inside_polygon = helper.is_match(user_location.lower(), config_loader.user_location_filters)

                                if (inside_polygon):
                                    emotions = helper.extract_emotions(tweet_text)
                                    word_count, pronoun_count = helper.extract_word_count(tweet_text)
                                    politician_type = config_loader.get_politician_type(tweet_doc['user']['screen_name'])
                                            
                                    filter_data = {'_id' : tweet_doc['id_str'], 'created_at' : tweet_doc['created_at'],\
                                            'text' : tweet_text, 'user' : tweet_doc['user']['screen_name'],\
                                            'match_track_filter': match_track_filter, 'politician' : politician_type, 'calculated_coordinates' : coordinates, \
                                            'coordinates_source' : source, 'emotions': emotions, \
                                            'tweet_wordcount' : word_count, "pronoun_count" : pronoun_count,\
                                            'raw_data' : json.dumps(tweet_doc)}

                                    print(f"{tweet_doc['id_str']}    {emotions}    {word_count}    {pronoun_count}")
                                    db_connection.write_tweet(filter_data)

                            except ValueError as error:
                                print("Failed to decode JSON content from [%d] rank. Error: %s" %(rank, error))
                                print("Processed tweet: %s" %line)
                        else:
                            print("Ignore header line.")
            except Exception as exception:
                print("Error occurred processing twitter data from [%d] rank. Exception: %s" %(rank, exception))
    else:
        print("The twitter data file does not exist. Path: %s", data_path)

def main(args):
    # Get the main communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    processor_size = comm.Get_size()

    # Parse command line arguments to get twitter data file and database config path
    data_path, filter_config_path, database_config_path = parse_arguments(args)

    if (not (data_path and filter_config_path and database_config_path)):
        print("The required parameters are not specified in command line arguments.")
        print_usage()
    else:
        if(database_config_path):
             # Instantiate the configuration loader
            config_loader = ConfigurationLoader("", filter_config_path, database_config_path)
            config_loader.load_filter_config()
            config_loader.load_couchdb_config()

            process_twitter_data(rank, processor_size, data_path, config_loader)

# Run the actual program
if __name__ == "__main__":
    main(sys.argv[1:])