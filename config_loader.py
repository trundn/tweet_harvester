# JSON parsing library
import json
# Using library for system dependent functionalities
import os
# Implements some useful functions on pathnames
from pathlib import Path
from shapely.geometry import Point
from shapely.geometry import Polygon, MultiPolygon
# Contains the tweet authentication keys
from auth_info import AuthenticationInfo
# The harvest constant definitions
import constants

UTF8_ENCODING = "utf-8"

class ConfigurationLoader(object):
    def __init__(self, authen_config_path, filter_config_path, database_config_path):
        self.authen_config_path = authen_config_path
        self.filter_config_path = filter_config_path
        self.database_config_path = database_config_path

        self.api_key = ""
        self.api_secret_key = ""
        self.access_token = ""
        self.access_token_secret = ""

        self.track = []
        self.users = []
        self.ruling_politicians = []
        self.opposition_politicians = []
        self.user_location_filters = []
        self.geometry_filters = []
        self.geometry_data = []

        self.streaming = {}
        self.searching = {}
        self.tweetid = {}

        self.couchdb_connection_string = ""
        self.couchdb_database_name = ""

    def load_authentication_config(self):
        if os.path.exists(self.authen_config_path):
            with open(self.authen_config_path, encoding=UTF8_ENCODING) as fstream:
                try:
                    config_content = json.loads(fstream.read())
                    self.api_key = config_content[constants.JSON_API_KEY_PROP]
                    self.api_secret_key = config_content[constants.JSON_API_SECRET_KEY_PROP]
                    self.access_token = config_content[constants.JSON_ACCESS_TOKEN_PROP]
                    self.access_token_secret = config_content[constants.JSON_ACCESS_TOKEN_SECRET]
                except Exception as exception:
                    print("Error occurred during loading the authentication configuration file. Exception: %s" %exception)
        else:
            print(f"The authentication configuration file does not exist. Path: {self.authen_config_path}")

    def load_filter_config(self):
        if os.path.exists(self.filter_config_path):
            with open(self.filter_config_path, encoding=UTF8_ENCODING) as fstream:
                try:
                    config_content = json.loads(fstream.read())
                    self.streaming = config_content[constants.JSON_STREAMING_SECTION_PROP]
                    self.searching = config_content[constants.JSON_SEARCHING_SECTION_PROP]
                    self.tweetid = config_content[constants.JSON_TWEET_IDS_SECTION_PROP]
                    self.track = config_content[constants.JSON_TRACK_PROP]
                    self.user_location_filters = config_content[constants.JSON_USER_LOCATION_FILTERS_PROP]
                    self.ruling_politicians = config_content[constants.JSON_RULING_POLITICIANS_PROP]
                    self.opposition_politicians = config_content[constants.JSON_OPP_POLITICIANS_PROP]

                    self.geometry_filters = config_content[constants.JSON_GEOMETRY_FILTERS_PROP]
                    for filter_file in self.geometry_filters:
                        self.load_geometry_filter(filter_file)

                except Exception as exception:
                    print("Error occurred during loading the tweet filter configuration file. Exception: %s" %exception)
        else:
            print(f"The filter configuration file does not exist. Path: {self.filter_config_path}")

    def load_couchdb_config(self):
        if os.path.exists(self.database_config_path):
            with open(self.database_config_path, encoding=UTF8_ENCODING) as fstream:
                try:
                    config_content = json.loads(fstream.read())

                    username = config_content[constants.JSON_COUCHDB_SECTION_PROP][constants.JSON_USERNAME_PROP]
                    password = config_content[constants.JSON_COUCHDB_SECTION_PROP][constants.JSON_PASSWORD_PROP]
                    host = config_content[constants.JSON_COUCHDB_SECTION_PROP][constants.JSON_HOST_PROP]
                    port = config_content[constants.JSON_COUCHDB_SECTION_PROP][constants.JSON_PORT_PROP]

                    self.couchdb_database_name = config_content[constants.JSON_COUCHDB_SECTION_PROP][constants.JSON_DATABASE_NAME_PROP]
                    self.couchdb_connection_string = "http://{}:{}@{}:{}/".format(username, password, host, port)
                except Exception as exception:
                    print("Error occurred during loading the tweet database configuration file. Exception: %s" %exception)
        else:
            print(f"The database configuration file does not exist. Path: {self.database_config_path}")
    
    def load_geometry_filter(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, encoding=UTF8_ENCODING) as fstream:
                try:
                    config_content = json.loads(fstream.read())
                    features = config_content[constants.JSON_FEATURES_PROP]
                    if (features):
                        for feature in features:
                            coordinates = feature[constants.JSON_GEOMETRY_PROP][constants.COORDINATES]
                            for i, coordinate in enumerate(coordinates):
                                try:
                                    polygon = Polygon(coordinate[0])
                                    self.geometry_data.append(polygon)
                                except:
                                    print(f"Failed to load coordinates {i} into Polygon object.")
                except Exception as exception:
                    print("Error occurred during loading geometry filter file. Exception: %s" %exception)
    
    def get_streaming_locations(self):
        locations = []

        if (self.streaming is not None):
            locations = self.streaming[constants.JSON_LOCATIONS_PROP]

        return locations
    
    def get_tweetid_executors_config(self, rank):
        num_thread = 1
        thread_names = []

        if (self.tweetid is not None):
            exectors = self.tweetid[constants.JSON_EXECUTORS_PROP]
            if (exectors):
                num_thread = exectors[constants.JSON_NUM_THEAD_PROP]
                thread_names = exectors["thread_names_rank_{}".format(rank)]

        return num_thread, thread_names
    
    def get_tweetid_authentications(self, rank, num_thread):
        authen_info = {}

        if (self.tweetid is not None):
            authens = self.tweetid[constants.JSON_AUTHENS_PROP]
            if (authens):
                authens_for_rank = authens["rank_{}".format(rank)]
                for index in range(num_thread):
                    account = authens_for_rank["account{}".format(index)]
                    if (account is not None):
                        thread_owner = account[constants.JSON_THREAD_OWNER_PROP]
                        api_key = account[constants.JSON_API_KEY_PROP]
                        api_secret_key = account[constants.JSON_API_SECRET_KEY_PROP]
                        access_token = account[constants.JSON_ACCESS_TOKEN_PROP]
                        access_token_secret = account[constants.JSON_ACCESS_TOKEN_SECRET]

                        authen_obj = AuthenticationInfo(api_key, api_secret_key, access_token, access_token_secret)
                        authen_info[thread_owner] = authen_obj

        return authen_info

    def get_tweeid_dataset(self, processor_id, processor_size):
        groupped_dataset = []
        all_sub_folders = []

        if (self.tweetid is not None):
            folders = self.tweetid[constants.JSON_FOLDERS_PROP]
            if (folders):
                for configured_folder in folders:
                    print(f"Extracting tweetid datasets from {configured_folder}.")
                    if os.path.isdir(configured_folder):
                        data_folder_path = Path(configured_folder)
                        all_sub_folders.append(configured_folder)

                        for pth, dirs, files in os.walk(configured_folder):
                            for sub_folder in sorted(dirs, reverse = True):
                                print(f"Extracting tweetid datasets from sub folder {sub_folder}.")
                                sub_folder_path = data_folder_path / sub_folder
                                if os.path.isdir(sub_folder_path):
                                    all_sub_folders.append(sub_folder_path)
                    else:
                        print(f"{configured_folder} is not folder.")

                if(all_sub_folders):
                    for i, folder in enumerate(all_sub_folders):
                        if (i % processor_size == processor_id):
                            groupped_dataset.append(folder)

        return groupped_dataset
    
    def within_geometry_filters(self, user_coordinates):
        result = True

        if (self.geometry_data):
            result = False
            for polygon in self.geometry_data:
                if polygon.contains(Point(user_coordinates[0], user_coordinates[1])):
                    result = True
                    break

        return result

    def get_politician_type(self, screen_name):
        result = ""
        
        if (screen_name):
            if (screen_name in self.ruling_politicians):
                result = constants.RULING_POLITICIAN
            elif (screen_name in self.opposition_politicians):
                result = constants.OPPOSITION_POLITICIAN

        return result
    
    def get_searching_users(self, processor_id, processor_size):
        groupped_users = []

        if (self.searching is not None):
            user_list = self.searching[constants.JSON_USERS_PROP]
            user_list = self.ruling_politicians + self.opposition_politicians + user_list
            for i, user in enumerate(user_list):
                if (i % processor_size == processor_id):
                    groupped_users.append(user)

        return groupped_users

    def get_searching_authen(self, processor_id):
        # Initialise with common authentication keys
        api_key = self.api_key
        api_secret_key = self.api_secret_key
        access_token = self.access_token
        access_token_secret = self.access_token_secret

        # Extract the authentication keys from specified processor
        if (self.searching is not None):
            authens = self.searching[constants.JSON_AUTHENS_PROP]
            if (authens is not None):
                account = authens["account{}".format(processor_id)]
                if (account is not None):
                    api_key = account[constants.JSON_API_KEY_PROP]
                    api_secret_key = account[constants.JSON_API_SECRET_KEY_PROP]
                    access_token = account[constants.JSON_ACCESS_TOKEN_PROP]
                    access_token_secret = account[constants.JSON_ACCESS_TOKEN_SECRET]

        return api_key, api_secret_key, access_token, access_token_secret
