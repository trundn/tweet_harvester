# Definition of Constants
CMD_LINE_DEFINED_ARGUMENTS = "ha:f:d:m:"
HELP_ARGUMENT = "-h"
AUTHEN_CONFIG_ARGUMENT = "-a"
FILTER_CONFIG_ARGUMENT = "-f"
DATABASE_CONFIG_ARGUMENT = "-d"
HARVEST_MODE_ARGUMENT = "-m"

CMD_LINE_MIGRATION_ARGUMENTS = "hj:f:d:"
DATA_CONFIG_ARGUMENT = "-j"

UTF8_ENCODING = "utf8"
JSON_NEW_LINE_STRING = ",\n"


ALL_HARVEST_MODE = "all"
STREAM_HARVEST_MODE = "stream"
SEARCH_HARVEST_MODE = "search"
TWEETID_HARVEST_MODE = "tweetid"

JSON_THREAD_OWNER_PROP = "thread_owner"
JSON_API_KEY_PROP = "api_key"
JSON_API_SECRET_KEY_PROP = "api_secret_key"
JSON_ACCESS_TOKEN_PROP = "access_token"
JSON_ACCESS_TOKEN_SECRET = "access_token_secret"

JSON_STREAMING_SECTION_PROP = "streaming"
JSON_SEARCHING_SECTION_PROP = "searching"
JSON_TWEET_IDS_SECTION_PROP = "tweetids"
JSON_EXECUTORS_PROP = "executors"
JSON_NUM_THEAD_PROP = "num_thread_per_process"
JSON_FOLDERS_PROP = "folders"
JSON_PROCESSOR_PROP = "processor"
JSON_AUTHENS_PROP = "authens"
JSON_LOCATIONS_PROP = "locations"
JSON_LANGUAGES_PROP = "languages"
JSON_TRACK_PROP = "track"
JSON_USERS_PROP = "users"
JSON_USER_LOCATION_FILTERS_PROP = "user_location_filers"
JSON_RULING_POLITICIANS_PROP = "ruling_politicians"
JSON_OPP_POLITICIANS_PROP = "opposition_politicians"
JSON_GEOMETRY_FILTERS_PROP = "geometry_filters"

JSON_COUCHDB_SECTION_PROP = "couchdb"
JSON_DATABASE_NAME_PROP = "database_name"
JSON_USERNAME_PROP = "username"
JSON_PASSWORD_PROP = "password"
JSON_HOST_PROP = "host"
JSON_PORT_PROP = "port"

JSON_FEATURES_PROP = "features"
JSON_GEOMETRY_PROP = "geometry"

JSON_DOCUMENT = "doc"
JSON_TRUNCATED_PROP = "truncated"
JSON_EXTENDED_TWEET_PROP = "extended_tweet"
JSON_RETWEETED_STATUS_PROP = "retweeted_status"
JSON_FULL_TEXT_PROP = "full_text"
JSON_TEXT_PROP = "text"

TWEET_ID_COLUMN = "tweet_id"
TAB_CHAR = "\t"
TWEETS_DATABASE = "twitter_database"
LIMIT_COUNT_PER_REQ = 200
TWEET_MODE = "extended"

ONE_SECOND = 1
TWO_SECONDS = 2
ONE_MINUTE = 60

GEO = "geo"
PLACE = "place"
COORDINATES = "coordinates"
BOUNDING_BOX = "bounding_box"
AUSTRALIA_COUNTRY_NAME = "australia"

RULING_POLITICIAN = "ruling"
OPPOSITION_POLITICIAN = "opposition"

FIRST_PERSON_SINGULAR = ["i", "me", "my", "mine", "myself"]
FIRST_PERSON_PLURAL = ["we", "us", "our", "ours", "ourselves"]
SECOND_PERSON_PRONOUN = ["you", "your", "yours", "yourselves"]
THIRD_PERSON_PRONOUN = ["he", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself"]