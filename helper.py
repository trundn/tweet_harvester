# Provides various time-related functions
import time
# Implements classes to read and write tabular data in CSV format
import csv
# An easy-to-use Python library for accessing the Twitter API
import tweepy
# The harvest constant definitions
import constants
# Sentimental anaylsis for extracting positve, negative, neutral, compound emotion
import nltk
nltk.download("vader_lexicon")
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nltk.tokenize import RegexpTokenizer

class Helper(object):
    def is_match(self, text, filters):
        result = False

        if (filters):
            if (text):
                if any(key_word in text for key_word in filters):
                    result = True
        else:
            # No need to apply any text filters
            result = True

        return result

    def extract_coordinates(self, tweet):
        source = ""
        coordinates = []

        if (tweet is not None):
            if ((tweet.geo is not None) \
                and hasattr(tweet.geo, constants.COORDINATES) \
                and (tweet.geo.coordinates is not None)):

                source = constants.GEO
                coordinates = [tweet.geo.coordinates[1], tweet.geo.coordinates[0]]
            elif ((tweet.coordinates is not None) \
                and hasattr(tweet.coordinates, constants.COORDINATES) \
                and (tweet.coordinates.coordinates is not None)):

                source = constants.COORDINATES
                coordinates = tweet.coordinates.coordinates
            elif ((tweet.place is not None) \
                and hasattr(tweet.place, constants.BOUNDING_BOX) \
                and (tweet.place.bounding_box is not None) \
                and hasattr(tweet.place.bounding_box, constants.COORDINATES) \
                and (tweet.place.bounding_box.coordinates is not None)):

                source = constants.PLACE
                tmp_coordinates = tweet.place.bounding_box.coordinates[0]
                longitude =(tmp_coordinates[0][0] + tmp_coordinates[2][0]) / 2
                latitude = (tmp_coordinates[0][1] + tmp_coordinates[2][1]) / 2

                coordinates = [longitude, latitude]

        return source, coordinates


    def get_followers(self, tweepy_api, user_name, max_count):
        # Initialize a list to hold all followers
        followers = []

        for page in tweepy.Cursor(tweepy_api.followers,
                            screen_name = user_name,
                            wait_on_rate_limit = True,
                            count = constants.LIMIT_COUNT_PER_REQ).pages():
            
            try:
                # Put all new follower into final follower list
                followers.extend(page)

                # Check if total followers reached max count or not
                if ((max_count != -1) and (len(followers) >= max_count)):
                    break
            except tweepy.TweepError as ex:
                print("Going to sleep:", ex)
                # Sleep 60 seconds to avoid rate limit issue
                time.sleep(constants.ONE_MINUTE)

            # Sleep 60 seconds to avoid rate limit issue
            time.sleep(constants.ONE_MINUTE)

        return followers

    def get_all_tweets(self, tweepy_api, screen_name):
        # Initialize a list to hold all the tweepy Tweets
        alltweets = []

        try:
            # Make initial request for most recent tweets
            new_tweets = tweepy_api.user_timeline(screen_name = screen_name,
                count = constants.LIMIT_COUNT_PER_REQ, tweet_mode = constants.TWEET_MODE)
            
            # Put all new tweets into final tweets list
            alltweets.extend(new_tweets)
            
            if (len(new_tweets) > 0):
                # Save the id of the oldest tweet less one
                oldest = alltweets[-1].id - 1
                
                # Keep grabbing tweets until there are no tweets left to grab
                while len(new_tweets) > 0:
                    time.sleep(constants.TWO_SECONDS)
                    
                    # All subsiquent requests use the max_id param to prevent duplicates
                    new_tweets = tweepy_api.user_timeline(screen_name = screen_name,
                        count = constants.LIMIT_COUNT_PER_REQ, max_id = oldest, tweet_mode = constants.TWEET_MODE)
                    
                    # Put all new tweets into final tweets list
                    alltweets.extend(new_tweets)
                    
                    # Update the id of the oldest tweet less one
                    oldest = alltweets[-1].id - 1
        except tweepy.TweepError as ex:
            print(ex)

        return alltweets
    
    def filer_tweets_with_coordinates(self, all_tweets, recheck_with_user_location = False):
        processed_tweets = []

        if (all_tweets):
            # Check if tweet is in configured user filter locations
            for tweet in all_tweets:
                if (hasattr(tweet, constants.PLACE) \
                        and tweet.place is not None \
                        and tweet.place.country is not None):

                    location = tweet.place.country.lower()
                    if (constants.AUSTRALIA_COUNTRY_NAME == location):
                        processed_tweets.append(tweet)
                else:
                    source, coordinates = self.extract_coordinates(tweet)
                    if (coordinates):
                        if (recheck_with_user_location is True):
                            location = tweet.user.location.lower()
                            if (self.helper.is_match(location, self.config_loader.user_location_filters)):
                                processed_tweets.append(tweet)
                        else:
                            processed_tweets.append(tweet)

        return processed_tweets

    def extract_full_text(self, tweet):
        full_text = ""

        try:
            full_text = tweet.retweeted_status.extended_tweet[constants.JSON_FULL_TEXT_PROP]
        except:
            try:
                full_text = tweet.full_text
            except:
                full_text = tweet.text
        
        return full_text

    def extract_emotions(self, tweet_text):
        # A SentimentIntensityAnalyzer
        sia = SIA()
        # Get emotion scores e.g.: {'neg': 0.047, 'neu': 0.849, 'pos': 0.104, 'compound': 0.3565}
        emotions = sia.polarity_scores(tweet_text)
        return emotions
    
    def extract_word_count(self, tweet_text):
        tokenizer = RegexpTokenizer(r'\w+')
        words_list = tokenizer.tokenize(tweet_text.lower())
        
        # Count Tweet words length
        word_count = len(words_list)
        
        # Initial pronoun count
        pronoun_count = {"fps_cnt":0, "fpp_cnt":0, "sp_cnt":0, "tp_cnt":0}

        # Count each pronoun
        for w in words_list:
            for pron in constants.FIRST_PERSON_SINGULAR:
                if w == pron:
                    pronoun_count["fps_cnt"] += 1
            for pron in constants.FIRST_PERSON_PLURAL:
                if w == pron:
                    pronoun_count["fpp_cnt"] += 1
            for pron in constants.SECOND_PERSON_PRONOUN:
                if w == pron:
                    pronoun_count["sp_cnt"] += 1
            for pron in constants.THIRD_PERSON_PRONOUN:
                if w == pron:
                    pronoun_count["tp_cnt"] += 1
        
        return word_count, pronoun_count

