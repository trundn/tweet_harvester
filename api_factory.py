# An easy-to-use Python library for accessing the Twitter API
import tweepy

class APIFactory(object):
    def create_api(self, api_key, api_secret_key, access_token, access_token_secret):
        if not (api_key and api_secret_key and access_token and access_token_secret):
            raise RuntimeError("The authentication keys are missing.")

        # Creating the authentication object
        print("Creating OAuth user authentication.")
        auth = tweepy.OAuthHandler(api_key, api_secret_key)

        if (auth):
            # Setting your access token and secret
            auth.set_access_token(access_token, access_token_secret)
            
            # Creating the API object while passing in auth information
            print("Creating Tweepy API from OAuth user authentication.")
            api = tweepy.API(auth, wait_on_rate_limit = True)

            if (not api):
                raise RuntimeError("Cannot create Tweepy API.")
        else:
            raise RuntimeError("Cannot create OAuth user authentication.")

        return api