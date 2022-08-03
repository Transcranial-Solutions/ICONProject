
#########################################################################
## Project: Shill Your Favourite P-Reps                                ##
## Date: October 2020                                                  ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################


import os
import tweepy as tw
import pandas as pd
import ssl
import csv



# consumer_key = 'XXXXXXXX'
# consumer_secret = 'XXXXXXXX'
# access_token = 'XXXXXXXX'
# access_token_secret = 'XXXXXXXX'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)


name = 'tonoplast'
tweet_id = '1306385545325494272'


replies = []
for tweet in tw.Cursor(api.search, q='to:'+name, result_type='recent', timeout=999999).items(1000):
    if hasattr(tweet, 'in_reply_to_status_id_str'):
        if (tweet.in_reply_to_status_id_str==tweet_id):
            replies.append(tweet)

# with open('replies_clean.csv', 'w') as f:
#     csv_writer = csv.DictWriter(f, fieldnames=('user', 'text'))
#     csv_writer.writeheader()
#     for tweet in replies:
#         row = {'user': tweet.user.screen_name, 'text': tweet.text.replace('\n', ' ')}
#         csv_writer.writerow(row)


def update_urls(tweet, api, urls):
    tweet_id = tweet.id
    user_name = tweet.user.screen_name
    max_id = None
    replies = tw.Cursor(api.search, q='to:{}'.format(user_name),
                                since_id=tweet_id, max_id=max_id, tweet_mode='extended').items()

    for reply in replies:
        if(reply.in_reply_to_status_id == tweet_id):
            urls.append(get_twitter_url(user_name, reply.id))
            try:
                for reply_to_reply in update_urls(reply, api, urls):
                    pass
            except Exception:
                pass
        max_id = reply.id
    return urls


def get_api():
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    return api

def get_tweet(url):
    tweet_id = url.split('/')[-1]
    api = get_api()
    tweet = api.get_status(tweet_id)
    return tweet

def get_twitter_url(user_name, status_id):
    return "https://twitter.com/" + str(user_name) + "/status/" + str(status_id)


api = get_api()
tweet = get_tweet('https://twitter.com/tonoplast/status/1300978803175432193')
urls = [url]
urls = update_urls(tweet, api, urls)



import sys
import json
import time
import logging
import twitter
import urllib.parse

from os import environ as e

t = twitter.Api(
    consumer_key=e["CONSUMER_KEY"],
    consumer_secret=e["CONSUMER_SECRET"],
    access_token_key=e["ACCESS_TOKEN"],
    access_token_secret=e["ACCESS_TOKEN_SECRET"],
    sleep_on_rate_limit=True
)

def tweet_url(t):
    return "https://twitter.com/%s/status/%s" % (t.user.screen_name, t.id)

def get_tweets(filename):
    for line in open(filename):
        yield twitter.Status.NewFromJsonDict(json.loads(line))

def get_replies(tweet):
    user = tweet.user.screen_name
    tweet_id = tweet.id
    max_id = None
    logging.info("looking for replies to: %s" % tweet_url(tweet))
    while True:
        q = urllib.parse.urlencode({"q": "to:%s" % user})
        try:
            replies = t.GetSearch(raw_query=q, since_id=tweet_id, max_id=max_id, count=100)
        except twitter.error.TwitterError as e:
            logging.error("caught twitter api error: %s", e)
            time.sleep(60)
            continue
        for reply in replies:
            logging.info("examining: %s" % tweet_url(reply))
            if reply.in_reply_to_status_id == tweet_id:
                logging.info("found reply: %s" % tweet_url(reply))
                yield reply
                # recursive magic to also get the replies to this reply
                for reply_to_reply in get_replies(reply):
                    yield reply_to_reply
            max_id = reply.id
        if len(replies) != 100:
            break

if __name__ == "__main__":
    logging.basicConfig(filename="replies.log", level=logging.INFO)
    tweets_file = sys.argv[1]
    for tweet in get_tweets(tweets_file):
        for reply in get_replies(tweet):
            print(reply.AsJsonString())



























search_words = "$ICX"
date_since = "2020-09-26"

# Collect tweets
tweets = tw.Cursor(api.search,
              q=search_words,
              lang="en",
              since=date_since).items(5)
tweets


# Collect tweets
tweets = tw.Cursor(api.search,
              q=search_words,
              lang="en",
              since=date_since).items(5)

# Iterate and print tweets
for tweet in tweets:
    print(tweet.text)

    # Collect tweets
    tweets = tw.Cursor(api.search,
                       q=search_words,
                       lang="en",
                       since=date_since).items(5)

    # Collect a list of tweets
    [tweet.text for tweet in tweets]








import requests
import bs4
from bs4 import BeautifulSoup
from requests_oauthlib import OAuth1

auth_params = {
    'app_key':'xxx',
    'app_secret':'xxx',
    'oauth_token':'xxx',
    'oauth_token_secret':'xxx'
}

# Creating an OAuth Client connection
auth = OAuth1 (
    auth_params['app_key'],
    auth_params['app_secret'],
    auth_params['oauth_token'],
    auth_params['oauth_token_secret']
)


# url according to twitter API
url_rest = "https://api.twitter.com/1.1/search/tweets.json"

# getting rid of retweets in the extraction results and filtering all replies to the tweet often uncessary for the analysis
q = '%tonoplast -filter:retweets -filter:replies' # Twitter handle of Amazon India

# count : no of tweets to be retrieved per one call and parameters according to twitter API
params = {'q': q, 'count': 100, 'lang': 'en',  'result_type': 'recent'}
results = requests.get(url_rest, params=params, auth=auth)


tweets = results.json()

messages = [BeautifulSoup(tweet['text'], 'html.parser').get_text() for tweet in tweets['statuses']]
print(messages)






