""" Counts appearances of a given topic in the Twitter stream. 
Run container with: docker run --network="host" --name container topic "TOPIC",
where topic is the name of the image and "TOPIC" the topic argument. The
network flag is for the container to connect to the redis container.
"""


import json
import sys
from time import time

import twitter
import redis


class Topic:
    def __init__(self, topic):
        self.api = self.get_twitter_api()
        self.db = redis.Redis(host='localhost', port=6379, db=0)
        self.topic = topic
        self.stream = self.api.GetStreamFilter(track=[topic])

    def get_twitter_api(self):
        with open('/twitter_credentials.json') as f:
            credentials = json.load(f)
        return twitter.Api(
            consumer_key=credentials['consumer_key'],
            consumer_secret=credentials['consumer_secret'],
            access_token_key=credentials['access_token_key'],
            access_token_secret=credentials['access_token_secret']
        )

    def count_tweets(self):
        while next(self.stream):
            self.db.publish(self.topic, time())


if __name__ == '__main__':
    topic = Topic(sys.argv[1])
    topic.count_tweets()
