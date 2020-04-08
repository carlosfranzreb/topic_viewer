""" Receives topic occurrences from publishers and aggregates them
for a given granularity (in minutes). The aggregations are stored
in another Redis database, and flushed to a CSV once the current
time period is over. """


import sys
from math import inf
from time import perf_counter
import redis


class Aggregator:
    def __init__(self, granularity=1):
        """ Topics are given as arguments as so:
        python aggregator.py topic1 topic2 ... """
        self.topics = sys.argv[1:]
        self.granularity = granularity * 60  # granularity in seconds
        self.topic_db = redis.Redis(host='localhost', port=6379, db=0)
        self.agg_db = redis.Redis(host='localhost', port=6379, db=1)
        self.pubsub = self.topic_db.pubsub()
        for topic in self.topics:
            self.pubsub.subscribe(topic)

    def listen(self):
        start = int(perf_counter())  # timestamp in seconds
        max_group = -inf  # Most recent group
        for item in self.pubsub.listen():
            if item['type'] == 'subscribe':
                continue
            topic = item['channel'].decode('utf-8')
            timestamp = int(float((item['data'].decode('utf-8'))))
            group = int((timestamp - start) / self.granularity)
            self.agg_db.zincrby(group, 1, topic)
            if group > max_group:
                max_group = group
                self.agg_db.publish('group', group)


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.listen()
