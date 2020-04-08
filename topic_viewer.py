""" Launches topics and graph. """

import docker
from redis_container import RedisDB
from time import sleep
from math import inf


class TopicViewer:
    def __init__(self, topics):
        self.topics = topics
        self.docker_client = docker.from_env()
        self.db = RedisDB('redis')
        self.topic_containers = self.start_topics()
        self.aggregator = self.start_aggregator()

    def start_topics(self):
        """ Starts one container for each topic and returns their references
        as a list. Topics must be strings and are therefore casted here. It
        is is required for the join in self.start_aggregator to work. """
        containers = list()
        for i in range(len(self.topics)):
            self.topics[i] = str(self.topics[i])
            containers.append(self.docker_client.containers.run(
                image='topic',
                name=f'{self.topics[i]}_container',
                entrypoint=f'python /topic.py {self.topics[i]}',
                network='host',
                detach=True,
                remove=True  # remove container when stopped
            ))
        return containers

    def start_aggregator(self):
        """ Starts the aggregator container and returns its reference. """
        return self.docker_client.containers.run(
            image='aggregator',
            name='aggregator_container',
            entrypoint=f'python /aggregator.py {" ".join(self.topics)}',
            network='host',
            detach=True,
            remove=True  # remove container when stopped
        )

    def listen(self):
        """ Every time a new group starts (published in the 'group' channel)
        retrieve and store the previous group.  The counts are then reset
        to zero. Previous groups are checked again for new values, and if they
        are still empty after three checks, they are deleted. """
        pubsub = self.db.agg.pubsub()
        pubsub.subscribe('group')
        i = 0
        for item in pubsub.listen():
            if item['type'] == 'subscribe':
                continue
            new_group = int(item['data'].decode('utf-8'))
            self.db.activity.zadd('active', {new_group: 0})
            self.update(new_group)
            i += 1
            if i > 10:
                break
        self.stop()

    def update(self, new_group):
        """ Flush the data for all older groups and set their counts to zero.
        If all topics of a group are zero, increment its value. Once it reaches
        three, remove it. """
        for key in self.db.activity.zrange('active', 0, -1):
            group_nr = int(key.decode('utf-8'))
            if group_nr == new_group:
                continue
            scores = self.db.agg.zrange(group_nr, 0, -1, withscores=True)
            if scores[-1][1] == 0:
                if self.db.activity.zscore('active', group_nr) < 3:
                    self.db.activity.zincrby('active', 1, group_nr)
                else:
                    self.db.activity.zrem('active', group_nr)
                    self.db.agg.zremrangebyrank(group_nr, 0, len(self.topics))
            else:
                self.save(group_nr)
                self.db.agg.zadd(
                    group_nr, {topic: 0 for topic in self.topics}, xx=True
                )

    def check_aggregations(self, group):
        print(f'Group: {group}')
        print(f'Number of members: {self.db.agg.zcount(group, -inf, inf)}')
        print(f'Top 3: {self.db.agg.zrange(group, 0, 3, withscores=True)}')

    def stop(self):
        """ Stop and remove all containers. """
        self.aggregator.stop()
        for container in self.topic_containers:
            container.stop()
        self.db.stop()


if __name__ == '__main__':
    viewer = TopicViewer(['Spain', 'Germany'])
    viewer.listen()
