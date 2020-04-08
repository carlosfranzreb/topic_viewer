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

    def store_groups(self):
        """ Every time a new group starts (published in the 'group' channel)
        retrieve and store the previous group.  The counts are then reset
        to zero. Previous groups are checked again for new values, and if they
        are still empty after three checks, they are deleted. """
        groups = list()  # group keys are stored here

    def check_aggregations(self, group):
        print(f'Group: {group}')
        print(f'Number of members: {self.db.agg_db.zcount(group, -inf, inf)}')
        print(f'Top 3: {self.db.agg_db.zrange(group, 0, 3, withscores=True)}')

    def stop(self):
        """ Stop and remove all containers. """
        self.aggregator.stop()
        for container in self.topic_containers:
            container.stop()
        self.db.stop()


if __name__ == '__main__':
    viewer = TopicViewer(['Trump', 'covid'])
    sleep(5)
    viewer.check_aggregations('caca')
    sleep(5)
    viewer.check_aggregations('caca')
    sleep(5)
    viewer.check_aggregations('caca')
    viewer.stop()
