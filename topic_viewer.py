""" Launches topics and graph.
TODO: the first period is shorter than it should be.
TODO: works only with two topics, not more.
    - Because of twitter limits.
"""


import docker
from redis_container import RedisDB
from db import DB


class TopicViewer:
    def __init__(self, topics):
        self.topics = topics
        self.docker_client = docker.from_env()
        self.db = RedisDB('redis')
        self.topic_containers = self.start_topics()
        self.aggregator = self.start_aggregator()
        self.persist = DB()
        self.persist.create_db()

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
        for item in pubsub.listen():
            if item['type'] == 'subscribe':
                continue
            group, start = item['data'].decode('utf-8').split('-')
            self.db.activity.zadd('active', {group: 0})
            self.db.activity.zadd('timestamps', {group: start})
            self.update(int(group))

    def update(self, new_group):
        """ Flush the data for all older groups and set their counts to zero.
        If all topics of a group are zero, increment its value. Once it reaches
        three, remove it. """
        for key in self.db.activity.zrange('active', 0, -1):
            group_nr = int(key.decode('utf-8'))
            if group_nr == new_group:  # skip new group
                continue
            scores = self.db.agg.zrange(group_nr, 0, -1, withscores=True)
            if scores[-1][1] == 0:  # no new values since last check
                if self.db.activity.zscore('active', group_nr) < 3:
                    self.db.activity.zincrby('active', 1, group_nr)
                else:  # delete sorted set; empty for three checks.
                    self.db.activity.zrem('active', group_nr)
                    self.db.agg.zremrangebyrank(group_nr, 0, len(self.topics))
            else:  # new values since last check
                self.save(group_nr)
                self.db.agg.zadd(
                    group_nr, {topic: 0 for topic in self.topics}, xx=True
                )

    def save(self, group_nr):
        """ Stores updates in the DB. """
        cursor = self.persist.get_cursor()
        cursor.execute(f'SELECT COUNT(*) FROM groups WHERE id = {group_nr}')
        exists = cursor.fetchone()[0]
        if exists:
            for topic in self.topics:
                update = self.db.agg.zscore(group_nr, topic)
                if update is None:
                    update = 0
                cursor.execute(f"""
                    UPDATE topics SET value = value + {update}
                    WHERE group_id = {group_nr} AND topic = '{topic}'
                """)
        else:
            start = self.db.activity.zscore('timestamps', group_nr)
            self.db.activity.zrem('timestamps', group_nr)  # only needed once
            cursor.execute(f"""
                INSERT INTO groups (id, starting_timestamp)
                VALUES ({group_nr}, {start})
            """)
            for topic in self.topics:
                update = self.db.agg.zscore(group_nr, topic)
                if update is None:
                    update = 0
                cursor.execute(f"""
                    INSERT INTO topics (group_id, topic, value)
                    VALUES ({group_nr}, '{topic}', {update})
                """)
        self.persist.commit()

    def stop(self):
        """ Stop and remove all containers. """
        self.aggregator.stop()
        for container in self.topic_containers:
            container.stop()
        self.db.stop()


if __name__ == '__main__':
    viewer = TopicViewer([
        'covid', 'Trump'
    ])
    viewer.listen()
    viewer.stop()
