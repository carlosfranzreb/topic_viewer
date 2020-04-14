import docker
import redis


class RedisDB():
    def __init__(self, image):
        client = docker.from_env()
        self.container = client.containers.run(
            image=image,
            name='redis_container',
            ports={6379: 6379},
            detach=True,
            remove=True  # Remove container when stopped
        )
        self.topic = redis.Redis(host='localhost', port=6379, db=0)
        self.agg = redis.Redis(host='localhost', port=6379, db=1)
        self.activity = redis.Redis(host='localhost', port=6379, db=2)

    def stop(self):
        self.container.stop()
