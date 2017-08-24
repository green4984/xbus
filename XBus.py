# -*- coding: utf-8 -*-
import abc
import redis
import time
import zmq
import threading
import logging


class QueueFactory(object):
    def __init__(self, proxy):
        """
        being as base class of queue
        :param proxy: the proxy instance
        """
        self.proxy = proxy
        assert self.proxy is not None

    def publish(self, body):
        self.proxy.publish(body)

    def subscribe(self, callback):
        self.proxy.subscribe(callback=callback)

    def unsubscribe(self):
        self.proxy.unsubscribe()

    def consume(self):
        return self.proxy.consume()


class MQProxy(object):
    @abc.abstractmethod
    def publish(self, body):
        pass

    @abc.abstractmethod
    def subscribe(self, callback):
        pass

    @abc.abstractmethod
    def unsubscribe(self):
        pass

    @abc.abstractmethod
    def consume(self):
        pass


class RedisMQProxy(MQProxy):
    def __init__(self, redis_instance, *channel):
        assert redis_instance is not None
        self.redis = redis_instance
        self.channel = channel if isinstance(channel, (tuple, list)) else [channel]
        self.ps = self.redis.pubsub(ignore_subscribe_messages=True)
        self.thread = None

    def publish(self, body):
        for chan in self.channel:
            self.redis.publish(chan, body)

    def subscribe(self, callback=None):
        dic = dict()
        for chan in self.channel:
            dic.setdefault(chan, callback)
        self.ps.subscribe(**dic)
        self.thread = self.ps.run_in_thread(sleep_time=0.3)

    def unsubscribe(self):
        self.thread.stop()

    def consume(self):
        return self.ps.get_message()


class ZMQProxy(MQProxy):
    def __init__(self, url):
        assert url is not None
        self.url = url
        self.context_sub = zmq.Context()
        self.context_pub = zmq.Context()
        self.socket_sub = None
        self.socket_pub = None
        self.callback = None

    def subscribe(self, callback=None):
        self.socket_sub = self.context_sub.socket(zmq.SUB)
        self.socket_sub.connect(self.url)
        self.socket_sub.setsockopt(zmq.SUBSCRIBE, '')
        self.callback = callback

    def unsubscribe(self):
        pass

    def _pub(self, body):
        self.socket_pub = self.context_pub.socket(zmq.PUB)
        self.socket_pub.bind(self.url)
        ind = 1
        time.sleep(0.2)
        while True:
            ret = self.socket_pub.send('{0} {1}'.format(body, ind), copy=False, track=True)
            ret.wait()
            ind += 1

    def publish(self, body):
        t = threading.Thread(target=ZMQProxy._pub, args=(self, body))
        t.setDaemon(True)
        t.start()

    def consume(self):
        msg = self.socket_sub.recv()
        if self.callback:
            self.callback(msg)
        return msg


def get_message(message):
    logging.warning(msg=message)


def test_redis_proxy():
    url = 'redis://:@127.0.0.1:6380'
    rd1 = redis.Redis.from_url(url=url, db=11)
    rd2 = redis.Redis.from_url(url=url, db=11)
    q1 = QueueFactory(RedisMQProxy(rd1, 'tt1'))

    q2 = QueueFactory(RedisMQProxy(rd2, 'tt1'))
    q2.subscribe(get_message)
    time.sleep(5)
    q1.publish('hello')
    time.sleep(9)
    q2.consume()
    q2.unsubscribe()


def test_zmq_proxy():
    z_sub = QueueFactory(ZMQProxy('tcp://127.0.0.1:5003'))
    z_pub = QueueFactory(ZMQProxy('tcp://127.0.0.1:5003'))

    z_sub.subscribe(get_message)
    time.sleep(2)
    z_pub.publish(b'hello zmq')
    time.sleep(2)
    while True:
        z_sub.consume()
    z_sub.unsubscribe()



if __name__ == '__main__':
    # test_redis_proxy()
    test_zmq_proxy()
    pass
