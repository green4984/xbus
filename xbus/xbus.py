# -*- coding: utf-8 -*-
import abc
import redis
import time
import zmq
import threading
import logging

try:
    import simplejson as json
except:
    import json


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

    @abc.abstractmethod
    def push(self, body):
        pass

    @abc.abstractmethod
    def pull(self):
        pass


class MessageProxy(object):
    def __init__(self, data):
        self.data = data

    @abc.abstractmethod
    def serialize(self):
        pass

    @abc.abstractmethod
    def deserialization(self):
        pass


class JsonMessage(MessageProxy):
    def serialize(self):
        return json.dump(self.data)

    def deserialization(self):
        return json.loads(self.data)


class SmsMessageProxy(MessageProxy):
    pass


class RedisMQProxy(MQProxy):
    def __init__(self, redis_instance, *channel):
        assert redis_instance is not None
        assert isinstance(redis_instance, (redis.Redis, redis.StrictRedis))
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