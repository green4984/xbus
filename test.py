# -*- coding: utf-8 -*-
import redis
import xbus
import time
import logging


def get_message(message):
    logging.warning(msg=message)


def test_redis_proxy():
    url = 'redis://:@127.0.0.1:6380'
    rd1 = redis.Redis.from_url(url=url, db=11)
    rd2 = redis.Redis.from_url(url=url, db=11)
    q1 = xbus.QueueFactory(xbus.RedisMQProxy(rd1, 'tt1'))

    q2 = xbus.QueueFactory(xbus.RedisMQProxy(rd2, 'tt1'))
    q2.subscribe(get_message)
    time.sleep(5)
    q1.publish('hello')
    time.sleep(9)
    q2.consume()
    q2.unsubscribe()


def test_zmq_proxy():
    z_sub = xbus.QueueFactory(xbus.ZMQProxy('tcp://127.0.0.1:5003'))
    z_pub = xbus.QueueFactory(xbus.ZMQProxy('tcp://127.0.0.1:5003'))

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
