#!/usr/bin/env python

# Copyright 2019 Dynamic Object Language Labs Inc.
#
# This software is licensed under the terms of the
# Apache License, Version 2.0 which can be found in
# the file LICENSE at the root of this distribution.

import pika
import json
import time
import threading
import collections

'''
Helper functions for plant interface

Does not work with pika 1.0.0 and variants because of breaking changes
pip2 install 'pika==0.13.1' --user

'''


def get_time_millis():
    return time.time() * 1000


id_index = 0


def make_id(id_prefix="id-"):
    global id_index
    id_index += 1
    return id_prefix + str(id_index)


# Convert RMQ message body of type bytes to object.
# Assume contents of bytes is json string
def to_object(body):
    return json.loads(body.decode("utf-8"))


class Plant:
    def __init__(self, plantid, exchange, host='localhost', port=5672):
        self.plantid = plantid
        self.exchange = exchange
        self.routing_key = 'observations'
        self.host = host
        self.port = port
        self.cb_function = None
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.connection.channel()
        # Sending messages to channel must be called from the same thread as the one creating this one.
        self.to_rmq = collections.deque()
        self.connection.add_callback_threadsafe(self.rmq_is_idle)
        self.channel_thread = threading.current_thread()
        self.channel.exchange_declare(exchange=exchange, exchange_type='topic')
        self.queue = self.channel.queue_declare(exclusive=True)
        self.qname = self.queue.method.queue
        self.done = False
        if plantid is not None:
            self.channel.queue_bind(queue=self.qname, exchange=exchange, routing_key=plantid)

    def myprint(self):
        print("plantid: " + str(self.plantid) + ", exchange: " + str(self.exchange) +
              ", host: " + self.host + ", port: " + str(self.port))

    def subscribe(self, keys):
        for key in keys:
            self.channel.queue_bind(queue=self.qname, exchange=self.exchange, routing_key=key)

    def wait_for_messages(self, cb_fn_name):
        self.myprint()
        print("Waiting for commands")
        # Tell the broker that we won't be providing explicit acks for messages received.
        self.cb_function = cb_fn_name
        self.channel.basic_consume(self.message_receiver_internal, queue=self.qname,
                                   no_ack=True)
        self.wait_until_keyboard_interrupt()

    def wait_until_keyboard_interrupt(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print(" Keyboard interrupt, perhaps Control-C.")
        # except Exception as e:
        #     print 'plant.py wait_until_keyboard_interrupt \nThis happens when closing RMQ connections\n',e.__class__.__name__ + ": " + e.message
        # print 'plant.py done '

    def message_receiver_internal(self, channel, method, properties, body):
        msg = to_object(body)
        # print("Dispatching received plant message method: " + str(method))
        # print("Dispatching received plant message properties: " + str(properties))
        # print("Dispatching received plant message body: " + str(msg))
        self.cb_function(msg, method.routing_key)

    def close(self):
        # print("closing rmq connection")

        while self.to_rmq:
            print 'plant closing, process pending messages', len(self.to_rmq)
            self.process_to_rmq()

        self.channel.close()
        self.connection.close()

    def get_plantId(self, msg):

        if msg is not None and 'plant-id' in msg:
            plid = msg['plant-id']
        else:
            plid = self.plantid

        return plid

    # Plant helper functions

    # Generate a 'start' message
    def start(self, fn_name, plant_id, args=[], argsmap={}, timestamp=get_time_millis()):
        msg = {'id': make_id(),
               'plant-id': plant_id,
               'state': 'start',
               'function-name': fn_name,
               'args': args,
               'argsmap': argsmap,
               'timestamp': timestamp}
        self.__enque_to_rmq(self.exchange, plant_id, json.dumps(msg))

    def started(self, orig_msg):
        msg = {'id': orig_msg['id'],
               'plant-id': self.get_plantId(orig_msg),
               'state': 'started',
               'timestamp': get_time_millis()}
        self.__enque_to_rmq(self.exchange, self.routing_key, json.dumps(msg))

    def failed(self, orig_msg, failure_message):
        msg = {'id': orig_msg['id'],
               'plant-id': self.get_plantId(orig_msg),
               'state': 'finished',
               'timestamp': get_time_millis(),
               'reason': {'finish-state': 'failed',
                          'failed-reason': failure_message}}
        self.__enque_to_rmq(self.exchange, self.routing_key, json.dumps(msg))

    def finished(self, orig_msg):
        msg = {'id': orig_msg['id'],
               'plant-id': self.get_plantId(orig_msg),
               'state': 'finished',
               'timestamp': get_time_millis(),
               'reason': {'finish-state': 'success'}}
        self.__enque_to_rmq(self.exchange, self.routing_key, json.dumps(msg))

    def make_observation(self, key, value, timestamp=None):
        if timestamp is not None:
            return {'field': key, 'value': value, 'timestamp': timestamp}
        else:
            return {'field': key, 'value': value}

    def observations(self, orig_msg, obs_vec, timestamp=get_time_millis()):
        obs_vec_copy = []
        for obs in obs_vec:
            obs_copy = obs.copy()
            # pprint.pprint(obs_copy)
            if 'timestamp' not in obs_copy:
                obs_copy['timestamp'] = timestamp
            obs_vec_copy.append(obs_copy)

        msg = {}
        if orig_msg is not None:
            msg['id'] = orig_msg['id']

        msg['plant-id'] = self.get_plantId(orig_msg)
        msg['state'] = 'observations'
        msg['timestamp'] = timestamp
        msg['observations'] = obs_vec_copy
        self.__enque_to_rmq(self.exchange, self.routing_key, json.dumps(msg))

    def binary_publish(self, routing_key, data):
        # print 'publishing data of len {}'.format(len(data))
        properties = pika.BasicProperties(content_type='application/x-binary')
        self.__enque_to_rmq(self.exchange, routing_key, data, properties)
        # print '---- done my generic_publish\n'

    def process_to_rmq(self):
        n_waiting = len(self.to_rmq)

        if n_waiting > 0:
            for i in range(n_waiting):
                # print 'plant.rmq_is_idle is sending ', n_waiting
                msg = self.to_rmq.popleft()
                self.__basic_publish(msg['exchange'], msg['routing-key'], msg['data'], msg['properties'])

    def rmq_is_idle(self):
        # This method is called whenever RMQ has free time.
        # We send messages to rmq on behalf of all threads
        self.process_to_rmq()
        if not self.done:
            self.connection.add_callback_threadsafe(self.rmq_is_idle)

    def __enque_to_rmq(self, exchange, routing_key, data, properties=None):
        self.to_rmq.append({'exchange': exchange, 'routing-key': routing_key, 'data': data, 'properties': properties})

    def __basic_publish(self, exchange, routing_key, data, properties=None):
        th = threading.current_thread()

        if th.ident != self.channel_thread.ident or th.getName() != self.channel_thread.getName():
            print 'WARN: ', 'discrepancy in plant create thread and outgoing thread sending the outgoing message'
            print 'current thread', th.ident, th.getName()
            print 'channel thread', self.channel_thread.ident, self.channel_thread.getName()

        if properties is None:
            self.channel.basic_publish(exchange, routing_key, data)
        else:
            self.channel.basic_publish(exchange, routing_key, data, properties)
