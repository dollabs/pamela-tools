#!/usr/bin/env python

# Copyright 2019 Dynamic Object Language Labs Inc.
#
# This software is licensed under the terms of the
# Apache License, Version 2.0 which can be found in
# the file LICENSE at the root of this distribution.



# A trivial example of a plant that executes any command for 2 seconds.
# This is for trivial testing only. You may see exceptions like this one
# # Expect to see pika.exceptions.ConnectionClosed: (505, 'UNEXPECTED_FRAME - expected content header for class 60, got non content header frame instead')
import sys
import time
import argparse
import plant
import threading

# Global
plant_g = None
fail_all_activities = False

def sim_command(msg):

    # print 'dispaching command:{} {}\n'.format(msg['function-name'], msg['args']),# Thread safe
    print 'dispatching command:', msg['function-name'], msg['args'] # Not threadsafe
    plant_g.started(msg)
    time.sleep(2)
    if fail_all_activities:
        plant_g.failed(msg, 'I will fail all activities')
    else:
        plant_g.finished(msg)

# Rabbit MQ incoming message handler function
def dispatch_func(msg, routing_key):

    if 'function-name' in msg:
        sim_command(msg)
        # threading.Thread(target=sim_command,args=[msg]).start() # Pika does not seem to like sending messages from different threads
    # else:
    #     print msg





def main(args):

    global plant_g, fail_all_activities

    if args.fail:
        print 'Args fail', args.fail
        fail_all_activities = True

    plantid = args.plantid
    if plantid == '':
        plantid = '#'

    local_plant = plant.Plant(plantid, args.exchange, args.host, args.port)
    plant_g = local_plant

    local_plant.wait_for_messages(dispatch_func)
    local_plant.close()
    plant_g = None


if __name__ == "__main__":
    print("plant-main.py as script")
    parser = argparse.ArgumentParser(description='Plant Sim (Python)')

    parser.add_argument('--host', default='localhost', help='RMQ host')
    parser.add_argument('-p', '--port', default=5672, help='RMQ Port', type=int)
    parser.add_argument('-e', '--exchange', default='tpn-updates', help='RMQ Exchange')
    parser.add_argument('--plantid', default="plant", help='default plant-id')
    parser.add_argument('--fail', dest='fail', action='store_true', help='Will fail all activities')
    args = parser.parse_args()
    sys.exit(main(args))
