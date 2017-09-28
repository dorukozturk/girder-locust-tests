# little changes from: https://github.com/pglass/designate-locust/blob/master/graphite_client.py
# See also: https://gist.github.com/lucindo/1f7739cdc9187b54d84e
import sys
import os
import time
import locust
import gevent

from gevent.socket import socket
from gevent.queue import Queue

from six.moves import range


graphite_queue = Queue()
user_count_map = {}
HOST = os.getenv('GRAPHITE_HOST', '127.0.0.1')
PORT = os.getenv('GRAPHITE_PORT', '2003')

def is_slave():
    return '--slave' in sys.argv

def graphite_worker():
    """The worker pops each item off the queue and sends it to graphite."""
    print('connecting to graphite on (%s, %s)' % (HOST, PORT))
    sock = socket()

    timeout = 0
    while True:
        try:
            sock.connect((HOST, PORT))
            break
        except Exception as e:
            if timeout < 10:
                print("Couldn't connect to Graphite server {0} on port {1}: {2}, trying again..."
                      .format(HOST, PORT, e))
                timeout += 1
                time.sleep(1)
            else:
                print("Couldn't connect to Graphite server {0} on port {1}: {2}, quitting."
                      .format(HOST, PORT, e))
                return

    print('Done connecting to graphite')

    while True:
        data = graphite_queue.get()
        # print "graphite_worker: got data {0!r}".format(data)
        # print("sending data")
        sock.sendall(data.encode('utf-8'))

def _get_requests_per_second_graphite_message(stat, client_id):
    request = stat['method'] + '.' + stat['name'].replace(' - ', '.').replace('/', '-')
    graphite_key = "locust.{0}.reqs_per_sec".format(request)
    graphite_data = "".join(
        "{0} {1} {2}\n".format(graphite_key, count, epoch_time)
        for epoch_time, count in stat['num_reqs_per_sec'].items())
    return graphite_data

def _get_response_time_graphite_message(stat, client_id):
    request = stat['method'] + '.' + stat['name'].replace(' - ', '.').replace('/', '-')
    graphite_key = "locust.{0}.response_time".format(request)
    epoch_time = int(stat['start_time'])

    # flatten a dictionary of {time: count} to [time, time, time, ...]
    response_times = []
    for t, count in stat['response_times'].items():
        for _ in range(count):
            response_times.append(t)

    graphite_data = "".join(
        "{0} {1} {2}\n".format(graphite_key, response_time, epoch_time)
        for response_time in response_times)
    return graphite_data

def graphite_producer(client_id, data):
    """This takes a Locust client_id and some data, as given to
    locust.event.slave_report handlers."""
    #print "Got data: ", data, 'from client', client_id
    for stat in data['stats']:
        graphite_data = (
            _get_response_time_graphite_message(stat, client_id)
            + _get_requests_per_second_graphite_message(stat, client_id))
        graphite_queue.put(graphite_data)

def setup_graphite_communication():
    # only the master sends data to graphite
    if not is_slave():
        gevent.spawn(graphite_worker)
        locust.events.slave_report += graphite_producer
