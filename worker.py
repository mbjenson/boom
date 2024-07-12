import time
import uuid
import zmq
import random

# compute the hash of the current python file
# this is used to authenticate the worker on the controller
import hashlib
import os

WORKER_FILE = os.path.abspath(__file__)
WORKER_FILE_HASH = hashlib.md5(open(WORKER_FILE, 'rb').read()).hexdigest()
WORKER_FILE_HASH = WORKER_FILE_HASH[:10]

CONTROL_PORT = 5755

class Job(object):
    def __init__(self, work):
        self.id = uuid.uuid4().hex
        self.work = work

class Worker(object):
    def __init__(self, control_port=CONTROL_PORT):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.identity = uuid.uuid4().hex.encode('ascii')[0:10]
        self.socket.connect(f'tcp://127.0.0.1:{control_port}')
        self._auth()

    def run(self):
        try:
            # Send a connect message
            self.socket.send_json({'message': 'connect'})
            # Poll the socket for incoming messages. This will wait up to
            # 0.1 seconds before returning False. The other way to do this
            # is is to use zmq.NOBLOCK when reading from the socket,
            # catching zmq.AGAIN and sleeping for 0.1.
            n_retries = 0
            while True:
                if self.socket.poll(100):
                    # Note that we can still use send_json()/recv_json() here,
                    # the DEALER socket ensures we don't have to deal with
                    # client ids at all.
                    try:
                        job_id, work = self.socket.recv_json()
                    except ValueError:
                        print('Received invalid JSON, ignoring')
                        continue
                    self.socket.send_json(
                        {'message': 'job_done',
                         'result': self._do_work(work),
                         'job_id': job_id,
                         'worker_hash': WORKER_FILE_HASH,
                        })
                    n_retries = 0
                elif n_retries > 100:
                    print(f'Did not receive any work for >10 seconds, checking if controller is still alive')
                    if not self._controller_still_alive():
                        print('Controller is not responding, trying again in 5 seconds')
                        time.sleep(5)
                    else:
                        self._reconnect()
                        n_retries = 0
                    
                else:
                    # sleep for a bit to avoid busy waiting
                    time.sleep(0.1)
                    print(f'Worker {str(self.socket.identity)} is waiting for work')
                    n_retries += 1
        finally:
            self._disconnect()

    
    def _do_work(self, work):
        result = work['number'] ** 2
        # sleep randomly between 0.1 and 0.5 seconds to simulate work
        time.sleep(random.uniform(0.1, 0.5))
        print(f'Worker {self.socket.identity} squared {work["number"]} to get {result}')
        return result
    
    def _auth(self):
        self.socket.send_json({'message': 'auth', 'worker_hash': WORKER_FILE_HASH})
    
    def _disconnect(self):
        """Send the Controller a disconnect message and end the run loop.
        """
        self.socket.send_json({'message': 'disconnect', 'worker_hash': WORKER_FILE_HASH})

    def _controller_still_alive(self):
        # send a ping to the controller
        self.socket.send_json({'message': 'ping', 'worker_hash': WORKER_FILE_HASH})
        n_retries = 0
        while True:
            if self.socket.poll(100):
                response = self.socket.recv_json()
                if response.get('message') == 'pong':
                    return True
            elif n_retries > 100:
                break
            else:
                # sleep for a bit to avoid busy waiting
                time.sleep(0.1)
                n_retries += 1
        return False

    def _reconnect(self):
        self._disconnect()
        self.socket.connect(f'tcp://127.0.0.1:{CONTROL_PORT}')
        self._auth()


if __name__ == '__main__':
    worker = Worker()
    worker.run()

            