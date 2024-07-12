import time
import uuid
import zmq
import json

# compute the hash of the worker.py file
# this is used to authenticate the worker on the controller
# that way we can be sure that only workers we trust are allowed to connect
# and send results back
# for dev purposes, the controller can be started with the --skip-auth flag
import hashlib
import os

import argparse

WORKER_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'worker.py'))
WORKER_FILE_HASH = hashlib.md5(open(WORKER_FILE, 'rb').read()).hexdigest()
WORKER_FILE_HASH = WORKER_FILE_HASH[:10]

CONTROL_PORT = 5755

class Job(object):
    def __init__(self, work):
        self.id = uuid.uuid4().hex
        self.work = work

class Controller(object):
    """Generate jobs, send the jobs out to workers and collect the results.
    """
    def __init__(self, control_port=CONTROL_PORT, skip_auth=False):
        self.context = zmq.Context()
        self.socket: zmq.Socket = self.context.socket(zmq.ROUTER)
        self.socket.bind('tcp://*:{0}'.format(control_port))
        # We'll keep our workers here, this will be keyed on the worker id,
        # and the value will be a dict of Job instances keyed on job id.
        self.workers = {}
        # We won't assign more than 50 jobs to a worker at a time; this ensures
        # reasonable memory usage, and less shuffling when a worker dies.
        self.max_jobs_per_worker = 50
        # We'll keep a list of work that needs to be requeued here. This will
        # be a list of Job instances.
        self._work_to_requeue = []

        self.skip_auth = skip_auth

    def work_iterator(self):
        # iter() makes our xrange object into an iterator so we can use
        # next() on it.
        iterator = iter(range(0, 10000))
        while True:
            # Return requeued work first. We could simplify this method by
            # returning all new work then all requeued work, but this way we
            # know _work_to_requeue won't grow too large in the case of
            # many disconnects.
            if self._work_to_requeue:
                yield self._work_to_requeue.pop()
            # if we reached the end of the iterator, we're done
            elif not iterator:
                return
            else:
                yield Job({'number': next(iterator)})


    def _get_next_worker_id(self):
        """Return the id of the next worker available to process work. Note
        that this will return None if no clients are available.
        """
        # It isn't strictly necessary since we're limiting the amount of work
        # we assign, but just to demonstrate that we could have any
        # algorithm here that we wanted we'll find the worker with the least
        # work and try that.
        if not self.workers:
            return None
        worker_id, work = sorted(self.workers.items(), key=lambda x: len(x[1]))[0]
        if len(work) < self.max_jobs_per_worker:
            return worker_id
        # No worker is available. Our caller will have to handle this.
        return None
    
    def _handle_worker_message(self, worker_id, message):
        """Handle a message from the worker identified by worker_id.

        {'message': 'connect'}
        {'message': 'disconnect'}
        {'message': 'job_done', 'job_id': 'xxx', 'result': 'yyy'}
        """
        worker_hash = message.get('worker_hash')
        if not self.skip_auth and worker_hash != WORKER_FILE_HASH:
            print(f'Worker {worker_id} failed authentication, got hash {worker_hash}, expected {WORKER_FILE_HASH}. Ignoring.')
            return
        if message['message'] == 'auth' or (message['message'] == 'connect' and self.skip_auth):
            if worker_id not in self.workers:
                print(f'Worker {worker_id} connected')
                self.workers[worker_id] = {}
            elif len(self.workers[worker_id]) > 0:
                print(f'Worker {worker_id} reconnected')
                # if there is work left from that existing worker, we'll requeue it
                self._work_to_requeue.extend(self.workers[worker_id].values())
        elif message['message'] == 'ping':
            # worker is trying to assess if the controller is still alive
            # we'll just respond with a pong
            # and register the worker if it's not already in our list
            if worker_id not in self.workers:
                print(f'Worker {worker_id} reconnected')
                self.workers[worker_id] = {}
            self.socket.send_multipart([worker_id, json.dumps({'message': 'pong'}).encode('utf-8')])
        elif worker_id not in self.workers:
            # If we get a message from a worker that isn't in our list, we'll
            # just ignore it. This could happen the controller is restarted
            return
        elif message['message'] == 'disconnect':
            remaining_work = self.workers.pop(worker_id)
            # Remove the worker so no more work gets added, and put any
            # remaining work into _work_to_requeue
            self._work_to_requeue.extend(remaining_work.values())
            print(f'Worker {worker_id} disconnected')
        elif message['message'] == 'job_done':
            result = message['result']
            job = self.workers[worker_id].pop(message['job_id'])
            # _process_results() is just a trivial logging function so I've
            # omitted it from here, but you can find it in the final source
            # code.
            self._process_results(worker_id, job, result)
        else:
            raise ValueError(f'Unknown message type {message["message"]}')
    
    def run(self):
        for job in self.work_iterator():
            next_worker_id = None
            while next_worker_id is None:
                # listen for messages from workers, handle them, and then check
                # if we have a worker available to send work to.
                while self.socket.poll(0):
                    # Note that we're using recv_multipart() here, this is a
                    # special method on the ROUTER socket that includes the
                    # id of the sender. It doesn't handle the json decoding
                    # automatically though so we have to do that ourselves.
                    worker_id, message = self.socket.recv_multipart()
                    message = json.loads(message)
                    self._handle_worker_message(worker_id, message)
                next_worker_id = self._get_next_worker_id()
                
                # no available worker, sleep for a bit
                if next_worker_id is None:
                    time.sleep(0.1)
            
            # Send the job to the worker
            self.socket.send_multipart(
                [next_worker_id, json.dumps((job.id, job.work)).encode('utf-8')]
            )
            self.workers[next_worker_id][job.id] = job
            print(f'Sent job {job.id} to worker {next_worker_id}')

    def _process_results(self, worker_id, job, result):
        print(f'Worker {worker_id} finished job {job.id} with result {result}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-auth', action='store_true')

    control = Controller(skip_auth=parser.parse_args().skip_auth)
    control.run()

            