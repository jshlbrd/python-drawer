from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import schedule
import json
import time
import os
import multiprocessing as mp
from argparse import ArgumentParser
from laikaboss.objectmodel import ExternalObject, ExternalVars
from laikaboss.constants import level_minimal
from laikaboss.clientLib import Client

'''
The queue where file paths to-be-scanned are
stored.
'''
file_queue = mp.Queue()

'''
Function that handles inserting file paths
into the queue.
'''
class laika_watcher(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
             file_queue.put(event.src_path)

'''
Function that takes a Bro supplied filename 
and turns it into JSON metadata for Laika BOSS.
'''
def fname_to_json(fname):
    fname_split = fname.rsplit('/',1)[1].split('_')

    json_d = { "log_source": fname_split[0], "file_id": fname_split[1], "src_addr": fname_split[2], "dst_addr": fname_split[3] }
    return json.loads(json.dumps(json_d))

'''
Function that defines the worker routine 
for each subprocess. 
'''
def laika_worker(broker):
    client = Client(broker, async=True)
    for fname in iter(file_queue.get, None):
         with open(fname, 'rb') as f:
             file_buffer = f.read()
             externalObject = ExternalObject(buffer=file_buffer,
                                             externalVars=ExternalVars(filename=fname,
                                                                       source='bro',
                                                                       extMetaData=fname_to_json(fname)),                                             
                                             level='level_minimal')
             client.send(externalObject)
         os.remove(fname)

'''
Function that kicks off the workers.
'''
def kick(broker,num_workers,worker_timeout):
    if file_queue.qsize() != 0:        
        pool = []

        for i in xrange(num_workers):
            file_queue.put(None)
            pool.append(mp.Process(target=laika_worker, args=(broker,)))

        for p in pool:
            p.start()

        for p in pool:
            p.join(worker_timeout)

if __name__ == '__main__':
    parser = ArgumentParser(description=
            '''
            Prototype client to send files from a Bro sensor to a server running Laika BOSS (laikad). This script monitors a directory for files extracted by Bro and sends them to the Laika server. The laikad service is expected to be in asynchronous mode. This script requires a companion Bro script that extracts files with a specific filename pattern to a directory of the user's choice. Files will be deleted from the Bro sensor after being sent to the Laika server.
            ''')
    parser.add_argument('-a', '--address', action='store', dest='broker', default='tcp://localhost:5558',
            help='Laika BOSS broker address. (Default: tcp://localhost:5558)')
    parser.add_argument('-f', '--file-path', action='store', dest="fpath", default="",
            help='Path to the monitored directory. Files in this directory will be deleted. (No default)')
    parser.add_argument('-w', '--workers', action='store', type=int, dest="num_processors",
            help='Number of worker processes to use during file transfer. (Default: number of cores available on system * 2)')
    parser.add_argument('-st', '--schedule-time', action='store', type=int, dest="schedule_time", default=5,
            help='Number of minutes for the scheduler to kick off new file transfers. (Default: Every 5 minutes)')
    parser.add_argument('-wt', '--worker-timeout', action='store', type=int, dest="worker_timeout", default=30,
            help='Number of seconds for each worker process to timeout if it does not finish file transfer (this assists with reaping zombie processes). (Default: 60 seconds)')
    args = parser.parse_args()
    
    if args.num_processors:
        num_workers = args.num_processors
    else:
        num_workers = mp.cpu_count() * 2

    schedule.every(args.schedule_time).minutes.do(kick, args.broker, num_workers, args.worker_timeout)
    event_handler = laika_watcher()
    observer = Observer()
    observer.schedule(event_handler, args.fpath, recursive=False)
    observer.start()

    while 1:
        schedule.run_pending()
        time.sleep(1)
    observer.join()
