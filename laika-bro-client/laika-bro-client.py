from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import json
from argparse import ArgumentParser
import time
import os
import multiprocessing as mp
from laikaboss.objectmodel import ExternalObject, ExternalVars
from laikaboss.clientLib import Client

'''
The queue where to-be-scanned file paths go.
'''
job_queue = mp.Queue()

'''
Function that takes a Bro supplied filename
and turns it into JSON metadata for Laika BOSS.
'''
def fname_to_json(fname):
    fname_split = fname.rsplit('/',1)[1].split('_')

    json_d = { "log_source": fname_split[0], "file_id": fname_split[1], "src_addr": fname_split[2], "dst_addr": fname_split[3] }
    return json.loads(json.dumps(json_d))

'''
Class and function that monitors the input
directory and adds files to the queue.
'''
class laika_watcher(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
             job_queue.put(event.src_path)

'''
Function that defines the worker routine 
for each subprocess.
'''
def laika_worker(broker):
    client = Client(broker, async=True)
    for fname in iter(job_queue.get, None):
         with open(fname, 'rb') as f:
             file_buffer = f.read()
             externalObject = ExternalObject(buffer=file_buffer,
                                             externalVars=ExternalVars(filename=fname,
                                                                       source='bro',
                                                                       extMetaData=fname_to_json(fname)),                                                                                                                            level='level_minimal')

             client.send(externalObject)
         os.remove(fname)

if __name__ == '__main__':
    parser = ArgumentParser(description=
            '''
            Prototype client to send files from a Bro sensor to a server running Laika BOSS (laikad). This script monitors a directory for files extracted by Bro and sends them to the Laika server. The laikad service is expected to be in asynchronous mode. This script requires a companion Bro script that extracts files with a specific filename pattern to a directory of the user's choice. Files will be deleted from the Bro sensor after being sent to the Laika server.
            ''')
    parser.add_argument('-a', '--address', action='store', dest='broker', default='tcp://localhost:5558',
            help='Laika BOSS broker address. (Default: tcp://localhost:5558)')
    parser.add_argument('-f', '--fpath', action='store', dest="fpath", default='',
            help='Path to the monitored directory. Files in this directory will be deleted. (No default)')
    parser.add_argument('-w', '--workers', action='store', type=int, dest='num_workers',
            help='Number of worker processes to use during file transfer. (Default: number of cores available on system.)')
    args = parser.parse_args()
    
    if args.num_workers:
        num_workers = args.num_workers
    else:
        num_workers = mp.cpu_count()

    event_handler = laika_watcher()
    observer = Observer()
    observer.schedule(event_handler, args.fpath, recursive=False)
    observer.start()

    p = mp.Pool(num_workers, laika_worker, (args.broker,))

    while 1:
        time.sleep(1)
    observer.join()
