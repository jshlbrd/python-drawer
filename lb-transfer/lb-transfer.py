from argparse import ArgumentParser
import time
import schedule
import os
import multiprocessing as mp
from random import randint
from laikaboss.objectmodel import ExternalObject, ExternalVars
from laikaboss.constants import level_minimal
from laikaboss.clientLib import Client

input_queue = mp.Queue()

def async_worker(broker):
    client = Client(broker, async=True)
    randNum = randint(1,10000)

    for fname in iter(input_queue.get, None):
        print 'Worker %s sending new object' % randNum
        file_buffer = open(fname, 'rb').read()
        externalObject = ExternalObject(buffer=file_buffer,
                                        externalVars=ExternalVars(filename=fname,
                                                                  source="monitored-dir"),                                             
                                        level="level_minimal")

        client.send(externalObject)
        os.remove(fname)

def kick(broker,fpath,procs):
    if len(os.listdir(fpath)) != 0:
        for fname in os.listdir(fpath):
            ffile_path = fpath + fname
            input_queue.put(ffile_path)

        for i in xrange(procs):
            input_queue.put(None)

        for i in xrange(procs):
            mp.Process(target=async_worker, args=(broker,)).start()

def main(broker,fpath,procs,sched_time):
    schedule.every(sched_time).minutes.do(kick,broker,fpath,procs)
    while 1:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    parser = ArgumentParser(description=
            '''
            A simple script to send files from a monitored directory to a laikad server running asynchronously. The intended use for this script is to send files in batches from an NSM server to a Laika BOSS cluster. Performs no error checking and files are removed from the client after they are sent to the server.
            ''')
    parser.add_argument('-b', '--broker', action='store', dest='broker', default='tcp://localhost:5558',
            help='Laika BOSS broker (Default: tcp://localhost:5558)')
    parser.add_argument('-f', '--fpath', action='store', dest="fpath", default="",
            help='Path to the monitored directory. Files in this directory will be deleted.')
    parser.add_argument('-p', '--processors', action='store', type=int, dest="num_processors", default=6,
            help='Number of processors to use during file transfer')
    parser.add_argument('-t', '--time', action='store', type=int, dest="sched_time", default=5,
            help='Number of minutes for the scheduler to check for new files. Defaults to every five minutes.')
    args = parser.parse_args()

    main(args.broker, args.fpath, args.num_processors, args.sched_time)
