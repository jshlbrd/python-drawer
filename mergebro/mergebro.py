# mergebro.py v0.1
# Merges Bro logs into a single file
# Performs no error checking
# Should only be run on directories that contains only gzip Bro logs
# Not recommended to be run on Bro logs that span more than one day (log entries will be out of order and the open and close time values will be inaccurate)
#
# Josh Liburdi 2016

from os import listdir
import sys
import gzip
import argparse

def proc_bro(fout,input,file_loc):
  with gzip.open(input) as fin:
    if file_loc == 'first':
      for line in fin.readlines()[0:-1]:
        fout.write(line)
    elif file_loc == 'last':
      for line in fin.readlines()[8:]:
        fout.write(line)
    elif file_loc == 'unknown':
      for line in fin.readlines()[8:-1]:
        fout.write(line)

def main():
  parser = argparse.ArgumentParser(description='Merge Bro logs from a single day')
  parser.add_argument('--bro-dir', '-bd', dest='directory', action='store')
  parser.add_argument('--label', '-l', dest='label', action='store', default=None)
  argsout = parser.parse_args()

  dir_list = listdir(argsout.directory)
  log_dict = {}

  for log_file in dir_list:
    log_type = log_file.split('.')[0]
    log_dict.setdefault(log_type,[]).append(log_file)

  for key,list_val in log_dict.iteritems():
    if argsout.label is None:
      fout_name = key + '.log.gz' 
    else:
      fout_name = key + '.' + argsout.label + '.log.gz'
 
    with gzip.open(fout_name, 'wb') as fout:
      for f in list_val:
        fpath = argsout.directory + f
        if f == list_val[0]:
          proc_bro(fout,fpath,'first')
        if f == list_val[-1]:
          proc_bro(fout,fpath,'last')
        else:
          proc_bro(fout,fpath,'unknown')

if __name__ == "__main__":
  main()
