# findbro.py v0.1
# Matches Bro logs against a specified list of UIDs
# Can run on N number of Bro logs
# Performs no error checking
# Should only be run on directories that contains only gzip Bro logs
# Best way to collect UIDs is via bro-cut and grep
# 
# Josh Liburdi 2016

from os import listdir
import sys
import gzip
import argparse

def write_file(fout_name,file_contents):
    fout = gzip.open(fout_name, 'w')
    fout.write(file_contents)
    fout.close()

def proc_bro(fout_name,input,uid_list):
  file_cache = ''
  with gzip.open(input) as fin:
    lines = fin.readlines()
    file_cache += lines[6]
    file_cache += lines[7]
    for line in lines[8:-1]:
      if any(uid in line for uid in uid_list): 
        file_cache += line

  if len(file_cache.split('\n')) == 3:
    print 'No matches in %s' % input
  else:
    print '%d matches in %s' % ( (len(file_cache.split('\n')) - 3), input )
    write_file(fout_name,file_cache)

def main():
  parser = argparse.ArgumentParser(description='Merge Bro logs from a single day')
  parser.add_argument('--bro-dir', '-bd', dest='directory', action='store')
  parser.add_argument('--label', '-l', dest='label', action='store', default=None)
  parser.add_argument('--uid', '-u', dest='uid_file', action='store')
  argsout = parser.parse_args()

  dir_list = listdir(argsout.directory)
  log_dict = {}
  uid_list = [line.strip() for line in open(argsout.uid_file, 'r')]

  for log_file in dir_list:
    log_type = log_file.split('.')[0]
    log_dict.setdefault(log_type,[]).append(log_file)

  for key,list_val in log_dict.iteritems():
    if argsout.label is None:
      fout_name = key + '.log.gz' 
    else:
      fout_name = key + '.' + argsout.label + '.log.gz'
 
    for f in list_val:
      fpath = argsout.directory + f
      proc_bro(fout_name,fpath,uid_list)

if __name__ == "__main__":
  main()
