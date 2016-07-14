# Converts Unified2 event artifacts to simple JSON file
# Primary use case is to convert Snort alerts to JSON
# Performs no error checking
#
# Josh Liburdi 2016

import unified2.parser
import argparse
import socket, struct
import json

def dec_to_ipv4(decimal):
  return socket.inet_ntoa(struct.pack('!L',decimal))

def main():
  parser = argparse.ArgumentParser(description='')
  parser.add_argument('--input', '-i', dest='input_file', action='store')
  argsout = parser.parse_args()

  evts = []
  for ev,ev_tail in unified2.parser.parse(argsout.input_file):
    if 'ip_source' in ev:
      ev['ip_source'] = dec_to_ipv4(ev['ip_source'])
    if 'ip_destination' in ev:
      ev['ip_destination'] = dec_to_ipv4(ev['ip_destination'])
    evts.append(ev)

  outfile = argsout.input_file + '.json'
  with open(outfile, 'wb') as fout:
    for item in evts:
      fout.write("%s\n" % item)

if __name__ == "__main__":
  main()
