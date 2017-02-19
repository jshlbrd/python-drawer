laika-bro-client.py
================
This Python script is a client-side component for sending files from a Bro network sensor to a Laika BOSS cluster running the laikad service in asynchronous mode. The script uses two external libraries (watchdog and schedule) that can be installed via pip. 

TODO
---

* Add error checking for fail states
* Add client-side logging of scheduled job status (execution time, number of files sent, etc.)
