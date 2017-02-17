lb-transfer
================
This Python script is a client-side component for sending files to a Laika BOSS cluster running the laikad service in asynchronous mode. The primary motivation for creating this script is to send files from an NSM sensor to a LB cluster. See TODO for future work.

TODO
---

* Add error checking for failed file transfers and failed file removals
* Add client-side logging of scheduled job status (execution time, number of files sent, etc.)
