# kv-store
A persistent in-memory Key Value store concept.

Key Ideas:
1. PUT(k,v): records Event in Log file, and then updates in-memory hashmap with {k,v}.
2. GET(k): returns the value from the hashmap for key _k_.
3. DELETE(k): records Event in Log file, and then updates in-memory hashmap by erasing key _k_.
3. Async Flushing: an asynchronously running scheduled "flushing" thread that periodically processes and "flushes" the latest batch of Events in the Log file to the database data files. It does so by compacting the latest batch of events into the final state for these affected keys, and then writing these updated values to the disk file.
4. On Startup, KV store object loads the existing disk into memory. 

Why is it persistent?
1. It is 100% persistent since writes must succeed on the Log file prior to updation in the in-memory hashmap. 
2. In the case of an outage, the service will restart and reattempt processing the Log file from the last checkpoint. 
3. In production-level code, this checkpoint must be preserved on disk. In this code however, this checkpoint is stored in-memory -- which means on every restart, the whole log file will be processed from the first event to compute the final state to be written to the disk (which is unnecessary but accurate, at least).
