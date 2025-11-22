```
# Web Server

Port: 8000

Exposes client-facing APIs /upload, /photo/{id}, /photo/{id} DELETE and internal endpoint /internal/replication/trigger invoked by RM.

Relies on Directory Service (8001), Store Service (8002), Replication Manager (8003) and Redis (cache).
```