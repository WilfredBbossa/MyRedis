\# MyRedis



A miniature Redis-like in-memory key-value server written in Python, using gevent

for concurrent client handling and a Redis-style wire protocol.



\## Setup



&#x20;   python -m venv .venv

&#x20;   .venv\\Scripts\\activate

&#x20;   pip install gevent



\## Run the server



&#x20;   python server.py



\## Use the client



&#x20;   from server import Client

&#x20;   c = Client()

&#x20;   c.set('k1', 'v1')

&#x20;   c.get('k1')



\## Supported commands



GET, SET, DEL/DELETE, FLUSH, MGET, MSET

