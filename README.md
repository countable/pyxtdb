# pyxtdb

This is a minimal wrapper for the XTDB REST API for Python.

It includes some convenience functions for common DB operations.

To use this example, first spin up `xtdb` with `docker run -p 3000:3000 juxt/xtdb-in-memory:1.20.0`

Clone this repo and `pip install -r requirements.txt`

```
python

>>> import pyxtdb

>>> node = pyxtdb.Node('http://localhost:3000') # the rest API endpoint.

>>> node.put({"xt/id": "billy", "name": "Billy", "last-name": "Idol"})

>>> print(
    node.query(
        r'{:find [?e], :where [[?e :xt/id]]}'
    )
)

[['billy']]
```

## Tests

To run the tests, xtdb must be running as above, and you should install `pytest`

```
pytest tests.py
```

