import json
import pytest
import os
import pyxtdb

XTDB_URL = os.environ.get('XTDB_URL', 'http://localhost:3000')

@pytest.fixture
def node():
    return pyxtdb.Node(XTDB_URL)

def billies(result):
    return len([rec for rec in result if rec[0]=='billy'])

def test_put_and_query(node):
    # Remove record with xt/id "billy", check it's gone.
    result = node.evict("billy").submit()
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # None should have xt/id "billy"
    assert billies(result) == 0

    # Create a record and ensure it's found.
    result = node.put({"xt/id": "billy", "name": "Billy", "last-name": "Idol"}).submit()
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # One should have xt/id "billy"
    assert billies(result) == 1

    # Ensure it's gone again after deletion.
    result = node.evict("billy").submit()
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # None should have xt/id "billy"
    assert billies(result) == 0

def test_query_model(node):

    # Remove records with name "Billy"
    result = node.evict(1) \
                 .evict(2) \
                 .evict(3) \
                 .submit()
    # Fetch records with name "Billy"
    result = node.find("?e").where("?e :name \"Billy\"")
    # None should have xt/id "billy"
    assert len(list(result)) == 0

    # Create a record and ensure it's found.
    result = node.put({"xt/id": 1, "name": "Billy", "last-name": "Idol"}) \
                 .put({"xt/id": 2, "name": "Billy", "last-name": "Joel"}) \
                 .put({"xt/id": 3, "name": "Billy", "last-name": "Bob"}) \
                 .submit()
    
    # Fetch records with name "Billy"
    result = node.find("?e").where("?e :name \"Billy\"")
    assert len(list(result)) == 3

    result = node.find("?e").where('?e :last-name "Joel"')
    assert len(list(result)) == 1
    assert result.error == None

def test_kwargs(node):

    known_args = ['my-foo', 'my-bar?', 'my-json', 'my-edn']

    # confirm keyword underscores become hyphens
    params = node.parse_kwargs(known_args, {'my_foo': 1})
    assert 'my-foo' in params
    assert params['my-foo'] == 1

    # confirm keyword Qs become ?s
    params = node.parse_kwargs(known_args, {'my_barQ': 1})
    assert 'my-bar?' in params
    assert params['my-bar?'] == 1

    # raise exception if keyword not in known_args
    with pytest.raises(pyxtdb.UnknownParameter) as e:
        params = node.parse_kwargs(known_args, {'unknown': 'xyzzy'})
    assert 'Unknown parameter: unknown' in str(e.value)

    # automatic json conversion for keywords ending -json
    params = node.parse_kwargs(known_args, {'my_json': {'a': 1}})
    assert params['my-json'] == '{"a": 1}'

    # automatic edn conversion for keywords ending -edn
    params = node.parse_kwargs(known_args, {'my_edn': {'a': 1}})
    assert params['my-edn'] == '{"a" 1}'

def test_txops(node):
    records = [
        {"xt/id": 10, "band": "King Crimson", "name": "Bill",   "last-name": "Bruford"},
        {"xt/id": 11, "band": "King Crimson", "name": "Robert", "last-name": "Fripp"},
        {"xt/id": 12, "band": "King Crimson", "name": "Tony",   "last-name": "Levin"},
        {"xt/id": 13, "band": "King Crimson", "name": "Adrian", "last-name": "Belew"}
    ]

    # evict records
    ids = [rec['xt/id'] for rec in records]
    result = node.evict(ids).submit()

    # confirm they're gone
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(list(result)) == 0

    # now add the records in a single operation
    node.put(records).submit()

    # confirm they're there
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(list(result)) == 4

    # delete two records
    result = node.delete(eid=11) \
                 .delete(eid=13) \
                 .submit()
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(list(result)) == 2

    # use match to update the two remaining records
    for rec in records:
        result = node.match(rec['xt/id'], rec) \
                     .put({**rec, 'album': 'Discipline'}) \
                     .submit()

    # still only two records
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(list(result)) == 2

    # but now they have an album
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"] [?e :album "Discipline"]]}')
    assert len(list(result)) == 2
