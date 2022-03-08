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
    node.evict("billy")
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # None should have xt/id "billy"
    assert billies(result) == 0

    # Create a record and ensure it's found.
    node.put({"xt/id": "billy", "name": "Billy", "last-name": "Idol"})
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # One should have xt/id "billy"
    assert billies(result) == 1

    # Ensure it's gone again after deletion.
    node.evict("billy")
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # None should have xt/id "billy"
    assert billies(result) == 0

def test_query_model(node):

    # Remove record with xt/id "billy", check it's gone.
    node.evict(1)
    node.evict(2)
    node.evict(3)
    # Fetch ALL records
    result = node.find("?e").where("?e :xt/id")
    # None should have xt/id "billy"
    assert len(list(result)) == 0

    # Create a record and ensure it's found.
    node.put({"xt/id": 1, "name": "Billy", "last-name": "Idol"})
    node.put({"xt/id": 2, "name": "Billy", "last-name": "Joel"})
    node.put({"xt/id": 3, "name": "Billy", "last-name": "Bob"})
    
    # Fetch ALL records
    result = node.find("?e").where("?e :xt/id")
    assert len(list(result)) == 3

    result = node.find("?e").where('?e :last-name "Joel"')
    assert len(list(result)) == 1
    assert result.error == None
    print(result)
