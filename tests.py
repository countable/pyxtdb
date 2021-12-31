import pytest

import pyxtdb

@pytest.fixture
def node():
    return pyxtdb.Node()

def test_put_and_query(node):

    # Remove record with xt/id "billy", check it's gone.
    node.evict("billy")
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # One should have xt/id "billy"
    assert len([rec for rec in result if rec[0]=='billy']) == 0

    # Create a record and ensure it's found.
    node.put({"xt/id": "billy", "name": "Billy", "last-name": "Idol"})
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # One should have xt/id "billy"
    assert len([rec for rec in result if rec[0]=='billy']) == 1

    # Ensure it's gone again after deletion.
    node.evict("billy")
    # Fetch ALL records
    result = node.query(r"{:find [?e], :where [[?e :xt/id]]}")
    # One should have xt/id "billy"
    assert len([rec for rec in result if rec[0]=='billy']) == 0

