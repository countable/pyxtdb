import json
import pytest
import os
import pyxtdb
from pyxtdb import Symbol, Keyword
import edn_format

XTDB_URL = os.environ.get('XTDB_URL', 'http://localhost:3000')

@pytest.fixture
def node():
    # create a connection and evict all records
    node = pyxtdb.Node(XTDB_URL)
    records = node.find('?e').where('?e :xt/id')
    ids = [rec[0] for rec in records]
    result = node.evict(ids).submit()
    return node

@pytest.fixture
def billies_db(node):
    records = [
        {'xt/id': 'billyi', 'name': 'Billy', 'last-name': 'Idol', 'profession': 'singer'},
        {'xt/id': 'billyj', 'name': 'Billy', 'last-name': 'Joel', 'profession': 'singer'},
        {'xt/id': 'billyb', 'name': 'Billy', 'last-name': 'Bob',  'profession': 'actor'},
    ]
    result = node.put(records).submit()
    return node

def billies(result):
    return len([rec for rec in result if rec[0]=='billy'])

def test_put_and_query(billies_db):
    node = billies_db

    result = node.put({'xt/id': 'billy', 'name': 'Billy', 'last-name': 'Elliot'}).submit()

    # Fetch ALL records
    result = node.query(r'{:find [?e], :where [[?e :xt/id]]}')
    # One should have xt/id "billy"
    assert billies(result) == 1

    # Ensure it's gone again after deletion.
    result = node.evict('billy').submit()
    # Fetch ALL records
    result = node.query(r'{:find [?e], :where [[?e :xt/id]]}')
    # None should have xt/id "billy"
    assert billies(result) == 0

def test_bad_query_inputs(billies_db):
    node = billies_db

    with pytest.raises(pyxtdb.BadEDNString) as e:
        result = node.query(query='{')            # brace not closed
    assert 'EOF Reached' in str(e)

    with pytest.raises(pyxtdb.BadEDNString) as e:
        result = node.query(query='{:find [?e] :where [[?e :xt/id]]}',
                            in_args='[1]]')      # too many right brackets
    assert 'VECTOR_END' in str(e)

    with pytest.raises(pyxtdb.XtdbError) as e:
        result = node.query(query='{:find [?e]} :where [bogus]')
    assert "Client Error" in str(e)

def test_query_with_args(billies_db):
    node = billies_db

    # scalar argument
    result = node.query(query='{:find [?e] :where [[?e :last-name last]] :in [last]}',
                        in_args=['Joel'])
    assert len(result) == 1

    # multiple scalars
    result = node.query(query='{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [first last]}',
                        in_args=['Billy', 'Joel'])
    assert len(result) == 1

    # tuple
    result = node.query(query='{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [[first last]]}',
                        in_args=[['Billy', 'Joel']])
    assert len(result) == 1

    # tuple and scalar
    result = node.query(
        query="""
        {
          :find [?e]
          :where [
            [?e :name first]
            [?e :last-name last]
            [?e :profession job]
          ]
          :in [[first last] job]
        }
        """,
        in_args=[['Billy', 'Joel'], 'singer'])
    assert len(result) == 1

    # collection
    result = node.query(query='{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [[[first last]]]}',
                        in_args=[[['Billy', 'Joel'], ['Billy', 'Idol']]])
    assert len(result) == 2

    # in_args can be edn-formatted string rather than a collection
    result = node.query(query='{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [[[first last]]]}',
                        in_args='[[["Billy" "Joel"] ["Billy" "Idol"]]]')
    assert len(result) == 2

def test_query_model_strings(billies_db):
    node = billies_db

    # Fetch records with name "Billy"
    result = node.find('?e').where('?e :name "Billy"')
    assert len(list(result)) == 3
    assert result.error == None

    # Fetch records with last name "Joel"
    result = node.find('?e').where('?e :last-name "Joel"')
    assert len(list(result)) == 1
    assert result.error == None

    # scalar argument
    result = node.find('?e').where('?e :last-name last').in_('last').in_args('Joel')
    assert str(result) == '{:find [?e] :where [[?e :last-name last]] :in [last]}'
    assert len(list(result)) == 1

    # multiple scalars
    result = node.find('?e') \
                 .where('?e :name first') \
                 .where('?e :last-name last') \
                 .in_('first last') \
                 .in_args('Billy', 'Joel')
    assert str(result) == '{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [first last]}'
    assert len(list(result)) == 1

    # tuple
    result = node.find('?e') \
                 .where('?e :name first') \
                 .where('?e :last-name last') \
                 .in_('[first last]') \
                 .in_args(['Billy', 'Joel'])
    assert str(result) == '{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [[first last]]}'
    assert len(list(result)) == 1

    # tuple and scalar
    result = node.find('?e') \
                 .where('?e :name first') \
                 .where('?e :last-name last') \
                 .where('?e :profession job') \
                 .in_('[first last] job') \
                 .in_args(['Billy', 'Joel'], 'singer')
    assert str(result) == '{:find [?e] :where [[?e :name first] [?e :last-name last] [?e :profession job]] :in [[first last] job]}'
    assert len(list(result)) == 1

    # collection
    result = node.find('?e') \
                 .where('?e :name first') \
                 .where('?e :last-name last') \
                 .in_('[[first last]]') \
                 .in_args([['Billy', 'Joel'], ['Billy', 'Idol']])
    assert str(result) == '{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [[[first last]]]}'
    assert len(list(result)) == 2

def test_query_model_edn(billies_db):
    node = billies_db

    # scalar argument
    result = node.find(Symbol('?e')) \
                 .where(Symbol('?e'), Keyword('last-name'), Symbol('last')) \
                 .in_(Symbol('last')) \
                 .in_args('Joel')
    assert str(result) == '{:find [?e] :where [[?e :last-name last]] :in [last]}'
    assert len(list(result)) == 1

    # multiple scalars
    result = node.find(Symbol('?e')) \
                 .where(Symbol('?e'), Keyword('name'), Symbol('first')) \
                 .where(Symbol('?e'), Keyword('last-name'), Symbol('last')) \
                 .in_(Symbol('first'), Symbol('last')) \
                 .in_args('Billy', 'Joel')
    assert str(result) == '{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [first last]}'
    assert len(list(result)) == 1

    # tuple
    result = node.find(Symbol('?e')) \
                 .where(Symbol('?e'), Keyword('name'), Symbol('first')) \
                 .where(Symbol('?e'), Keyword('last-name'), Symbol('last')) \
                 .in_([Symbol('first'), Symbol('last')]) \
                 .in_args(['Billy', 'Joel'])
    assert str(result) == '{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [[first last]]}'
    assert len(list(result)) == 1

    # tuple and scalar
    result = node.find(Symbol('?e')) \
                 .where(Symbol('?e'), Keyword('name'), Symbol('first')) \
                 .where(Symbol('?e'), Keyword('last-name'), Symbol('last')) \
                 .where(Symbol('?e'), Keyword('profession'), Symbol('job')) \
                 .in_([Symbol('first'), Symbol('last')], Symbol('job')) \
                 .in_args(['Billy', 'Joel'], 'singer')
    assert str(result) == '{:find [?e] :where [[?e :name first] [?e :last-name last] [?e :profession job]] :in [[first last] job]}'
    assert len(list(result)) == 1

    # collection
    result = node.find(Symbol('?e')) \
                 .where(Symbol('?e'), Keyword('name'), Symbol('first')) \
                 .where(Symbol('?e'), Keyword('last-name'), Symbol('last')) \
                 .in_([[Symbol('first'), Symbol('last')]]) \
                 .in_args([['Billy', 'Joel'], ['Billy', 'Idol']])
    assert str(result) == '{:find [?e] :where [[?e :name first] [?e :last-name last]] :in [[[first last]]]}'
    assert len(list(result)) == 2

def test_kwargs():

    known_args = ['my-foo', 'my-bar?', 'my-json', 'my-edn']

    # confirm keyword underscores become hyphens
    params = pyxtdb.Node._parse_kwargs(known_args, {'my_foo': 1})
    assert 'my-foo' in params
    assert params['my-foo'] == 1

    # confirm keyword Qs become ?s
    params = pyxtdb.Node._parse_kwargs(known_args, {'my_barQ': 1})
    assert 'my-bar?' in params
    assert params['my-bar?'] == 1

    # raise exception if keyword not in known_args
    with pytest.raises(pyxtdb.UnknownParameter) as e:
        params = pyxtdb.Node._parse_kwargs(known_args, {'unknown': 'xyzzy'})
    assert 'Unknown parameter: unknown' in str(e.value)

    # automatic json conversion for keywords ending -json
    params = pyxtdb.Node._parse_kwargs(known_args, {'my_json': {'a': 1}})
    assert params['my-json'] == '{"a": 1}'

    # automatic edn conversion for keywords ending -edn
    params = pyxtdb.Node._parse_kwargs(known_args, {'my_edn': {'a': 1}})
    assert params['my-edn'] == '{"a" 1}'

def test_txops(node):

    # add a record
    tx = node.put({'xt/id': 14, 'band': 'King Crimson', 'name': 'Greg', 'last-name': 'Lake'}) \
             .submit()
    assert 'txId' in tx and 'txTime' in tx

    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(result) == 1

    # add multiple records
    records = [
        {'xt/id': 10, 'band': 'King Crimson', 'name': 'Bill',   'last-name': 'Bruford'},
        {'xt/id': 11, 'band': 'King Crimson', 'name': 'Robert', 'last-name': 'Fripp'},
        {'xt/id': 12, 'band': 'King Crimson', 'name': 'Tony',   'last-name': 'Levin'},
        {'xt/id': 13, 'band': 'King Crimson', 'name': 'Adrian', 'last-name': 'Belew'}
    ]
    tx = node.put(records).submit()
    assert 'txId' in tx and 'txTime' in tx

    result = node.query('{:find [(pull ?e [*])] :where [[?e :band "King Crimson"]]}')
    assert len(result) == 5
    records = [x[0] for x in result]

    # malformed records
    with pytest.raises(pyxtdb.XtdbError) as e:
        result = node.put({'malformed': 'this has no xt/id'}).submit()

    # delete one record
    tx = node.delete(eid=14).submit()
    assert 'txId' in tx and 'txTime' in tx

    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(result) == 4

    # delete multiple records
    tx = node.delete(eid=[11,13]).submit()
    assert 'txId' in tx and 'txTime' in tx

    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(result) == 2

    # delete nonexistent record is NOT an error
    tx = node.delete(eid=9999).submit()
    assert 'txId' in tx and 'txTime' in tx

    # evict nonexistent record is NOT an error
    tx = node.delete(eid=9999).submit()
    assert 'txId' in tx and 'txTime' in tx

    # use match to update the two remaining records
    for rec in records:
        tx = node.match(rec['xt/id'], rec) \
                 .put({**rec, 'album': 'Discipline'}) \
                 .submit()
        assert 'txId' in tx and 'txTime' in tx

    # still only two records
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(result) == 2

    # but now they have an album
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"] [?e :album "Discipline"]]}')
    assert len(result) == 2

    # use match against Nil to re-insert the deleted records 
    for rec in records:
        tx = node.match(rec['xt/id'], None) \
                 .put({**rec, 'album': 'Discipline'}) \
                 .submit()
        assert 'txId' in tx and 'txTime' in tx

    # now we have 5 records
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"]]}')
    assert len(result) == 5

    # and they have an album
    result = node.query('{:find [?e] :where [[?e :band "King Crimson"] [?e :album "Discipline"]]}')
    assert len(result) == 5
