from enum import Enum
import json
import requests


class AlreadySent(Exception):
    pass

class MissingParameter(Exception):
    pass

class UnknownParameter(Exception):
    pass

class Query:
    def __init__(self, uri="http://localhost:3000", node=None):
        if node:
            self.node = node
        else:
            self.node = Node(uri)
        self._find_clause = None
        self._where_clauses = []
        self._values = None
        self.error = None
    
    def find(self, clause):
        if self._values:
            raise AlreadySent()
        self._find_clause = clause
        return self

    def where(self, clause):
        if self._values:
            raise AlreadySent()
        self._where_clauses.append(clause)
        return self

    def values(self):
        if not self._where_clauses:
            raise Exception("No Where Clause")
        _query = """
        {
            :find [%s]
            :where [
                [%s]
            ]
        }
        """ % (self._find_clause, ']\n['.join(self._where_clauses))
       
        result = self.node.query(_query)
        if type(result) is dict:
            self.error = json.dumps(result, indent=4, sort_keys=True)
            return []
        return result

    def __iter__(self):
        self._values = self.values()
        return self
    
    def __next__(self):
        if len(self._values):
            return self._values.pop()
        else:
            raise StopIteration

class TxOps:

    def __init__(self):
        self.ops = []

    def put(self, rec, valid_time=None, end_valid_time=None):
        op = ['put', rec]
        if valid_time:
            op.append(valid_time)
        if valid_time and end_valid_time:
            op.append(end_valid_time)
        self.ops.append(op)

    def delete(self, eid, valid_time=None, end_valid_time=None):
        op = ['delete', eid]
        if valid_time:
            op.append(valid_time)
        if valid_time and end_valid_time:
            op.append(end_valid_time)
        self.ops.append(op)

    def evict(self, eid):
        self.ops.append(['evict', eid])

    def match(self, eid, rec, ops, valid_time=None):
        self.ops.append(['match', eid, rec, ops])

    def get_all(self):
        return {'tx-ops': self.ops}


class Node:

    def __init__(self, uri="http://localhost:3000"):
        self.uri = uri

    def find(self, find_clause):

        return Query(node=self).find(find_clause)

    def parse_kwargs(self, known_args, provided_args):
        params = {}
        for k,v in provided_args.items():
            if k not in known_args:
                raise UnknownParameter(f'Unknown parameter: {k}')
            if v is not None:
                params[k.replace("_","-").replace('Q','?')] = v
        return params

    def call_rest_api(self, action, params={}):

        endpoint = "{}/_xtdb/{}".format(self.uri, action)

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }

        rsp = requests.get(endpoint, headers=headers, params=params)
        rsp.raise_for_status()
        return rsp.json()

    def submitTx(self, txops):
        action = "submit-tx"
        endpoint = "{}/_xtdb/{}".format(self.uri, action)
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }
        rsp = requests.post(endpoint, headers=headers, json=txops.get_all())
        rsp.raise_for_status()
        return rsp.json()

    def put(self, rec, valid_time=None, end_valid_time=None):
        op = TxOps()
        op.put(rec, valid_time, end_valid_time)
        return self.submitTx(op)

    def delete(self, eid, valid_time=None, end_valid_time=None):
        op = TxOps()
        op.delete(eid, valid_time, end_valid_time)
        return self.submitTx(op)

    def evict(self, eid):
        op = TxOps()
        op.evict(eid)
        return self.submitTx(transaction)

    def query(self, query, **kwargs):
        query=query.strip()
        # basic syntax check
        assert query.startswith("{") and query.endswith("}")

        known_args = [
            "valid_time",
            'tx_time',
            'tx_id'
        ]
        params = self.parse_kwargs(known_args, kwargs)

        action = "query"
        endpoint = "{}/_xtdb/{}".format(self.uri, action)
        headers = {"Accept": "application/json", "Content-Type": "application/edn"}
        query = "{:query %s}" % query
        rsp = requests.post(endpoint, headers=headers, data=query, params=params)
        return rsp.json()

    def status(self):
        action = "status"
        return self.call_rest_api(action)

    def entity(self, **kwargs):
        action = "entity"
        known_args = [
            'eid',
            'eid_json',
            'eid_edn',
            'valid_time',
            'tx_time',
            'tx_id'
        ]
        params = self.parse_kwargs(known_args, kwargs)
        return self.call_rest_api(action, params)

    def entityHistory(self, **kwargs):
        action = "entity?history=true"
        known_args = [
            'eid',
            'eid_json',
            'eid_edn',
            'sort_order',
            'with_corrections',
            'with_docs',
            'start_valid_time',
            'start_tx_time',
            'start_tx_id',
            'end_valid_time',
            'end_tx_time',
            'end_tx_id'
        ]
        params = self.parse_kwargs(known_args, kwargs)
        return self.call_rest_api(action, params)

    # -- TODO: explicit optional params for these, as named Python kwargs.

    def entityTx(self, **kwargs):
        action = "entity-tx"
        known_args = [
            'eid',
            'eid_json',
            'eid_edn',
            'valid_time',
            'tx_time',
            'tx_id'
        ]
        params = self.parse_kwargs(known_args, kwargs)
        return self.call_rest_api(action, params)

    def attributeStats(self):
        action = "attribute-stats"
        return self.call_rest_api(action)

    def sync(self, **kwargs):
        action = "sync"
        known_args = ['timeout']
        params = self.parse_kwargs(known_args, kwargs)
        return self.call_rest_api(action, params)

    def awaitTx(self, **kwargs):
        action = "await-tx"
        known_args = ['tx_id', 'timeout']
        params = self.parse_kwargs(known_args, kwargs)
        return self.call_rest_api(action, params)

    def awaitTxTime(self, **kwargs):
        action = "await-tx-time"
        known_args = ['tx_time', 'timeout']
        params = self.parse_kwargs(known_args, kwargs)
        return self.call_rest_api(action, params)

    def txLog(self, **kwargs):
        action = "tx-log"
        known_args = ['after_tx_id', 'with_opsQ']
        params = self.parse_kwargs(known_args, kwargs)
        return self.call_rest_api(action, params)

    def txCommitted(self, **kwargs):
        action = "tx-committed"
        known_args = ['tx_id']
        params = self.parse_kwargs(known_args, kwargs)
        return self.call_rest_api(action, params)

    def latestCompletedTx(self):
        action = "latest-completed-tx"
        return self.call_rest_api(action)

    def latestSubmittedTx(self):
        action = "latest-submitted-tx"
        return self.call_rest_api(action)

    def activeQueries(self):
        action = "active-queries"
        return self.call_rest_api(action)

    def recentQueries(self):
        action = "recent-queries"
        return self.call_rest_api(action)

    def slowestQueries(self):
        action = "slowest-queries"
        return self.call_rest_api(action)
