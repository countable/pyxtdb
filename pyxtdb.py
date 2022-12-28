from enum import Enum
import json

import edn_format
import requests


class AlreadySent(Exception):
    pass

class BadEDNString(Exception):
    pass

class BadQuery(Exception):
    pass

class MissingParameter(Exception):
    pass

class UnknownParameter(Exception):
    pass

class XtdbError(Exception):
    def __init__(self, desc, code):
        super().__init__(f'XtdbError({code}): {desc}')
        self.description = desc
        self.status_code = code


# Query Builder

class Symbol(edn_format.Symbol):
    pass

class Keyword(edn_format.Keyword):
    pass

class Char(edn_format.Char):
    pass

class Query:
    def __init__(self, uri="http://localhost:3000", node=None):
        if node:
            self.node = node
        else:
            self.node = Node(uri)
        self._find_clause = None
        self._in_clause = None
        self._where_clauses = []
        self._rules_clauses = []
        self._order_by_clauses = []
        self._limit = None
        self._offset = None
        self._values = None
        self._timeout = None
        self.error = None
    
    def find(self, *args):
        if self._values:
            raise AlreadySent()
        if len(args) == 0:
            raise ValueError('missing find arguments')
        if len(args) == 1 and type(args[0]) == str:
            self._find_clause = edn_format.loads('['+args[0]+']')
        else:
            self._find_clause = args
        return self

    def in_(self, *args):
        if self._values:
            raise AlreadySent()
        if len(args) == 0:
            raise ValueError('missing in arguments')
        if len(args) == 1 and type(args[0]) == str:
            self._in_clause = edn_format.loads('['+args[0]+']')
        else:
            self._in_clause = args
        return self

    def where(self, *args):
        if self._values:
            raise AlreadySent()
        if len(args) == 0:
            raise ValueError('missing where arguments')
        if len(args) == 1 and type(args[0]) == str:
            self._where_clauses.append(edn_format.loads('['+args[0]+']'))
        else:
            self._where_clauses.append(list(args))
        return self

    def rules(self, *args):
        if self._values:
            raise AlreadySent()
        if len(args) == 0:
            raise ValueError('missing rules arguments')
        if len(args) == 1 and type(args[0]) == str:
            self._rules_clauses.append(edn_format.loads('['+args[0]+']'))
        else:
            self._rules_clauses.append(list(args))
        return self

    def order_by(self, *args):
        if self._values:
            raise AlreadySent()
        if len(args) == 0:
            raise ValueError('missing order_by arguments')
        if len(args) == 1 and type(args[0]) == str:
            self._order_by_clauses.append(edn_format.loads('['+args[0]+']'))
        else:
            self._order_by_clauses.append(list(args))
        return self

    def limit(self, limit):
        if self._values:
            raise AlreadySent()
        self._limit = limit
        return self

    def offset(self, offset):
        if self._values:
            raise AlreadySent()
        self._offset = offset
        return self

    def timeout(self, timeout):
        if self._values:
            raise AlreadySent()
        self._timeout = timeout
        return self

    def query(self):
        q = {
            Keyword('find'):  self._find_clause,
            Keyword('where'): self._where_clauses
        }
        if self._in_clause:
            q[Keyword('in')] = self._in_clause
        if self._rules_clauses:
            q[Keyword('rules')] = self._rules_clauses
        if self._order_by_clauses:
            q[Keyword('order-by')] = self._order_by_clauses
        if self._limit:
            q[Keyword('limit')] = self._limit
        if self._offset:
            q[Keyword('offset')] = self._offset
        if self._timeout:
            q[Keyword('timeout')] = self._timeout
        return q

    def values(self):
        if not self._find_clause:
            raise BadQuery("query has no find clause")
        if not self._where_clauses:
            raise BadQuery("query has no where clause")
        query = self.query()
        result = self.node.query(query)
        if type(result) is dict:
            self.error = json.dumps(result, indent=4, sort_keys=True)
            return []
        return result

    def __str__(self):
        return edn_format.dumps(self.query(), indent=4)

    def __iter__(self):
        self._values = self.values()
        return self
    
    def __next__(self):
        if len(self._values):
            return self._values.pop()
        else:
            raise StopIteration

class TxOps:

    def __init__(self, uri="http://localhost:3000", node=None):
        if node:
            self.node = node
        else:
            self.node = Node(uri)
        self._ops = []
        self._result = None

    def _put_single(self, rec, valid_time=None, end_valid_time=None):
        op = ['put', rec]
        if valid_time:
            op.append(valid_time)
        if valid_time and end_valid_time:
            op.append(end_valid_time)
        self._ops.append(op)

    def put(self, rec, valid_time=None, end_valid_time=None):
        if type(rec) is list:
            for item in rec:
                self._put_single(item, valid_time, end_valid_time)
        else:
            self._put_single(rec, valid_time, end_valid_time)
        return self

    def _delete_single(self, eid, valid_time=None, end_valid_time=None):
        op = ['delete', eid]
        if valid_time:
            op.append(valid_time)
        if valid_time and end_valid_time:
            op.append(end_valid_time)
        self._ops.append(op)

    def delete(self, eid, valid_time=None, end_valid_time=None):
        if type(eid) is list:
            for item in eid:
                self._delete_single(item, valid_time, end_valid_time)
        else:
            self._delete_single(eid, valid_time, end_valid_time)
        return self

    def evict(self, eid):
        if type(eid) is list:
            for item in eid:
                self._ops.append(['evict', item])
        else:
            self._ops.append(['evict', eid])
        return self

    def match(self, eid, rec, valid_time=None):
        op = ['match', eid, rec]
        if valid_time:
            op.append(valid_time)
        self._ops.append(op)
        return self

    def submit(self):
        if len(self._ops) == 0:
            return None
        if not self._result:
            self._result = self.node.submitTx(self)
        return self._result

    @property
    def ops(self):
        return {'tx-ops': self._ops}


class Node:

    def __init__(self, uri="http://localhost:3000"):
        self.uri = uri

    def find(self, find_clause):
        return Query(node=self).find(find_clause)

    @staticmethod
    def _parse_kwargs(known_args, provided_args):
        params = {}
        for k,v in provided_args.items():
            # convert from valid python keywords to url parameters,
            # i.e. from 'with_opsQ' to 'with-ops?'
            k = k.replace("_","-").replace('Q','?')
            if k not in known_args:
                raise UnknownParameter(f'Unknown parameter: {k}')
            if v is not None:
                # handle formatted values if they are edn or json,
                # and for now we depend on the convention that
                # the url parameters are named with the desired format.
                if k.endswith('-edn'):
                    v = edn_format.dumps(v)
                if k.endswith('-json'):
                    v = json.dumps(v)
                params[k] = v
        return params

    @staticmethod
    def _check_status(request):
        if 400 <= request.status_code < 500:
            raise XtdbError(
                f"Client Error: {request.reason} for url: {request.url}",
                request.status_code
            )
        elif 500 <= request.status_code < 600:
            raise XtdbError(
                f"Server Error: {request.reason} for url: {request.url}",
                request.status_code
            )

    def _call_rest_api(self, action, params={}):
        endpoint = "{}/_xtdb/{}".format(self.uri, action)
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }

        rsp = requests.get(endpoint, headers=headers, params=params)
        self._check_status(rsp)
        return rsp.json()

    def submitTx(self, txops):
        action = "submit-tx"
        endpoint = "{}/_xtdb/{}".format(self.uri, action)
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }
        rsp = requests.post(endpoint, headers=headers, json=txops.ops)
        self._check_status(rsp)
        return rsp.json()

    def put(self, rec, valid_time=None, end_valid_time=None):
        return TxOps(node=self).put(rec, valid_time, end_valid_time)

    def delete(self, eid, valid_time=None, end_valid_time=None):
        return TxOps(node=self).delete(eid, valid_time, end_valid_time)

    def evict(self, eid):
        return TxOps(node=self).evict(eid)

    def match(self, eid, rec, valid_time=None):
        return TxOps(node=self).match(eid, rec, valid_time)

    def query(self, query, in_args=None, **kwargs):

        if type(query) is str:
            try: # use edn_format to validate the string
                query = query.strip()
                _ = edn_format.loads(query)
            except edn_format.exceptions.EDNDecodeError as e:
                raise BadEDNString(str(e)) from e
        else:
            query = edn_format.dumps(query)

        if in_args is not None:
            if type(in_args) is str:
                try: # use edn_format to validate the string
                    in_args = in_args.strip()
                    _ = edn_format.loads(in_args)
                except edn_format.exceptions.EDNDecodeError as e:
                    raise BadEDNString(str(e)) from e
            else:
                in_args = edn_format.dumps(in_args)

        if not in_args:
            data = "{:query %s}" % query
        else:
            data = "{:query %s :in-args %s}" % (query, in_args)

        # handle url parameters
        known_args = [
            "valid-time",
            'tx-time',
            'tx-id'
        ]
        params = self._parse_kwargs(known_args, kwargs)

        action = "query"
        endpoint = "{}/_xtdb/{}".format(self.uri, action)
        headers = {"Accept": "application/json", "Content-Type": "application/edn"}
        rsp = requests.post(endpoint, headers=headers, data=data, params=params)
        self._check_status(rsp)
        return rsp.json()

    def status(self):
        action = "status"
        return self._call_rest_api(action)

    def entity(self, **kwargs):
        action = "entity"
        known_args = [
            'eid',
            'eid-json',
            'eid-edn',
            'valid-time',
            'tx-time',
            'tx-id'
        ]
        params = self._parse_kwargs(known_args, kwargs)
        return self._call_rest_api(action, params)

    def entityHistory(self, **kwargs):
        action = "entity?history=true"
        known_args = [
            'eid',
            'eid-json',
            'eid-edn',
            'sort-order',
            'with-corrections',
            'with-docs',
            'start-valid-time',
            'start-tx-time',
            'start-tx-id',
            'end-valid-time',
            'end-tx-time',
            'end-tx-id'
        ]
        params = self._parse_kwargs(known_args, kwargs)
        return self._call_rest_api(action, params)

    def entityTx(self, **kwargs):
        action = "entity-tx"
        known_args = [
            'eid',
            'eid-json',
            'eid-edn',
            'valid-time',
            'tx-time',
            'tx-id'
        ]
        params = self._parse_kwargs(known_args, kwargs)
        return self._call_rest_api(action, params)

    def attributeStats(self):
        action = "attribute-stats"
        return self._call_rest_api(action)

    def sync(self, **kwargs):
        action = "sync"
        known_args = ['timeout']
        params = self._parse_kwargs(known_args, kwargs)
        return self._call_rest_api(action, params)

    def awaitTx(self, **kwargs):
        action = "await-tx"
        known_args = ['tx-id', 'timeout']
        params = self._parse_kwargs(known_args, kwargs)
        return self._call_rest_api(action, params)

    def awaitTxTime(self, **kwargs):
        action = "await-tx-time"
        known_args = ['tx-time', 'timeout']
        params = self._parse_kwargs(known_args, kwargs)
        return self._call_rest_api(action, params)

    def txLog(self, **kwargs):
        action = "tx-log"
        known_args = ['after-tx-id', 'with-ops?']
        params = self._parse_kwargs(known_args, kwargs)
        return self._call_rest_api(action, params)

    def txCommitted(self, **kwargs):
        action = "tx-committed"
        known_args = ['tx-id']
        params = self._parse_kwargs(known_args, kwargs)
        return self._call_rest_api(action, params)

    def latestCompletedTx(self):
        action = "latest-completed-tx"
        return self._call_rest_api(action)

    def latestSubmittedTx(self):
        action = "latest-submitted-tx"
        return self._call_rest_api(action)

    def activeQueries(self):
        action = "active-queries"
        return self._call_rest_api(action)

    def recentQueries(self):
        action = "recent-queries"
        return self._call_rest_api(action)

    def slowestQueries(self):
        action = "slowest-queries"
        return self._call_rest_api(action)
