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
            raise BadQuery("No Where Clause")
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
