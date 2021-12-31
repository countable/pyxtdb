import requests


class Node:
    def __init__(self, uri="http://localhost:3000"):
        self.uri = uri

    def call_rest_api(self, action, params):

        endpoint = "{}/_xtdb/{}".format(self.uri, action)

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }

        rsp = requests.get(endpoint, headers=headers, params=params)
        return rsp.json()

    def submitTx(self, tx):
        action = "submit-tx"
        endpoint = "{}/_xtdb/{}".format(self.uri, action)
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }
        rsp = requests.post(endpoint, headers=headers, json=tx)
        return rsp.json()

    def put(self, rec):
        transaction = {"tx-ops": [["put", rec]]}
        return self.submitTx(transaction)

    def delete(self, where):
        transaction = {"tx-ops": [["delete", where]]}
        return self.submitTx(transaction)

    def evict(self, id):
        transaction = {"tx-ops": [["evict", id]]}
        return self.submitTx(transaction)

    def query(self, query):
        # basic syntax check
        assert query.startswith("{") and query.endswith("}")

        action = "query"
        endpoint = "{}/_xtdb/{}".format(self.uri, action)
        headers = {"Accept": "application/json", "Content-Type": "application/edn"}
        query = "{:query %s}" % query
        rsp = requests.post(endpoint, headers=headers, data=query)
        return rsp.json()

    # -- TODO: explicit optional params for these, as named Python kwargs.

    def status(self, params):
        action = "status"
        return self.call_rest_api(action, params)

    def entity(self, params):
        action = "entity"
        return self.call_rest_api(action, params)

    def entityHistoryTrue(self, params):
        action = "entity?history=true"
        return self.call_rest_api(action, params)

    def entityTx(self, params):
        action = "entity-tx"
        return self.call_rest_api(action, params)

    def attributeStats(self, params):
        action = "attribute-stats"
        return self.call_rest_api(action, params)

    def sync(self, params):
        action = "sync"
        return self.call_rest_api(action, params)

    def awaitTx(self, params):
        action = "await-tx"
        return self.call_rest_api(action, params)

    def awaitTxTime(self, params):
        action = "await-tx-time"
        return self.call_rest_api(action, params)

    def txLog(self, params):
        action = "tx-log"
        return self.call_rest_api(action, params)

    def txCommitted(self, params):
        action = "tx-committed"
        return self.call_rest_api(action, params)

    def latestCompletedTx(self, params):
        action = "latest-completed-tx"
        return self.call_rest_api(action, params)

    def latestSubmittedTx(self, params):
        action = "latest-submitted-tx"
        return self.call_rest_api(action, params)

    def activeQueries(self, params):
        action = "active-queries"
        return self.call_rest_api(action, params)

    def recentQueries(self, params):
        action = "recent-queries"
        return self.call_rest_api(action, params)

    def slowestQueries(self, params):
        action = "slowest-queries"
        return self.call_rest_api(action, params)
