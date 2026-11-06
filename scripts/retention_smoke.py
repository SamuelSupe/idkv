#!/usr/bin/env python3
"""Exercise unified retention against a running server using a disposable tenant."""
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

base, token_file = sys.argv[1:3]
token = Path(token_file).read_text().strip()
def call(method, path, data=None):
    payload = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(base.rstrip('/') + path, data=payload, method=method,
                                 headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)

def ok(method, path, data=None):
    code, body = call(method, path, data)
    assert code == 200, (method, path, code, body)
    return body

tenant = ok('POST', '/v1/tenants', {'name': 'retention-' + uuid.uuid4().hex})['id']
prefix = '/v1/tenants/' + tenant
try:
    ok('PUT', prefix + '/retention', {'retention_seconds': 1})
    ok('POST', prefix + '/collections', {'name': 'docs'})
    context = ok('POST', prefix + '/retry-context', {})
    options = {'operation_id': 'once', 'expected_history': context['history'], 'retry_token': context['retry_token']}
    body = {'document': {'value': 'current'}, 'write_options': options}
    path = prefix + '/collections/docs/documents'
    first = ok('PUT', path, body)
    assert ok('PUT', path, body) == first
    time.sleep(1.1)
    code, error = call('PUT', path, body)
    assert code == 410 and error['error']['code'] == 'IDEMPOTENCY_EXPIRED', error
    assert ok('GET', path + '/' + first['key'])['value'] == 'current'
    previous = None
    versions = []
    for number in range(2):
        name = 'assets_' + str(number)
        collection = prefix + '/collections/' + name
        ok('POST', prefix + '/collections', {'name': name})
        ok('PUT', collection + '/documents', {'document': {'_key': 'k', 'ip': 'a', 'owner': number}})
        ok('POST', collection + '/indexes', {'name': 'ip', 'fields': [{'field': '/ip'}], 'unique': True, 'include': ['/owner']})
        ok('POST', collection + '/lookups', {'name': 'owner', 'index': 'ip', 'select': ['/owner']})
        context = ok('POST', prefix + '/retry-context', {})
        publication = {'version': str(uuid.uuid4()), 'collection': name, 'lookup': 'owner',
                       'expected_version': previous, 'retry_token': context['retry_token']}
        published = ok('PUT', prefix + '/resources/assets', publication)
        assert published['version'] != publication['version']
        assert ok('PUT', prefix + '/resources/assets', publication) == published
        previous = published['version']
        versions.append(previous)
    time.sleep(1.1)
    code, error = call('POST', prefix + '/resources/assets/execute', {'version': versions[0], 'inputs': [{'ip': 'a'}]})
    assert code == 410 and error['error']['code'] == 'VERSION_UNAVAILABLE', error
    assert ok('POST', prefix + '/resources/assets/execute', {'version': versions[1], 'inputs': [{'ip': 'a'}]})['version'] == versions[1]
    deadline = time.monotonic() + 10
    while any(c['name'] == 'assets_0' for c in ok('GET', prefix + '/collections')):
        assert time.monotonic() < deadline, 'expired collection was not reclaimed'
        time.sleep(.05)
    status = ok('GET', prefix + '/retention')
    print(json.dumps({'generated_write_not_repeated': True, 'current_document_retained': True,
                      'current_publication_retained': True, 'old_publication_reclaimed': True, 'retention': status}))
finally:
    deadline = time.monotonic() + 5
    while True:
        code, error = call('DELETE', prefix)
        if code == 200:
            break
        assert code == 409 and time.monotonic() < deadline, error
        time.sleep(.05)
