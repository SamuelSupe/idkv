#!/usr/bin/env python3
"""Replay the reported HTTP failures against preserved before/after binaries."""
import argparse
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from qualification import Lab
from recovery_drill import digest, save


def run(binary, root):
    lab = Lab(root, binary, replicated=False)
    lab.options['max_snapshots'] = 8
    save(root / 'options.json', lab.options)
    def request(path, body=None):
        req = urllib.request.Request(lab.urls[0] + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization': 'Bearer ' + lab.token, 'Content-Type': 'application/json'})
        try:
            response = urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return {'status': response.status, 'history': response.headers.get('x-tkv-history'),
                    'body': json.load(response)}
    result = {'binary_sha256': digest(binary), 'headers': []}
    try:
        lab.start(0)
        tenant = lab.call(0, '/v1/tenants', {'name': 'reported-http-regressions'})['id']
        lab.call(0, f'/v1/tenants/{tenant}/collections', {'name': 'docs'})
        base = f'/v1/tenants/{tenant}/collections/docs'
        result['internal_history'] = lab.call(0, f'/v1/tenants/{tenant}/context')['history']
        for key, history in [('forged', 'forged-token'), ('invalid', '历史\n')]:
            lab.call(0, base + '/documents', {'document': {'_key': key, 'history': history}}, method='PUT')
            for path, body in [(base + '/documents/' + key, None), (base + '/multi_get', {'keys': [key]})]:
                result['headers'].append({'case': key, 'path': path.rsplit('/', 1)[-1], **request(path, body)})
        retained = 0
        for _ in range(20):
            page = request(base + '/query', {'limit': 1})
            if page['status'] == 200:
                assert page['body']['cursor']
                retained += 1
            else:
                assert page['status'] == 429, page
                time.sleep(.02)
        result['retained_cursors'] = retained
        result['get_after_cursor_saturation'] = request(base + '/documents/forged')
        return result
    finally:
        lab.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before', type=Path, required=True)
    parser.add_argument('--after', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=False)
    result = {label: run(binary.resolve(strict=True), args.root / label)
              for label, binary in [('before', args.before), ('after', args.after)]}
    save(args.root / 'result.json', result)
    before, after = result['before'], result['after']
    assert all(r['status'] == 500 for r in before['headers'] if r['case'] == 'invalid'), before
    assert all(r['history'] == 'forged-token' for r in before['headers'] if r['case'] == 'forged'), before
    assert before['retained_cursors'] == 8, before
    assert before['get_after_cursor_saturation']['status'] == 429, before
    assert all(r['status'] == 200 and r['history'] == after['internal_history'] for r in after['headers']), after
    assert after['retained_cursors'] == 2, after
    assert after['get_after_cursor_saturation']['status'] == 200, after
    print(json.dumps({'result': 'passed', **result}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
