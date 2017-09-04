# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import re

import utils


def query_dxr(q):
    r = utils.get_with_retries('https://dxr.mozilla.org/mozilla-central/search', params={
        'q': q,
        'limit': 1000
    }, headers={
        'Accept': 'application/json'
    })

    if r.status_code != 200:
        print(r.text)
        raise Exception(r)

    return r.json()


def get_app_notes():
    results = query_dxr('ScopedGfxFeatureReporter ')['results']

    matches = [re.search(r'"(.*?)"', line['line']) for result in results for line in result['lines']]

    errors = [match.group(1) for match in matches if match is not None]

    # Remove duplicates and remove wrongly reported string.
    errors = set([error for error in errors if error != 'gfxCrashReporterUtils.h'])

    errors = sum([[error + '?', error + '-', error + '+'] for error in errors], [])

    return errors
