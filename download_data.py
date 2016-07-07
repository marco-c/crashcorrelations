# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import json
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
from datetime import (date, timedelta)
import requests

import config


LAST_DAY = date.today() # XXX: Should be the last day to avoid issues ("date can't be in the future") in Socorro.
# LAST_DAY = date.today() - timedelta(6)


def file_path(day, version):
    return 'data/' + version + '-crashes-' + str(day) + '.json'


def download_day_crashes(day, version):
    crashes = []

    path = file_path(day, version)

    try:
        with open(path, 'r') as f:
            crashes += json.load(f)
    except IOError:
        pass

    finished = True

    RESULTS_NUMBER = 1000

    while not finished:
        date_param = ['>=' + str(day)]
        if day != LAST_DAY:
            date_param.append('<' + str(day + timedelta(1)))

        params = {
            'product': 'Firefox',
            'date': date_param,
            'version': version,
            '_columns': [
                'signature',
                'build_id',
                'platform',
                'platform_pretty_version',
                'adapter_vendor_id',
                'adapter_device_id',
                'adapter_driver_version',
                'plugin_version',
                'url',
                'available_virtual_memory',
                'available_physical_memory',
                'total_virtual_memory',
                'total_physical_memory',
                'oom_allocation_size',
                'uptime',
                'number_of_processors',
                'jit_category',
                'is_garbage_collecting',
                'dom_ipc_enabled',
                'cpu_arch',
                'cpu_name',
                'cpu_info',
                'bios_manufacturer',
                'app_notes', # We have some stuff in the app_notes that we can't get from SuperSearch (e.g. if there are two GPUs)
                'addons',
            ],
            '_results_number': RESULTS_NUMBER,
            '_results_offset': len(crashes),
        }

        url = 'https://crash-stats.mozilla.com/api/SuperSearchUnredacted'
        headers = {
          'Auth-Token': config.get('Socorro', 'token', '')
        }

        print(params)
        r = requests.get(url, params=params, headers=headers)

        if r.status_code != 200:
            print(r.text)
            raise Exception('Unable to download crash data: ' + r.url)

        found = r.json()['hits']

        # Remove the URLs now, we don't want to store them locally!
        for crash in found:
            if not crash['url']:
                continue

            o = urlparse(crash['url'])
            if o.scheme == 'about':
                pass
            elif o.scheme != 'http' and o.scheme != 'https':
                crash['url'] = o.scheme
            else:
                crash['url'] = o.netloc

        crashes += found

        if len(found) < RESULTS_NUMBER:
            finished = True

        if len(found) != 0:
            with open(path, 'w') as f:
                json.dump(crashes, f)

    return path


def download_crashes(version, days):
    if not os.path.exists('data'):
        os.mkdir('data')

    paths = []

    for i in range(0, days):
        paths.append(download_day_crashes(LAST_DAY - timedelta(i), version))

    return paths


def get_crashes(sqlContext, version, days):
    return sqlContext.read.format('json').load(download_crashes(version, days))
