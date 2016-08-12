# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import json
from urlparse import urlparse
from datetime import datetime, timedelta
import requests
import shutil

import boto3
import botocore
import dateutil.parser

import config
import versions


def utc_today():
    return datetime.utcnow().date()


__token = ''
def set_token(token):
    global __token
    __token = token


__is_amazon = None
def is_amazon():
    global __is_amazon

    if __is_amazon is None:
        try:
            requests.get('http://169.254.169.254/latest/meta-data/ami-id', timeout=10)
            __is_amazon = True
        except:
            __is_amazon = False

    return __is_amazon


def delete(path):
    if is_amazon():
        bucket = boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis')

        for key in bucket.objects.filter(Prefix='marco/' + path):
            key.delete()
    else:
        shutil.rmtree(path)


def clean_old_data():
    MAX_AGE = 30

    if is_amazon():
        bucket = boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis')

        for key in bucket.objects.filter(Prefix='marco/crashcorrelations_data'):
            if dateutil.parser.parse(key.key[-15:-5]).date() < utc_today() - timedelta(MAX_AGE):
                key.delete()
    else:
        for root, dirs, files in os.walk('crashcorrelations_data'):
            for name in files:
                if dateutil.parser.parse(name[-15:-5]).date() < utc_today() - timedelta(MAX_AGE):
                    os.remove(os.path.join('crashcorrelations_data', name))


def download(path):
    try:
        boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis').download_file('marco/' + path, path)
    except:
        pass


def upload(path):
    boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis').upload_file(path, 'marco/' + path)


def exists(path):
    if is_amazon():
        try:
            boto3.resource('s3').Object('net-mozaws-prod-us-west-2-pipeline-analysis', 'marco/' + path).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e
        else:
            return True
    else:
        return os.path.isfile(path)


def file_path(version, day, product):
    return 'crashcorrelations_data/' + product.lower() + '-' + version + '-crashes-' + str(day) + '.json'


def get_path(version, day, product):
    path = file_path(version, day, product)

    if is_amazon():
        return 's3://net-mozaws-prod-us-west-2-pipeline-analysis/marco/' + path
    else:
        return path


def read_json(path):
    if is_amazon():
        download(path)

    data = []

    with open(path, 'r') as f:
        for line in f:
            data.append(json.loads(line))

    return data


def write_json(path, data):
    with open(path, 'w') as f:
        for elem in data:
            f.write(json.dumps(elem) + '\n')

    if is_amazon():
        upload(path)


def download_day_crashes(version, day, product='Firefox'):
    crashes = []

    path = file_path(version, day, product)

    try:
        crashes += read_json(path)
    except IOError:
        pass

    finished = False

    RESULTS_NUMBER = 1000

    while not finished:
        params = {
            'product': product,
            'date': ['>=' + str(day), '<' + str(day + timedelta(1))] if day != utc_today() else '>=' + str(day),
            'version': version,
            '_columns': [
                'signature',
                'build_id',
                'address',
                'platform',
                'platform_version',
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
                'contains_memory_report',
                'moz_crash_reason',
            ],
            '_results_number': RESULTS_NUMBER,
            '_results_offset': len(crashes),
        }

        url = 'https://crash-stats.mozilla.com/api/SuperSearchUnredacted'
        headers = {
          'Auth-Token': config.get('Socorro', 'token', __token),
        }

        print(params)
        r = requests.get(url, params=params, headers=headers)

        if r.status_code != 200:
            print(r.text)
            raise Exception(r)

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

    write_json(path, crashes)


def download_crashes(versions, days, product='Firefox'):
    if not os.path.exists('crashcorrelations_data'):
        os.mkdir('crashcorrelations_data')

    clean_old_data()

    for i in range(0, days):
        for version in versions:
            download_day_crashes(version, utc_today() - timedelta(i), product)


def get_paths(versions, days, product='Firefox'):
    last_day = utc_today()
    path = get_path(versions[0], last_day, product)
    if not exists(path):
        last_day -= timedelta(1)

    return [get_path(version, last_day - timedelta(i), product) for i in range(0, days) for version in versions]


def get_top(number, versions, days, product='Firefox'):
    url = 'https://crash-stats.mozilla.com/api/SuperSearch'

    params = {
        'product': product,
        'date': ['>=' + str(utc_today() - timedelta(days)), '<' + str(utc_today())],
        'version': versions,
        '_results_number': 0,
        '_facets_size': number,
    }

    r = requests.get(url, params=params)

    if r.status_code != 200:
        print(r.text)
        raise Exception(r)

    return [signature['term'] for signature in r.json()['facets']['signature']]


def get_versions(channel):
    channel = channel.lower()
    version = str(versions.get(base=True)[channel])

    r = requests.get('https://crash-stats.mozilla.com/api/ProductVersions', params={
        'product': 'Firefox',
        'active': True,
        'is_rapid_beta': False,
    })

    return [result['version'] for result in r.json()['hits'] if result['version'].startswith(version)]
