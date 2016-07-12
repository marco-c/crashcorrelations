# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import json
from urlparse import urlparse
from datetime import (date, timedelta)
import requests
import shutil

import boto3
from pyspark.sql import SQLContext

import config


__token = ''
def set_token(token):
    global __token
    __token = token


__is_amazon = None
def is_amazon():
    global __is_amazon

    if __is_amazon is None:
        try:
            requests.get('http://169.254.169.254/latest/meta-data/ami-id')
            __is_amazon = True
        except:
            __is_amazon = False

    return __is_amazon


def delete(path):
    if is_amazon():
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('net-mozaws-prod-us-west-2-pipeline-analysis')

        for key in bucket.objects.filter(Prefix='marco/' + path):
            key.delete()
    else:
        shutil.rmtree(path)


def download(path):
    try:
        boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis').download_file('marco/' + path, path)
    except:
        pass


def upload(path):
    boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis').upload_file(path, 'marco/' + path)


def file_path(version, day):
    return 'crashcorrelations_data/' + version + '-crashes-' + str(day) + '.json'


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
        return 's3://net-mozaws-prod-us-west-2-pipeline-analysis/marco/' + path
    else:
        return path


def download_day_crashes(version, day):
    crashes = []

    path = file_path(version, day)

    try:
        crashes += read_json(path)
    except IOError:
        pass

    finished = False

    RESULTS_NUMBER = 1000

    date_param = ['>=' + str(day), '<' + str(day + timedelta(1))]

    while not finished:
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
          'Auth-Token': config.get('Socorro', 'token', __token),
        }

        print(params)
        r = requests.get(url, params=params, headers=headers)

        if r.status_code != 200:
            try:
                error = r.json()['error']
                if error == 'date can\'t be in the future':
                    date_param = ['>=' + str(day)]
                    continue
                else:
                    raise Exception()
            except:
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

    return write_json(path, crashes)


def download_crashes(version, days):
    if not os.path.exists('crashcorrelations_data'):
        os.mkdir('crashcorrelations_data')

    paths = []

    for i in range(0, days):
        paths.append(download_day_crashes(version, date.today() - timedelta(i)))

    return paths


def get_crashes(sc, version, days):
    sqlContext = SQLContext(sc)
    return sqlContext.read.format('json').load(download_crashes(version, days))
