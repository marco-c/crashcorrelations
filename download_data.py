# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import json
from urlparse import urlparse
from datetime import timedelta

import boto3
import botocore
import dateutil.parser

import utils
import config
import versions


SCHEMA_VERSION = '10'
AMAZON_DIR = 'marco'


__token = ''
def set_token(token):
    global __token
    __token = token


__is_amazon = None
def is_amazon():
    global __is_amazon

    if __is_amazon is None:
        try:
            boto3.client('s3').head_bucket(Bucket='net-mozaws-prod-us-west-2-pipeline-analysis')
            __is_amazon = True
        except:
            __is_amazon = False

    return __is_amazon


def clean_old_data():
    try:
        old_schema = read_json('crashcorrelations_data/schema_version')[0]
    except IOError:
        old_schema = '0'

    MAX_AGE = 30

    if is_amazon():
        bucket = boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis')

        for key in bucket.objects.filter(Prefix=AMAZON_DIR + '/crashcorrelations_data'):
            if 'schema_version' not in key.key and (old_schema != SCHEMA_VERSION or dateutil.parser.parse(key.key[-15:-5]).date() < utils.utc_today() - timedelta(MAX_AGE)):
                key.delete()
    else:
        for root, dirs, files in os.walk('crashcorrelations_data'):
            for name in files:
                if 'schema_version' not in name and (old_schema != SCHEMA_VERSION or dateutil.parser.parse(name[-15:-5]).date() < utils.utc_today() - timedelta(MAX_AGE)):
                    os.remove(os.path.join('crashcorrelations_data', name))


def download(path):
    try:
        boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis').download_file(AMAZON_DIR + '/' + path, path)
    except:
        pass


def upload(path):
    boto3.resource('s3').Bucket('net-mozaws-prod-us-west-2-pipeline-analysis').upload_file(path, AMAZON_DIR + '/' + path)


def exists(path):
    if is_amazon():
        try:
            prefix = 's3://net-mozaws-prod-us-west-2-pipeline-analysis/' + AMAZON_DIR + '/'
            if prefix in path:
                path = path[len(prefix):]
            boto3.resource('s3').Object('net-mozaws-prod-us-west-2-pipeline-analysis', AMAZON_DIR + '/' + path).load()
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
        return 's3://net-mozaws-prod-us-west-2-pipeline-analysis/' + AMAZON_DIR + '/' + path
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
            'date': ['>=' + str(day), '<' + str(day + timedelta(1))],
            'version': version,
            '_columns': [
                'abort_message',
                'accessibility',
                'adapter_device_id',
                'adapter_driver_version',
                'adapter_subsys_id',
                'adapter_vendor_id',
                'addons',
                'address',
                'app_init_dlls',
                'app_notes', # We have some stuff in the app_notes that we can't get from SuperSearch (e.g. if there are two GPUs)
                'available_virtual_memory',
                'available_physical_memory',
                'bios_manufacturer',
                'build_id',
                'contains_memory_report',
                'cpu_arch',
                'cpu_info',
                'cpu_microcode_version',
                'date',
                'dom_ipc_enabled',
                'e10s_cohort',
                'gmp_plugin',
                'graphics_critical_error',
                'graphics_startup_test',
                'ipc_channel_error',
                'ipc_fatal_error_msg',
                'ipc_fatal_error_protocol',
                'ipc_message_name',
                'ipc_system_error',
                'is_garbage_collecting',
                'jit_category',
                'moz_crash_reason',
                'oom_allocation_size',
                'platform',
                'platform_pretty_version',
                'platform_version',
                'plugin_version',
                'process_type',
                'reason',
                'safe_mode',
                'shutdown_progress',
                'signature',
                'submitted_from_infobar',
                'theme',
                'total_physical_memory',
                'total_virtual_memory',
                'url',
                'useragent_locale',
                'user_comments',
                'uuid',
            ],
            '_results_number': RESULTS_NUMBER,
            '_results_offset': len(crashes),
            '_facets_size': 0,
        }

        if not version.startswith('50'):
            params['_columns'].append('startup_crash')

        url = 'https://crash-stats.mozilla.com/api/SuperSearch'
        headers = {}
        token = config.get('Socorro', 'token', __token)
        if token:
            url += 'Unredacted'
            headers['Auth-Token'] = token

        print(str(version) + ' - ' + str(day) + ' - ' + str(len(crashes)))
        r = utils.get_with_retries(url, params=params, headers=headers)

        if r.status_code != 200:
            print(r.text)
            raise Exception(r)

        found = r.json()['hits']

        if token:
            # Remove the URLs now, we don't want to store them locally!
            for crash in found:
                if not crash['url']:
                    continue

                if crash['url'].startswith('ed2k'):
                    crash['url'] = 'ed2k'
                else:
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

    uuids = set()
    filtered_crashes = []
    for crash in crashes:
        if crash['uuid'] not in uuids:
            uuids.add(crash['uuid'])
            filtered_crashes.append(crash)

    write_json(path, filtered_crashes)


def download_crashes(versions, days, product='Firefox'):
    global SCHEMA_VERSION

    if config.get('Socorro', 'token', __token) and not SCHEMA_VERSION.endswith('-with-token'):
        SCHEMA_VERSION += '-with-token'

    if not os.path.exists('crashcorrelations_data'):
        os.mkdir('crashcorrelations_data')

    clean_old_data()
    write_json('crashcorrelations_data/schema_version', [SCHEMA_VERSION])

    for i in range(0, days):
        for version in versions:
            download_day_crashes(version, utils.utc_today() - timedelta(1) - timedelta(i), product)


def get_days(days):
    return [utils.utc_today() - timedelta(1) - timedelta(i) for i in range(0, days)]


def get_paths(versions, days, product='Firefox'):
    return [get_path(version, day, product) for day in get_days(days) for version in versions]


def get_top(number, versions, days, product='Firefox'):
    url = 'https://crash-stats.mozilla.com/api/SuperSearch'

    params = {
        'product': product,
        'date': ['>=' + str(utils.utc_today() - timedelta(days) + timedelta(1))],
        'version': versions,
        '_results_number': 0,
        '_facets_size': number,
    }

    r = utils.get_with_retries(url, params=params)

    if r.status_code != 200:
        print(r.text)
        raise Exception(r)

    return [signature['term'] for signature in r.json()['facets']['signature']]


def get_versions(channel, product='Firefox'):
    channel = channel.lower()
    version = str(versions.get(base=True)[channel])

    r = utils.get_with_retries('https://crash-stats.mozilla.com/api/ProductVersions', params={
        'product': product,
        'active': True,
        'is_rapid_beta': False,
    })

    if r.status_code != 200:
        print(r.text)
        raise Exception(r)

    return [result['version'] for result in r.json()['hits'] if result['version'].startswith(version) and result['build_type'] == channel]
