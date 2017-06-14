import os
import errno
import json
import gzip
import shutil
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import boto3
import botocore


def utc_today():
    return datetime.utcnow().date()


def get_days(days):
    return [utils.utc_today() - timedelta(1) - timedelta(i) for i in range(0, days)]


def get_with_retries(url, params=None, headers=None):
    retries = Retry(total=16, backoff_factor=1, status_forcelist=[429])

    s = requests.Session()
    s.mount('https://crash-stats.mozilla.com', HTTPAdapter(max_retries=retries))

    return s.get(url, params=params, headers=headers)


def mkdir(path):
    try:
        os.mkdir(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e


def rmdir(path):
    try:
        shutil.rmtree(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise e


def write_json(path, obj):
    with gzip.open(path, 'wb') as f:
        json.dump(obj, f)


def copytree(src, dst):
    if not os.path.exists(dst):
        os.makedirs(dst)

    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            copytree(s, d)
        else:
            shutil.copy2(s, d)


def remove_results(job_name):
    bucket = boto3.resource('s3').Bucket('telemetry-public-analysis-2')

    for key in bucket.objects.filter(Prefix=job_name + '/data/'):
        key.delete()
