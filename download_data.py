# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import timedelta

import utils
import versions


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

    if channel == 'nightly':
        return ['{}a1'.format(version)]
    elif channel == 'release':
        return ['{}'.format(version)] + ['{}{}'.format(version, i) for i in range(1, 7)]
    elif channel == 'beta':
        return ['{}.0b{}'.format(version, i) for i in range(0, 99)]
    else:
        assert False, 'Unknown channel {}'.format(channel)

    # TODO: Switch to buildhub to get the good old behavior back.
    # r = utils.get_with_retries('https://crash-stats.mozilla.com/api/ProductVersions', params={
    #     'product': product,
    #     'active': True,
    #     'is_rapid_beta': False,
    # })

    # if r.status_code != 200:
    #     print(r.text)
    #     raise Exception(r)

    # return [result['version'] for result in r.json()['hits'] if result['version'].startswith(version) and result['build_type'] == channel]
