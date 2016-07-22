# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse

from pyspark import SparkContext

import download_data
import crash_deviations
import plot

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Signature Correlations')
    parser.add_argument('signature', action='store', help='The signature you\'re interested in.')
    parser.add_argument('-p', '--product', action='store', default='Firefox', help='The product you\'re interested in.')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-v', '--versions', action='store', nargs='+', help='The versions you\'re interested in.')
    group.add_argument('-c', '--channel', action='store', help='The channel you\'re interested in.')

    args = parser.parse_args()

    if args.channel:
        versions = download_data.get_versions(args.channel)
    else:
        versions = args.versions

    sc = SparkContext(appName='CrashCorrelations')

    download_data.download_crashes(versions=versions, days=5, product=args.product)

    df_a = crash_deviations.get_crashes(sc, versions=versions, days=5, product=args.product)
    df_b = df_a.filter(df_a['signature'] == args.signature)

    results = crash_deviations.find_deviations(sc, df_a, df_b, min_support_diff=0.15, min_corr=0.03, max_addons=50)

    plot.plot(results, 'Overall', args.signature)
