# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse

from pyspark import SparkContext

import download_data
import crash_deviations

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Signature Correlations')
    parser.add_argument('signature', action='store', help='The signature you\'re interested in.')
    parser.add_argument('version', action='store', help='The version you\'re interested in.')

    args = parser.parse_args()

    sc = SparkContext(appName='CrashCorrelations')

    df_a = download_data.get_crashes(sc, version=args.version, days=2)
    df_b = df_a.filter(df_a['signature'] == args.signature)

    crash_deviations.find_deviations(sc, df_a, df_b, min_support_diff=0.15, min_corr=0.03, max_addons=50)
