# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse

from pyspark import SparkContext
from pyspark.sql import SQLContext

import download_data
import crash_deviations

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Signature Correlations')
    parser.add_argument('signature', action='store', help='The signature you\'re interested in.')

    args = parser.parse_args()

    sc = SparkContext(appName='CrashCorrelations')
    sqlContext = SQLContext(sc)

    df_a = download_data.get_crashes(sqlContext, version='47.0', days=2)
    df_b = df_a.filter(df_a['signature'] == args.signature)

    crash_deviations.find_deviations(sc, df_a, df_b, min_support_diff=0.15, min_corr=0.03, max_addons=50)
