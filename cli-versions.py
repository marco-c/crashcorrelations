# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse

from pyspark import SparkContext

import download_data
import crash_deviations
import plot

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Versions Correlations')
    parser.add_argument('reference_version', action='store', help='The reference version.')
    parser.add_argument('version', action='store', help='The version you\'re interested in.')

    args = parser.parse_args()

    sc = SparkContext(appName='CrashCorrelations')

    download_data.download_crashes(args.version, 2)

    df_a = crash_deviations.get_crashes(sc, version=args.reference_version, days=2)
    df_b = crash_deviations.get_crashes(sc, version=args.version, days=2)

    results = crash_deviations.find_deviations(sc, df_a, df_b, min_support_diff=0.03, min_corr=0.06, max_addons=50)

    plot.plot(results, args.reference_version, args.version)
