import os
import time
import json
import urllib
import gzip
import errno
import argparse

import plot


def generate_plots(output_dir):
    try:
        os.mkdir(output_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e

    results_file = os.path.join(output_dir, 'top50_results.json.gz')
    results_file_backup = results_file + '.old'

    # Download the data file if it doesn't exist or if it is one day old.
    if not os.path.exists(results_file) or os.path.getmtime(results_file) + 24 * 60 * 60 < time.time():
        # Use the preexisting file as a backup.
        try:
            os.rename(results_file, results_file_backup)
        except OSError:
            pass

        try:
            urllib.URLopener().retrieve('https://analysis-output.telemetry.mozilla.org/top-50-signatures-correlations/data/top50_results.json.gz', results_file)
        except Exception as e:
            # Use the backup copy if there was an error during the download.
            try:
                os.rename(results_file_backup, results_file)
            except OSError:
                pass

        # Remove the backup copy.
        try:
            os.remove(results_file_backup)
        except OSError:
            pass

    with gzip.open(results_file, 'rb') as f:
        data = json.load(f)

    for channel, results in data.items():
        try:
            os.mkdir(os.path.join(output_dir, channel))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e

        for signature, rules in results.items():
            plot.plot(rules, 'Overall', signature, os.path.join(output_dir, channel, signature + '.png'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Top-50 Plot Generator')
    parser.add_argument('-o', '--output-directory', action='store', required=True, help='the output directory')

    args = parser.parse_args()

    generate_plots(args.output_directory)
