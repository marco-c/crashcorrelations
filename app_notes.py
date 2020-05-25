# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import re

from . import utils


app_notes = None
def get_app_notes():
    global app_notes

    if app_notes is None:
        results = utils.query_searchfox('ScopedGfxFeatureReporter ')

        matches = [re.search(r'"(.*?)"', line['line']) for result in results for line in result['lines']]

        app_notes = [match.group(1) for match in matches if match is not None]

        # Remove duplicates and remove wrongly reported string.
        app_notes = set([app_note for app_note in app_notes if app_note != 'gfxCrashReporterUtils.h'])

        app_notes = sum([[app_note + '?', app_note + '-', app_note + '+'] for app_note in app_notes], [])

    return app_notes
