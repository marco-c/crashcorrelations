# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams

rcParams.update({'figure.autolayout': True})


def rule_to_str(rule):
    return u' ∧ '.join([str(key) + '="' + str(value) + '"' for key, value in rule])


def plot(results):
    all_rules = sorted(results, key=lambda v: (-len(v[0]), round(v[1], 2), round(v[2], 2)))

    # TODO: Print all rules once we have good heuristics to prune rules longer than 1.
    all_rules = [(item, support_diff, support_b, support_a) for item, support_diff, support_b, support_a in all_rules if len(item) == 1]

    values_a = [100 * support_a for item, support_diff, support_b, support_a in all_rules]
    values_b = [100 * support_b for item, support_diff, support_b, support_a in all_rules]

    fig, ax = plt.subplots(figsize=(24, 18))
    index = np.arange(len(all_rules))
    bar_width = 0.35

    plt.barh(index, values_a, bar_width, color='b', label='Group A')
    plt.barh(index + bar_width, values_b, bar_width, color='r', label='Group B')

    plt.xlabel('Support')
    plt.ylabel('Rule')
    plt.title('Most interesting deviations')
    plt.yticks(index + bar_width, [rule_to_str(item) for item, supportDiff, mySupport, overallSupport in all_rules])
    plt.legend()

    plt.savefig('perf.png')
    plt.show()
