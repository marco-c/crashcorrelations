# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams


rcParams.update({'figure.autolayout': True})


def rule_to_str(rule):
    return u' ∧ '.join([str(key) + '="' + str(value) + '"' for key, value in rule.items()])


def plot(results, label_a, label_b, outputFile=None):
    all_rules = sorted(results, key=lambda v: (-len(v['item']), round(v['support_diff'], 2), round(v['support_a'], 2)))

    # TODO: Print all rules once we have good heuristics to prune rules longer than 1.
    all_rules = [rule for rule in all_rules if len(rule['item']) == 1]

    values_a = [100 * rule['support_a'] for rule in all_rules]
    values_b = [100 * rule['support_b'] for rule in all_rules]

    fig, ax = plt.subplots(figsize=(24, 18))
    index = np.arange(len(all_rules))
    bar_width = 0.35

    plt.barh(index, values_a, bar_width, color='b', label=label_a)
    plt.barh(index + bar_width, values_b, bar_width, color='r', label=label_b)

    plt.xlabel('Support')
    plt.ylabel('Rule')
    plt.title('Most interesting deviations')
    plt.yticks(index + bar_width, [rule_to_str(rule['item']) for rule in all_rules])
    plt.legend()

    if outputFile is not None:
        plt.savefig(outputFile)
    plt.show()
