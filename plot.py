# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import matplotlib.pyplot as plt


def rule_to_str(rule):
    return u' ∧ '.join([str(key) + '="' + str(value) + '"' for key, value in rule.items()])


def plot(results, label_a, label_b, outputFile=None):
    all_rules = sorted(results, key=lambda v: (-len(v['item']), round(abs(v['support_a'] - v['support_b']), 2), round(v['support_a'], 2)))

    values_a = [100 * rule['support_a'] for rule in all_rules]
    values_b = [100 * rule['support_b'] for rule in all_rules]

    plt.rc('figure', autolayout=True)
    plt.rc('font', size=22)

    fig, ax = plt.subplots(figsize=(24, 18))
    index = range(len(all_rules))
    bar_width = 0.35

    plt.barh(index, values_a, bar_width, color='b', label=label_a)
    plt.barh([i + bar_width for i in index], values_b, bar_width, color='r', label=label_b)

    plt.xlabel('Support')
    plt.ylabel('Rule')
    plt.title('Most interesting deviations')
    plt.yticks([i + bar_width for i in index], [rule_to_str(rule['item']) for rule in all_rules])
    if len(all_rules) > 0:
        plt.legend()

    if outputFile is not None:
        plt.savefig(outputFile)
    else:
        plt.show()
    plt.close(fig)
