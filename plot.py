# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import matplotlib.pyplot as plt


def rule_to_str(rule):
    return u' ∧ '.join([unicode(key) + unicode('="') + unicode(str(value)) + unicode('"') for key, value in rule.items()])


def plot(results, total_a, total_b, label_a, label_b, outputFile=None):
    all_rules = sorted(results, key=lambda v: (-len(v['item']), round(abs(v['count_a'] / total_a - v['count_b'] / total_b), 2), round(v['count_a'] / total_a, 2)))

    values_a = [100 * rule['count_a'] / total_a for rule in all_rules]
    values_b = [100 * rule['count_b'] / total_b for rule in all_rules]

    plt.rc('figure', autolayout=True)
    plt.rc('font', size=22)

    fig, ax = plt.subplots(figsize=(24, 18))
    index = range(len(all_rules))
    bar_width = 0.35

    if label_a.startswith('_'):
        label_a = ' ' + label_a
    if label_b.startswith('_'):
        label_b = ' ' + label_b

    bar_a = plt.barh(index, values_a, bar_width, color='b', label=label_a)
    bar_b = plt.barh([i + bar_width for i in index], values_b, bar_width, color='r', label=label_b)

    plt.xlabel('Support')
    plt.ylabel('Rule')
    plt.title('Most interesting deviations')
    plt.yticks([i + bar_width for i in index], [rule_to_str(rule['item']) for rule in all_rules])
    if len(all_rules) > 0:
        plt.legend(handles=[bar_b, bar_a], loc='best')

    if outputFile is not None:
        plt.savefig(outputFile)
    else:
        plt.show()
    plt.close(fig)
