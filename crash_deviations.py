# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
import operator
from collections import defaultdict
import scipy.stats
import math

from pyspark.sql import functions
from pyspark.sql import SQLContext

import download_data


MIN_COUNT = 5 # 5 for chi-squared test.


def get_crashes(sc, version, days):
    return SQLContext(sc).read.format('json').load(download_data.get_paths(version, days))


def find_deviations(sc, a, b, min_support_diff, min_corr, max_addons):
    # XXX: Also consider addons in the A group? a.select(functions.explode(a['addons']).alias('addon')).collect() +
    all_addons_versions = b.select(functions.explode(b['addons']).alias('addon')).collect()
    all_addons = list()
    for i in range(0, len(all_addons_versions), 2):
        all_addons.append(all_addons_versions[i].asDict()['addon'])

    # print(len(all_addons))

    all_addons_counts = defaultdict(int)
    for addon in all_addons:
        all_addons_counts[addon] += 1

    # Too many addons, restrict to the top max_addons.
    all_addons = [k for k, v in sorted(all_addons_counts.items(), key=lambda (k, v): v, reverse=True)[:max_addons]]

    # print(all_addons)

    # Aliases for the addons (otherwise Spark fails because it can't find the columns associated to addons, probably because they contain special characters).
    # TODO: Use https://services.addons.mozilla.org/en-US/firefox/api/1.4/search/guid:GUID to get the names.
    addons_map = {}
    reverse_addons_map = {}
    for i in range(0, len(all_addons)):
        addons_map[all_addons[i]] = 'a' + str(i)
        reverse_addons_map['a' + str(i)] = all_addons[i]


    def augment(df):
        df = df.select(['*'] + [functions.array_contains(df['addons'], addon).alias(addons_map[addon]) for addon in all_addons])

        return df.withColumn('startup', df['uptime'] < 60)\
                 .withColumn('plugin', df['plugin_version'].isNotNull())


    def drop_unneeded(df):
        return df.drop('signature')\
                 .drop('total_virtual_memory')\
                 .drop('total_physical_memory')\
                 .drop('available_virtual_memory')\
                 .drop('available_physical_memory')\
                 .drop('oom_allocation_size')\
                 .drop('app_notes')\
                 .drop('addons')\
                 .drop('uptime')\
                 .drop('plugin_version')\
                 .drop('cpu_arch')\
                 .drop('cpu_name')

    dfA = drop_unneeded(augment(a)).cache()
    # dfA.show(3)
    total_a = dfA.count()
    orig_total_a = total_a
    dfB = drop_unneeded(augment(b)).cache()
    total_b = dfB.count()

    # dfA.printSchema()

    saved_counts_a = {}
    saved_counts_b = {}


    def save_count(candidate, count, df):
        saved_counts = saved_counts_a if df == dfA else saved_counts_b
        saved_counts[candidate] = float(count)


    def get_count(candidate, df):
        saved_counts = saved_counts_a if df == dfA else saved_counts_b
        return saved_counts[candidate]


    def union(frozenset1, frozenset2):
        res = frozenset1.union(frozenset2)
        if len(set([key for key, value in res])) != len(res):
            return frozenset()
        return res


    def should_prune(parent1, parent2, candidate):
        count_a = get_count(candidate, dfA)
        support_a = count_a / total_a
        count_b = get_count(candidate, dfB)
        support_b = count_b / total_b

        if count_a < MIN_COUNT:
            return True

        if count_b < MIN_COUNT:
            return True

        if count_a / total_a < min_support_diff and count_b / total_b < min_support_diff:
            return True

        if parent1 is None or parent2 is None:
            return False

        parent1_count_a = get_count(parent1, dfA)
        parent1_support_a = parent1_count_a / total_a
        parent1_count_b = get_count(parent1, dfB)
        parent1_support_b = parent1_count_b / total_b
        parent2_count_a = get_count(parent2, dfA)
        parent2_support_a = parent2_count_a / total_a
        parent2_count_b = get_count(parent2, dfB)
        parent2_support_b = parent2_count_b / total_b

        # TODO: Add fixed relations pruning.

        # If there's no large change in the support of a set when extending the set, prune the node.
        threshold = min(0.01, min_support_diff / 2)
        if (abs(parent1_support_a - support_a) < threshold and abs(parent1_support_b - support_b) < threshold) and\
           (abs(parent2_support_a - support_a) < threshold and abs(parent2_support_b - support_b) < threshold):
            return True

        # If there's no significative change, prune the node.
        chi2, p1_a = scipy.stats.chisquare([parent1_count_a, count_a])
        chi2, p2_a = scipy.stats.chisquare([parent2_count_a, count_a])
        chi2, p1_b = scipy.stats.chisquare([parent1_count_b, count_b])
        chi2, p2_b = scipy.stats.chisquare([parent2_count_b, count_b])
        if p1_a > 0.05 and p2_a > 0.05 and p1_b > 0.05 and p2_b > 0.05:
            return True

        return False


    def count_candidates(df, candidates):
        broadcastVar = sc.broadcast(candidates)
        results = df.rdd.flatMap(lambda p: [(fset, 1) for fset in broadcastVar.value if all(p[key] == value for key, value in fset)]).reduceByKey(lambda x, y: x + y).collect()

        for result in results:
            save_count(result[0], result[1], df)
        for candidate in [candidate for candidate in candidates if candidate not in [result[0] for result in results]]:
            save_count(candidate, 0, df)

        return [result[0] for result in results]


    def generate_candidates(dfA, dfB, previous_candidates):
        candidates = set()
        parents = {}

        for i in range(0, len(previous_candidates)):
            for j in range(i + 1, len(previous_candidates)):
                props = union(previous_candidates[i], previous_candidates[j])
                if len(props) == len(previous_candidates[i]) + 1 and props not in candidates:
                    candidates.add(props)
                    parents[props] = (previous_candidates[i], previous_candidates[j])

        results_a = count_candidates(dfA, candidates)
        results_b = count_candidates(dfB, candidates)

        return [result for result in results_b if not should_prune(parents[result][0], parents[result][1], result)]


    candidates = {
      1: [],
    }

    # Generate first level candidates.
    broadcastVar = sc.broadcast(dfB.columns)
    results_b = dfB.rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastVar.value]).reduceByKey(lambda x, y: x + y).collect()
    for count in results_b:
        save_count(count[0], count[1], dfB)
    results_a = count_candidates(dfA, [count[0] for count in results_b])

    # Filter first level candidates.
    candidates_tmp = set([count[0] for count in results_b if not should_prune(None, None, count[0])])
    # Remove useless rules (e.g. addon_X=True and addon_X=False or is_garbage_collecting=1 and is_garbage_collecting=None).
    for elem in candidates_tmp:
        elem_key, elem_val = [(key, val) for key, val in elem][0]

        if elem_val == False and frozenset([(elem_key, True)]) in candidates_tmp:
            continue

        if elem_val == None and frozenset([(elem_key, u'1')]) in candidates_tmp:
            continue

        candidates[1].append(elem)

    # Filter reference dataset using the candidates that have support == 100%.
    prior_candidates = [c for c in candidates[1] if get_count(c, dfB) == total_b]
    candidates[1] = [c for c in candidates[1] if get_count(c, dfB) != total_b]
    if len(prior_candidates) > 0:
        condition = reduce(operator.__and__, [dfA[key] == value if value is not None else dfA[key].isNull() for c in prior_candidates for key, value in c])
        print(condition)
        dfA = dfA.filter(condition).cache()

        total_a = dfA.count()

        results_a = count_candidates(dfA, candidates[1])

    l = 1
    print('1 CANDIDATES: ' + str(len(candidates[1])))
    while len(candidates[l]) > 0 and l < 2:
        l += 1
        candidates[l] = generate_candidates(dfA, dfB, candidates[l - 1])
        print(str(l) + ' CANDIDATES: ' + str(len(candidates[l])))

    all_candidates = prior_candidates + sum([candidates[i] for i in range(1,l+1)], [])

    alpha = 0.05
    alpha_k = alpha
    results = []
    for candidate in all_candidates:
        count_a = get_count(candidate, dfA)
        count_b = get_count(candidate, dfB)
        tot_a = total_a if candidate not in prior_candidates else orig_total_a
        support_a = count_a / tot_a
        support_b = count_b / total_b

        # Discard element if the support in the subset is not different enough from the support in the entire dataset.
        support_diff = abs(support_a - support_b)
        if support_diff < min_support_diff:
            continue

        # Discard element if it is not significative.
        chi2, p, dof, expected = scipy.stats.chi2_contingency([[count_b, count_a], [total_b - count_b, tot_a - count_a]])
        #oddsration, p = scipy.stats.fisher_exact([[count_b, count_a], [total_b - count_b, tot_a - count_a]])
        alpha_k = min((alpha / pow(2, len(candidate))) / len(candidates[len(candidate)]), alpha_k)
        if p > alpha_k:
            continue

        phi = math.sqrt(chi2 / (tot_a + total_b))
        if phi < min_corr:
            continue

        # Discard element if the support is almost the same as if the variables were independent.
        if len(candidate) != 1:
            independent_support = reduce(operator.mul, [get_count(frozenset([item]), dfB) / total_b for item in candidate])
            if (independent_support == 1.0 and support_b == 1.0) or (independent_support != 1.0 and support_b - 0.05 <= independent_support <= support_b + 0.05):
                # print('SKIP ' + str(candidate) + ' BECAUSE ALMOST INDEPENDENT (' + str(independent_support) + ', ' + str(support_b) + ')')
                continue
            # else:
                # print('DONTSKIP ' + str(candidate) + ' BECAUSE ALMOST INDEPENDENT (' + str(independent_support) + ', ' + str(support_b) + ')')

        transformed_candidate = dict(candidate)
        for key, val in candidate:
            if key in reverse_addons_map:
                transformed_candidate[reverse_addons_map[key]] = val
                del transformed_candidate[key]

        results.append({
            'item': transformed_candidate,
            'support_a': support_a,
            'support_b': support_b,
        })


    '''len1 = [result for result in results if len(result['item']) == 1]
    len2 = [result for result in results if len(result['item']) == 2]
    others = [result for result in results if len(result['item']) > 2]

    for result in sorted(len1, key=lambda v: (-abs(v['support_a'] - v['support_b']))):
        print(str(result['item']) + ' - ' + str(result['support_diff']) + ' - ' + str(result['support_b']) + ' - ' + str(result['support_a']))

    print('\n\n')

    for result in sorted(len2, key=lambda v: (-abs(v['support_a'] - v['support_b']))):
        print(str(result['item']) + ' - ' + str(result['support_diff']) + ' - ' + str(result['support_b']) + ' - ' + str(result['support_a']))

    print('\n\n')

    for result in sorted(others, key=lambda v: (-round(abs(v['support_a'] - v['support_b']), 2), len(v['item']))):
        print(str(result['item']) + ' - ' + str(result['support_diff']) + ' - ' + str(result['support_b']) + ' - ' + str(result['support_a']))'''


    return results
