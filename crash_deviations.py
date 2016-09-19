# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
import operator
from collections import defaultdict
import scipy.stats
import math

from pyspark.sql import SQLContext, Row, functions
from pyspark.sql.types import StringType

import download_data
import addons
import gfx_critical_errors


MIN_COUNT = 5 # 5 for chi-squared test.


def get_crashes(sc, versions, days, product='Firefox'):
    return SQLContext(sc).read.format('json').load(download_data.get_paths(versions, days, product))


old_df_a = None
old_df_b = None
saved_counts_a = {}
saved_counts_b = {}
saved_addons = None
saved_critical_errors = None


def get_cached_columns(df, columns):
    global saved_counts_a, saved_counts_b
    saved_counts = saved_counts_a if df == 'a' else saved_counts_b
    return set([list(k)[0][0] for k in saved_counts.keys() if len(k) == 1 and list(k)[0][0] in columns])


def get_cached_first_level_results(df, columns):
    global saved_counts_a, saved_counts_b
    saved_counts = saved_counts_a if df == 'a' else saved_counts_b
    return [(k,v) for k,v in saved_counts.items() if len(k) == 1 and list(k)[0][0] in columns]


def clear_caches(df_a, df_b):
    global old_df_a, old_df_b, saved_counts_a, saved_counts_b, saved_addons

    if df_a != old_df_a:
        saved_counts_a = {}
        saved_addons = None
        old_df_a = df_a

    if df_b != old_df_b:
        saved_counts_b = {}
        old_df_b = df_b


def is_count_empty(df):
    global saved_counts_a, saved_counts_b
    saved_counts = saved_counts_a if df == 'a' else saved_counts_b
    return len(saved_counts) == 0


def save_count(candidate, count, df):
    global saved_counts_a, saved_counts_b
    saved_counts = saved_counts_a if df == 'a' else saved_counts_b
    saved_counts[candidate] = float(count)


def has_count(candidate, df):
    global saved_counts_a, saved_counts_b
    saved_counts = saved_counts_a if df == 'a' else saved_counts_b
    return candidate in saved_counts


def get_count(candidate, df):
    global saved_counts_a, saved_counts_b
    saved_counts = saved_counts_a if df == 'a' else saved_counts_b
    return saved_counts[candidate]


def get_global_count(df, df_name):
    if not has_count(frozenset(), df_name):
        save_count(frozenset(), df.count(), df_name)
    return get_count(frozenset(), df_name)


def get_addons(df):
    global saved_addons

    if saved_addons is None:
        addons = df.select(['signature'] + [functions.explode(df['addons']).alias('addon')]).rdd.zipWithIndex().filter(lambda (v, i): i % 2 == 0).flatMap(lambda (v, i): [(v, 1), (v['addon'], 1)]).reduceByKey(lambda x, y: x + y).collect()

        addons_a = [addon for addon in addons if not isinstance(addon[0], Row)]
        saved_addons = [addon for addon in addons if isinstance(addon[0], Row)]

        for addon, count in addons_a:
            # Could a crash be caused by the absence of an addon? Could be, but it
            # isn't worth the additional complexity.
            save_count(frozenset([(addon.replace('.', '__DOT__'), True)]), count, 'a')

    return saved_addons


def get_gfx_critical_errors(df, all_gfx_critical_errors=None):
    global saved_critical_errors

    if all_gfx_critical_errors is None:
        all_gfx_critical_errors = gfx_critical_errors.get_critical_errors()

    # Aliases for the errors (otherwise Spark fails because it can't find the columns associated to errors, because they contain dots).
    all_gfx_critical_errors = [error.replace('.', '__DOT__') for error in all_gfx_critical_errors]

    if saved_critical_errors is None:
        errors = df.select(['signature'] + [(functions.instr(df['graphics_critical_error'], error.replace('__DOT__', '.')) != 0).alias(error) for error in all_gfx_critical_errors]).rdd.flatMap(lambda v: [(error, 1) for error in all_gfx_critical_errors if v[error]] + [((v['signature'], error), 1) for error in all_gfx_critical_errors if v[error]]).reduceByKey(lambda x, y: x + y).collect()

        critical_errors_a = [error for error in errors if isinstance(error[0], basestring)]
        saved_critical_errors = [error for error in errors if not isinstance(error[0], basestring)]

        for error, count in critical_errors_a:
            save_count(frozenset([(error, True)]), count, 'a')

    return saved_critical_errors


def find_deviations(sc, a, b=None, signature=None, min_support_diff=0.15, min_corr=0.03, all_addons=None, all_gfx_critical_errors=None, analyze_addon_versions=False):
    if b is None and signature is None:
        raise Exception('Either b or signature should not be None')

    if signature is not None:
        b = a.filter(a['signature'] == signature)

    clear_caches(a, b)

    total_a = get_global_count(a, 'a')
    total_b = get_global_count(b, 'b')


    # Count graphics critical errors.
    if all_gfx_critical_errors is None:
        all_gfx_critical_errors = get_gfx_critical_errors(a)

    all_gfx_critical_errors = [(error, c) for (s, error), c in all_gfx_critical_errors if (signature is None or s == signature) and float(c) / total_b > min_support_diff]

    for error, count in all_gfx_critical_errors:
        save_count(frozenset([(error, True)]), count, 'b')

    all_gfx_critical_errors = [error for error, c in all_gfx_critical_errors]


    # Count addons.
    if all_addons is None:
        all_addons = get_addons(a)

    all_addons = [(addon, c) for (s, addon), c in all_addons if (signature is None or s == signature) and float(c) / total_b > min_support_diff]

    # Aliases for the addons (otherwise Spark fails because it can't find the columns associated to addons, because they contain dots).
    for addon, count in all_addons:
        # Could a crash be caused by the absence of an addon? Could be, but it
        # isn't worth the additional complexity.
        save_count(frozenset([(addon.replace('.', '__DOT__'), True)]), count, 'b')

    all_addons = [addon for addon, c in all_addons]


    def augment(df):
        if 'addons' in df.columns:
            def get_version(addons, addon):
                if addons is None:
                    return 'N/A'

                for i in range(0, len(addons), 2):
                    if addons[i] == addon:
                        return addons[i + 1]

                return 'Not installed'

            def create_get_version_udf(addon):
                return functions.udf(lambda addons: get_version(addons, addon), StringType())

            if analyze_addon_versions:
                df = df.select(['*'] + [create_get_version_udf(addon)(df['addons']).alias(addon.replace('.', '__DOT__')) for addon in all_addons])
            else:
                df = df.select(['*'] + [functions.array_contains(df['addons'], addon).alias(addon.replace('.', '__DOT__')) for addon in all_addons])

        if 'uptime' in df.columns:
            df = df.withColumn('startup', df['uptime'] < 60)

        if 'plugin_version' in df.columns:
            df = df.withColumn('plugin', df['plugin_version'].isNotNull())

        if 'app_notes' in df.columns:
            df = df.withColumn('has dual GPUs', (functions.instr(df['app_notes'], 'Has dual GPUs') != 0))

        if 'graphics_critical_error' in df.columns:
            df = df.select(['*'] + [(functions.instr(df['graphics_critical_error'], error.replace('__DOT__', '.')) != 0).alias(error) for error in all_gfx_critical_errors])

        return df


    def drop_unneeded(df):
        return df.select([c for c in df.columns if c not in [
            'signature',
            'total_virtual_memory', 'total_physical_memory', 'available_virtual_memory', 'available_physical_memory', 'oom_allocation_size',
            'app_notes',
            'graphics_critical_error',
            'addons',
            'uptime',
            'cpu_arch', 'cpu_name',
        ]])

    dfA = drop_unneeded(augment(a)).cache()
    dfB = drop_unneeded(augment(b)).cache()

    # dfA.show(3)
    # dfA.printSchema()


    def union(frozenset1, frozenset2):
        res = frozenset1.union(frozenset2)
        if len(set([key for key, value in res])) != len(res):
            return frozenset()
        return res


    def should_prune(parent1, parent2, candidate):
        count_a = get_count(candidate, 'a')
        support_a = count_a / total_a
        count_b = get_count(candidate, 'b')
        support_b = count_b / total_b

        if count_a < MIN_COUNT:
            return True

        if count_b < MIN_COUNT:
            return True

        if support_a < min_support_diff and support_b < min_support_diff:
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
        candidates_left = [c for c in candidates if not has_count(c, 'a' if df == dfA else 'b')]
        # Initialize all results to 0.
        for candidate in candidates_left:
            save_count(candidate, 0, 'a' if df == dfA else 'b')

        broadcastVar = sc.broadcast(candidates_left)
        results = df.rdd.flatMap(lambda p: [(fset, 1) for fset in broadcastVar.value if all(p[key] == value for key, value in fset)]).reduceByKey(lambda x, y: x + y).collect()

        for result in results:
            save_count(result[0], result[1], 'a' if df == dfA else 'b')

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

        print(str(len(previous_candidates[0]) + 1) + ' CANDIDATES: ' + str(len(candidates)))

        results_a = count_candidates(dfA, candidates)
        results_b = count_candidates(dfB, candidates)

        return [result for result in results_b if not should_prune(parents[result][0], parents[result][1], result)]


    candidates = {
      1: [],
    }

    # Generate first level candidates.
    results_a = get_cached_first_level_results('a', dfA.columns)
    a_columns = [c for c in dfA.columns if c not in get_cached_columns('a', dfA.columns)]
    broadcastVar = sc.broadcast(a_columns)
    if len(a_columns) > 0:
      results_a += dfA.rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastVar.value]).reduceByKey(lambda x, y: x + y).collect()

    results_b = get_cached_first_level_results('b', dfB.columns)
    b_columns = [c for c in dfB.columns if c not in get_cached_columns('b', dfB.columns)]
    broadcastVar = sc.broadcast(b_columns)
    if len(b_columns) > 0:
      results_b += dfB.rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastVar.value]).reduceByKey(lambda x, y: x + y).collect()

    for candidate in [count[0] for count in results_a + results_b]:
        if not has_count(candidate, 'a'):
            save_count(candidate, 0, 'a')
        if not has_count(candidate, 'b'):
            save_count(candidate, 0, 'b')
    for count in results_a:
        save_count(count[0], count[1], 'a')
    for count in results_b:
        save_count(count[0], count[1], 'b')

    # Filter first level candidates.
    candidates_tmp = set([count[0] for count in results_b if not should_prune(None, None, count[0])])
    # Remove useless rules (e.g. addon_X=True and addon_X=False or is_garbage_collecting=1 and is_garbage_collecting=None).
    for elem in candidates_tmp:
        elem_key, elem_val = list(elem)[0]

        if elem_val == False and frozenset([(elem_key, True)]) in candidates_tmp:
            continue

        if elem_val == None and frozenset([(elem_key, u'1')]) in candidates_tmp:
            continue

        if elem_val == None and frozenset([(elem_key, u'Active')]) in candidates_tmp:
            continue

        candidates[1].append(elem)

    print('1 RULES: ' + str(len(candidates[1])))

    l = 1
    while len(candidates[l]) > 0 and l < 2:
        l += 1
        candidates[l] = generate_candidates(dfA, dfB, candidates[l - 1])
        print(str(l) + ' RULES: ' + str(len(candidates[l])))

    all_candidates = sum([candidates[i] for i in range(1,l+1)], [])

    alpha = 0.05
    alpha_k = alpha
    results = []
    for candidate in all_candidates:
        count_a = get_count(candidate, 'a')
        count_b = get_count(candidate, 'b')
        support_a = count_a / total_a
        support_b = count_b / total_b

        # Discard element if the support in the subset is not different enough from the support in the entire dataset.
        support_diff = abs(support_a - support_b)
        if support_diff < min_support_diff:
            continue

        # Discard element if the support is almost the same as if the variables were independent.
        if len(candidate) != 1:
            # independent_support_a = reduce(operator.mul, [get_count(frozenset([item]), dfA) / total_a for item in candidate])
            independent_support_b = reduce(operator.mul, [get_count(frozenset([item]), dfB) / total_b for item in candidate])
            # if abs(independent_support_a - support_a) <= max(0.01, 0.15 * support_a) and abs(independent_support_b - support_b) <= max(0.01, 0.15 * support_b):
            if abs(independent_support_b - support_b) <= max(0.01, 0.15 * support_b):
                continue

        # Discard element if it is not significative.
        chi2, p, dof, expected = scipy.stats.chi2_contingency([[count_b, count_a], [total_b - count_b, total_a - count_a]])
        #oddsration, p = scipy.stats.fisher_exact([[count_b, count_a], [total_b - count_b, total_a - count_a]])
        num_candidates = len(candidates[len(candidate)])
        alpha_k = min((alpha / pow(2, len(candidate))) / num_candidates, alpha_k)
        if p > alpha_k:
            continue

        phi = math.sqrt(chi2 / (total_a + total_b))
        if phi < min_corr:
            continue

        transformed_candidate = dict(candidate)
        for key, val in candidate:
            addon_or_error = key.replace('__DOT__', '.')
            if addon_or_error in all_addons:
                transformed_candidate['Addon "' + (addons.get_addon_name(addon_or_error) or addon_or_error) + '"'] = val
                del transformed_candidate[key]
            if key in all_gfx_critical_errors:
                transformed_candidate['GFX_ERROR "' + addon_or_error + '"'] = val
                del transformed_candidate[key]

        results.append({
            'item': transformed_candidate,
            'count_a': count_a,
            'count_b': count_b,
        })


    '''len1 = [result for result in results if len(result['item']) == 1]
    others = [result for result in results if len(result['item']) > 1]

    for result in sorted(len1, key=lambda v: (-abs(v['count_a'] / total_a - v['count_b'] / total_b))):
        print(str(result['item']) + ' - ' + str(result['count_b'] / total_b) + ' - ' + str(result['count_a'] / total_a))

    print('\n\n')

    for result in sorted(others, key=lambda v: (-round(abs(v['count_a'] / total_a - v['count_b'] / total_b), 2), len(v['item']))):
        print(str(result['item']) + ' - ' + str(result['count_b'] / total_b) + ' - ' + str(result['count_a'] / total_a))'''


    return results, total_a, total_b
