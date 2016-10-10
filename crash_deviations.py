# -*- coding: utf-8 -*-
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
import app_notes
import pciids


MIN_COUNT = 5 # 5 for chi-squared test.


def get_crashes(sc, versions, days, product='Firefox'):
    return SQLContext(sc).read.format('json').load(download_data.get_paths(versions, days, product))


def find_deviations(sc, reference, groups=None, signatures=None, min_support_diff=0.15, min_corr=0.03, all_addons=None, all_gfx_critical_errors=None, all_app_notes=None, analyze_addon_versions=False):
    if groups is None and signatures is None:
        raise Exception('Either groups or signatures should not be None')

    if signatures is not None:
        groups = [(signature, reference.filter(reference['signature'] == signature)) for signature in signatures]
        total_groups = dict(reference.select('signature').filter(reference['signature'].isin(signatures)).groupBy('signature').count().rdd.map(lambda p: (p['signature'], p['count'])).collect())
    else:
        total_groups = dict([(group_name, group_df.count()) for group_name, group_df in groups])

    for group_name, df in groups:
        if group_name in total_groups and total_groups[group_name] < MIN_COUNT:
            print(group_name + ' is too small: ' + str(total_groups[group_name]) + ' crash reports.')

    total_reference = reference.count()
    group_names = [group_name for group_name, group_df in groups if group_name in total_groups and total_groups[group_name] >= MIN_COUNT]
    if signatures is not None:
        signatures = group_names


    saved_counts = {}


    def get_columns(df, columns):
        if df not in saved_counts:
            return set()
        return set([list(k)[0][0] for k in saved_counts[df].keys() if len(k) == 1 and list(k)[0][0] in columns])


    def get_first_level_results(df, columns):
        if df not in saved_counts:
            return []
        return [(k,v) for k,v in saved_counts[df].items() if len(k) == 1 and list(k)[0][0] in columns]


    def save_count(candidate, count, df):
        if df not in saved_counts:
            saved_counts[df] = {}
        saved_counts[df][candidate] = float(count)


    def get_count(candidate, df):
        return saved_counts[df][candidate]


    def save_results(results_ref, results_groups):
        all_results = results_ref + sum(results_groups.values(), [])

        for element, count in all_results:
            if isinstance(element, basestring):
                element = frozenset([(element.replace('.', '__DOT__'), True)])

            save_count(element, 0, 'reference')
            for group_name in group_names:
                save_count(element, 0, group_name)

        for element, count in results_ref:
            if isinstance(element, basestring):
                element = frozenset([(element.replace('.', '__DOT__'), True)])

            save_count(element, count, 'reference')

        for group_name in group_names:
            for element, count in results_groups[group_name]:
                if isinstance(element, basestring):
                    element = frozenset([(element.replace('.', '__DOT__'), True)])

                save_count(element, count, group_name)


    def count_substrings(substrings, field_name):
        substrings = [substring.replace('.', '__DOT__') for substring in substrings]

        if signatures is not None:
            found_substrings = reference.select(['signature'] + [(functions.instr(reference[field_name], substring.replace('__DOT__', '.')) != 0).alias(substring) for substring in substrings]).rdd.flatMap(lambda v: [(substring, 1) for substring in substrings if v[substring]] + ([] if v['signature'] not in signatures else [((v['signature'], substring), 1) for substring in substrings if v[substring]])).reduceByKey(lambda x, y: x + y).collect()

            substrings_ref = [substring for substring in found_substrings if isinstance(substring[0], basestring)]
            substrings_signatures = [substring for substring in found_substrings if not isinstance(substring[0], basestring)]
            substrings_groups = dict([(signature, [(substring, count) for (s, substring), count in substrings_signatures if s == signature]) for signature in signatures])
        else:
            substrings_ref = reference.select([(functions.instr(reference[field_name], substring.replace('__DOT__', '.')) != 0).alias(substring) for substring in substrings]).rdd.flatMap(lambda v: [(substring, 1) for substring in substrings if v[substring]]).reduceByKey(lambda x, y: x + y).collect()

            substrings_groups = dict([(group[0], group[1].select([(functions.instr(reference[field_name], substring.replace('__DOT__', '.')) != 0).alias(substring) for substring in substrings]).rdd.flatMap(lambda v: [(substring, 1) for substring in substrings if v[substring]]).reduceByKey(lambda x, y: x + y).collect()) for group in groups])

        save_results(substrings_ref, substrings_groups)

        return set([substring for substring, count in substrings_ref if float(count) / total_reference > min_support_diff] + [substring for group_name in group_names for substring, count in substrings_groups[group_name] if float(count) / total_groups[group_name] > min_support_diff])


    # Count app notes
    if all_app_notes is None:
        all_app_notes = count_substrings(app_notes.get_app_notes(), 'app_notes')


    # Count graphics critical errors.
    if all_gfx_critical_errors is None:
        all_gfx_critical_errors = count_substrings(gfx_critical_errors.get_critical_errors(), 'graphics_critical_error')


    # Count addons.
    if all_addons is None:
        if signatures is not None:
            found_addons = reference.select(['signature'] + [functions.explode(reference['addons']).alias('addon')]).rdd.zipWithIndex().filter(lambda (v, i): i % 2 == 0).flatMap(lambda (v, i): [(v, 1), (v['addon'], 1)] if v['signature'] in signatures else [(v['addon'], 1)]).reduceByKey(lambda x, y: x + y).collect()

            addons_ref = [addon for addon in found_addons if not isinstance(addon[0], Row)]
            addons_signatures = [addon for addon in found_addons if isinstance(addon[0], Row)]
            addons_groups = dict([(signature, [(addon, count) for (s, addon), count in addons_signatures if s == signature]) for signature in signatures])
        else:
            addons_ref = reference.select(functions.explode(reference['addons']).alias('addon')).rdd.zipWithIndex().filter(lambda (v, i): i % 2 == 0).map(lambda (v, i): (v['addon'], 1)).reduceByKey(lambda x, y: x + y).collect()

            addons_groups = dict([(group[0], group[1].select(functions.explode(reference['addons']).alias('addon')).rdd.zipWithIndex().filter(lambda (v, i): i % 2 == 0).map(lambda (v, i): (v['addon'], 1)).reduceByKey(lambda x, y: x + y).collect()) for group in groups])

        save_results(addons_ref, addons_groups)

        all_addons = set([addon for addon, count in addons_ref if float(count) / total_reference > min_support_diff] + [addon for group_name in group_names for addon, count in addons_groups[group_name] if float(count) / total_groups[group_name] > min_support_diff])


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
            df = df.select(['*'] + [(functions.instr(df['app_notes'], app_note.replace('__DOT__', '.')) != 0).alias(app_note) for app_note in all_app_notes] + [(functions.instr(df['app_notes'], 'Has dual GPUs') != 0).alias('has dual GPUs')])

        if 'graphics_critical_error' in df.columns:
            df = df.select(['*'] + [(functions.instr(df['graphics_critical_error'], error.replace('__DOT__', '.')) != 0).alias(error) for error in all_gfx_critical_errors])

        if 'total_virtual_memory' in df.columns and 'platform_version' in df.columns and 'platform' in df.columns:
            def get_arch(total_virtual_memory, platform, platform_version):
                if total_virtual_memory:
                    if int(total_virtual_memory) < 2684354560:
                        return 'x86'
                    elif int(total_virtual_memory) > 2684354560:
                        return 'amd64'
                elif platform == 'Mac OS X':
                    return 'amd64'
                else:
                    if 'i686' in platform_version:
                        return 'x86'
                    elif 'x86_64' in platform_version:
                        return 'amd64'

            get_arch_udf = functions.udf(get_arch, StringType())

            df = df.withColumn('os_arch', get_arch_udf(df['total_virtual_memory'], df['platform'], df['platform_version']))

        if 'adapter_driver_version' in df.columns:
            def get_driver_version(adapter_vendor_id, adapter_driver_version):
                # XXX: Sometimes we have a driver which is not actually made by the vendor,
                #      in those cases these rules are not valid (e.g. 6.1.7600.16385).
                if adapter_driver_version:
                    if adapter_vendor_id == '0x8086' or adapter_vendor_id == '8086':
                        return adapter_driver_version[adapter_driver_version.rfind('.')+1:]
                    elif adapter_vendor_id == '0x10de' or adapter_vendor_id == '10de':
                        return adapter_driver_version[-6:-5] + adapter_driver_version[-4:-2] + '.' + adapter_driver_version[-2:]
                    # TODO: AMD?

                return adapter_driver_version

            get_driver_version_udf = functions.udf(get_driver_version, StringType())

            df = df.withColumn('adapter_driver_version_clean', get_driver_version_udf(df['adapter_vendor_id'], df['adapter_driver_version']))

        return df


    def drop_unneeded(df):
        return df.select([c for c in df.columns if c not in [
            'total_virtual_memory', 'total_physical_memory', 'available_virtual_memory', 'available_physical_memory', 'oom_allocation_size',
            'app_notes',
            'graphics_critical_error',
            'addons',
            'uptime',
            'date',
        ]])

    dfReference = drop_unneeded(augment(reference)).cache()
    if signatures is None:
        groups = [(group[0], drop_unneeded(augment(group[1])).cache()) for group in groups]

    # dfReference.show(3)
    # dfReference.printSchema()


    def union(frozenset1, frozenset2):
        res = frozenset1.union(frozenset2)
        if len(set([key for key, value in res])) != len(res):
            return frozenset()
        return res


    def should_prune(group_name, parent1, parent2, candidate):
        count_reference = get_count(candidate, 'reference')
        support_reference = count_reference / total_reference
        count_group = get_count(candidate, group_name)
        support_group = count_group / total_groups[group_name]

        if count_reference < MIN_COUNT:
            return True

        if count_group < MIN_COUNT:
            return True

        if support_reference < min_support_diff and support_group < min_support_diff:
            return True

        if parent1 is None or parent2 is None:
            return False

        parent1_count_reference = get_count(parent1, 'reference')
        parent1_support_reference = parent1_count_reference / total_reference
        parent1_count_group = get_count(parent1, group_name)
        parent1_support_group = parent1_count_group / total_groups[group_name]
        parent2_count_reference = get_count(parent2, 'reference')
        parent2_support_reference = parent2_count_reference / total_reference
        parent2_count_group = get_count(parent2, group_name)
        parent2_support_group = parent2_count_group / total_groups[group_name]

        # TODO: Add fixed relations pruning.

        # If there's no large change in the support of a set when extending the set, prune the node.
        threshold = min(0.05, min_support_diff / 2)
        if (abs(parent1_support_reference - support_reference) < threshold or abs(parent2_support_reference - support_reference) < threshold) and (abs(parent1_support_group - support_group) < threshold or abs(parent2_support_group - support_group) < threshold):
            return True

        # If there's no significative change, prune the node.
        chi2, p1_reference = scipy.stats.chisquare([parent1_count_reference, count_reference])
        chi2, p2_reference = scipy.stats.chisquare([parent2_count_reference, count_reference])
        chi2, p1_group = scipy.stats.chisquare([parent1_count_group, count_group])
        chi2, p2_group = scipy.stats.chisquare([parent2_count_group, count_group])
        if (p1_reference > 0.05 or p2_reference > 0.05) and (p1_group > 0.05 or p2_group > 0.05):
            return True

        return False

    def count_candidates(candidates):
        broadcastVar1 = sc.broadcast(set.union(*candidates.values()))
        if signatures is not None:
            broadcastVar2 = sc.broadcast(candidates)
            results = dfReference.rdd.flatMap(lambda p: [(fset, 1) for fset in broadcastVar1.value if all(p[key] == value for key, value in fset)] + ([] if p['signature'] not in signatures else [((p['signature'], fset), 1) for fset in broadcastVar2.value[p['signature']] if all(p[key] == value for key, value in fset)])).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

            results_ref = [r for r in results if isinstance(r[0], frozenset)]
            results_groups = dict([(signature, [(r[0][1], r[1]) for r in results if not isinstance(r[0], frozenset) and r[0][0] == signature]) for signature in signatures])
        else:
            results_ref = dfReference.rdd.flatMap(lambda p: [(fset, 1) for fset in broadcastVar1.value if all(p[key] == value for key, value in fset)]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()
            results_groups = []
            for group in groups:
                broadcastVar2 = sc.broadcast(candidates[group[0]])
                results_groups.append((group[0], group[1].rdd.flatMap(lambda p: [(fset, 1) for fset in broadcastVar2.value if all(p[key] == value for key, value in fset)]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()))
            results_groups = dict(results_groups)

        save_results(results_ref, results_groups)

        return results_groups


    def generate_candidates(previous_candidates):
        candidates = {}
        parents = {}

        for group_name in group_names:
            candidates[group_name] = set()
            parents[group_name] = {}

            for i in range(0, len(previous_candidates[group_name])):
                for j in range(i + 1, len(previous_candidates[group_name])):
                    props = union(previous_candidates[group_name][i], previous_candidates[group_name][j])
                    if len(props) == len(previous_candidates[group_name][i]) + 1 and props not in candidates[group_name]:
                        candidates[group_name].add(props)
                        parents[group_name][props] = (previous_candidates[group_name][i], previous_candidates[group_name][j])

        print(str(len(previous_candidates[group_names[0]][0]) + 1) + ' CANDIDATES: ' + str(sum(len(candidates[group_name]) for group_name in group_names)))

        results_groups = count_candidates(candidates)

        return dict([(group_name, list(set([result[0] for result in results_groups[group_name] if not should_prune(group_name, parents[group_name][result[0]][0], parents[group_name][result[0]][1], result[0])]))) for group_name in group_names])


    candidates = {
      1: dict([(group_name, []) for group_name in group_names])
    }

    # Generate first level candidates.
    results_ref = get_first_level_results('reference', dfReference.columns)
    results_groups = dict([(group_name, get_first_level_results(group_name, dfReference.columns)) for group_name in group_names])
    columns = [c for c in dfReference.columns if c not in get_columns('reference', dfReference.columns) and c != 'signature']
    broadcastVar = sc.broadcast(columns)
    if signatures is not None:
        results = dfReference.rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastVar.value] + ([] if p['signature'] not in signatures else [((p['signature'], frozenset([(key,p[key])])), 1) for key in broadcastVar.value])).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

        results_ref += [r for r in results if isinstance(r[0], frozenset)]
        for group_name in group_names:
            results_groups[group_name] += [(r[0][1], r[1]) for r in results if not isinstance(r[0], frozenset) and r[0][0] == group_name]
    else:
        results_ref += dfReference.rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastVar.value]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()
        for group in groups:
            results_groups[group[0]] += group[1].rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastVar.value]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

    save_results(results_ref, results_groups)


    # Filter first level candidates.
    for group_name in group_names:
        candidates[1][group_name] = set([element for element, count in results_groups[group_name] if not should_prune(group_name, None, None, element)])


    # Remove useless rules (e.g. addon_X=True and addon_X=False or is_garbage_collecting=1 and is_garbage_collecting=None).
    def ignore_rule(candidate, candidates):
        elem_key, elem_val = list(candidate)[0]

        if elem_val == False and frozenset([(elem_key, True)]) in candidates:
            return True

        if elem_val == None and frozenset([(elem_key, u'1')]) in candidates:
            return True

        if elem_val == None and frozenset([(elem_key, u'Active')]) in candidates:
            return True

        # We only care when submitted_from_infobar is true.
        if elem_key == 'submitted_from_infobar' and elem_val is None:
            return True

        return False

    for group_name in group_names:
        candidates[1][group_name] = [c for c in candidates[1][group_name] if not ignore_rule(c, candidates[1][group_name])]

    print('1 RULES: ' + str(sum(len(candidates[1][group_name]) for group_name in group_names)))


    l = 1
    while sum(len(candidates[l][group_name]) for group_name in group_names) > 0 and l < 2:
        l += 1
        candidates[l] = generate_candidates(candidates[l - 1])
        print(str(l) + ' RULES: ' + str(sum(len(candidates[l][group_name]) for group_name in group_names)))


    all_candidates = dict([(group_name, sum([candidates[i][group_name] for i in range(1,l+1)], [])) for group_name in group_names])


    alpha = 0.05
    alpha_k = alpha
    results = {}
    for group_name in group_names:
        results[group_name] = []

        total_group = total_groups[group_name]

        for candidate in all_candidates[group_name]:
            count_reference = get_count(candidate, 'reference')
            count_group = get_count(candidate, group_name)
            support_reference = count_reference / total_reference
            support_group = count_group / total_group

            # Discard element if the support in the subset is not different enough from the support in the entire dataset.
            support_diff = abs(support_reference - support_group)
            if support_diff < min_support_diff:
                continue

            # Discard element if the support is almost the same as if the variables were independent.
            if len(candidate) != 1:
                # independent_support_reference = reduce(operator.mul, [get_count(frozenset([item]), 'reference') / total_reference for item in candidate])
                independent_support_group = reduce(operator.mul, [get_count(frozenset([item]), group_name) / total_group for item in candidate])
                # if abs(independent_support_reference - support_reference) <= max(0.01, 0.15 * support_reference) and abs(independent_support_group - support_group) <= max(0.01, 0.15 * support_group):
                if abs(independent_support_group - support_group) <= max(0.01, 0.15 * support_group):
                    continue

                # TODO: Don't assume just two elements.
                elem1 = [get_count(frozenset([item]), group_name) for item in candidate][0]
                elem2 = [get_count(frozenset([item]), group_name) for item in candidate][1]
                oddsratio, p = scipy.stats.fisher_exact([[count_group, total_group - elem1], [total_group - elem2, total_group - count_group]])
                if p > alpha_k:
                    continue

            # Discard element if it is not significative.
            chi2, p, dof, expected = scipy.stats.chi2_contingency([[count_group, count_reference], [total_group - count_group, total_reference - count_reference]])
            #oddsration, p = scipy.stats.fisher_exact([[count_group, count_reference], [total_group - count_group, total_reference - count_reference]])
            num_candidates = len(candidates[len(candidate)][group_name])
            alpha_k = min((alpha / pow(2, len(candidate))) / num_candidates, alpha_k)
            if p > alpha_k:
                continue

            phi = math.sqrt(chi2 / (total_reference + total_group))
            if phi < min_corr:
                continue

            transformed_candidate = dict(candidate)
            dict_candidate = transformed_candidate.copy()
            for key, val in candidate:
                addon_or_error = key.replace('__DOT__', '.')
                if addon_or_error in all_addons:
                    dict_candidate['Addon "' + (addons.get_addon_name(addon_or_error) or addon_or_error) + '"'] = val
                    del dict_candidate[key]
                elif key in all_gfx_critical_errors:
                    dict_candidate['GFX_ERROR "' + addon_or_error + '"'] = val
                    del dict_candididate[key]
                elif key in all_app_notes:
                    dict_candidate['"' + addon_or_error + '" in app_notes'] = val
                    del dict_candididate[key]
                elif key == 'adapter_vendor_id':
                    dict_candidate[key] = pciids.get_vendor_name(val)
                elif key == 'adapter_device_id' and 'adapter_vendor_id' in transformed_candidate:
                    dict_candidate[key] = pciids.get_device_name(transformed_candidate['adapter_vendor_id'], val)

            results[group_name].append({
                'item': dict_candidate,
                'count_reference': count_reference,
                'count_group': count_group,
            })


    return results, total_reference, total_groups


def print_results(results, total_reference, total_groups):
    def to_percentage(num):
        result = "%.2f" % (num * 100)

        if result == '100.00':
            return '100.0'

        if len(result[0:result.index('.')]) == 1:
            return '0' + result

        return result

    def item_to_label(rule):
        return u' ∧ '.join([key + '="' + str(value) + '"' for key, value in rule.items()]).encode('utf-8')

    def print_all(results):
        for result in results:
            print('(' + to_percentage(result['count_group'] / total_groups[group]) + '% in signature vs ' + to_percentage(result['count_reference'] / total_reference) + '% overall) ' + item_to_label(result['item']))

    for group in results.keys():
        print(group)

        len1 = [result for result in results[group] if len(result['item']) == 1]
        others = [result for result in results[group] if len(result['item']) > 1]

        print_all(sorted(len1, key=lambda v: (-abs(v['count_reference'] / total_reference - v['count_group'] / total_groups[group]))))

        print('\n\n')

        print_all(sorted(others, key=lambda v: (-round(abs(v['count_reference'] / total_reference - v['count_group'] / total_groups[group]), 2), len(v['item']))))

        print('\n\n\n')
