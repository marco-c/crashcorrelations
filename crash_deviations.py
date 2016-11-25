# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
import operator
from collections import defaultdict
import scipy.stats
import math
import time

from pyspark.sql import SQLContext, Row, functions
from pyspark.sql.types import StringType

import download_data
import addons
import gfx_critical_errors
import app_notes
import pciids


MIN_COUNT = 5 # 5 for chi-squared test.


# TODO: Once https://bugzilla.mozilla.org/show_bug.cgi?id=1311647 is fixed, stop using SuperSearch and use the Telemetry data for everything.
def get_crashes(sc, versions, days, product='Firefox'):
    return SQLContext(sc).read.format('json').load(download_data.get_paths(versions, days, product))


# TODO: Once https://bugzilla.mozilla.org/show_bug.cgi?id=1311647 is fixed, stop using SuperSearch and use the Telemetry data for everything.
def get_telemetry_crashes(sc, versions, days, product='Firefox'):
    days = download_data.get_days(days)
    dataset = SQLContext(sc).read.load(['s3://telemetry-parquet/socorro_crash/v1/crash_date=' + day.strftime('%Y%m%d') for day in days], 'parquet')
    return dataset.filter((dataset['product'] == product) & (dataset['version'].isin(versions)))


def find_deviations(sc, reference, groups=None, signatures=None, min_support_diff=0.15, min_corr=0.03, all_addons=None, all_gfx_critical_errors=None, all_app_notes=None, telemetry_dataset=None):
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
        broadcastSignatures = sc.broadcast(set(signatures))


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

        for element, count in results_ref:
            if isinstance(element, basestring):
                element = frozenset([(element.replace('.', '__DOT__'), True)])

            save_count(element, count, 'reference')
            for group_name in group_names:
                save_count(element, 0, group_name)

        for group_name in group_names:
            for element, count in results_groups[group_name]:
                if isinstance(element, basestring):
                    element = frozenset([(element.replace('.', '__DOT__'), True)])

                save_count(element, count, group_name)


    def count_substrings(substrings, field_name):
        substrings = [substring.replace('.', '__DOT__') for substring in substrings]

        if signatures is not None:
            found_substrings = reference.select(['signature'] + [(functions.instr(reference[field_name], substring.replace('__DOT__', '.')) != 0).alias(str(substrings.index(substring))) for substring in substrings]).rdd.flatMap(lambda v: [(i, 1) for i in range(0, len(substrings)) if v[str(i)]] + ([] if v['signature'] not in broadcastSignatures.value else [((v['signature'], i), 1) for i in range(0, len(substrings)) if v[str(i)]])).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

            substrings_ref = [(substrings[elem[0]], elem[1]) for elem in found_substrings if isinstance(elem[0], int)]
            substrings_signatures = [elem for elem in found_substrings if not isinstance(elem[0], int)]
            substrings_groups = dict([(signature, [(substrings[i], count) for (s, i), count in substrings_signatures if s == signature]) for signature in signatures])
        else:
            substrings_ref = reference.select([(functions.instr(reference[field_name], substring.replace('__DOT__', '.')) != 0).alias(substring) for substring in substrings]).rdd.flatMap(lambda v: [(substring, 1) for substring in substrings if v[substring]]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

            substrings_groups = dict([(group[0], group[1].select([(functions.instr(group[1][field_name], substring.replace('__DOT__', '.')) != 0).alias(substring) for substring in substrings]).rdd.flatMap(lambda v: [(substring, 1) for substring in substrings if v[substring]]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()) for group in groups])

        all_substrings_ref = set([substring for substring, count in substrings_ref if float(count) / total_reference > min_support_diff])
        all_substrings_groups = dict([(group_name, set([substring for substring, count in substrings_groups[group_name] if float(count) / total_groups[group_name] > min_support_diff])) for group_name in group_names])
        all_substrings = all_substrings_ref.union(*all_substrings_groups.values())

        substrings_ref = [(substring, count) for substring, count in substrings_ref if substring in all_substrings]
        for group_name in group_names:
            substrings_groups[group_name] = [(substring, count) for substring, count in substrings_groups[group_name] if substring in all_substrings_ref.union(all_substrings_groups[group_name])]

        save_results(substrings_ref, substrings_groups)

        return all_substrings


    # Count app notes
    if all_app_notes is None:
        print('Counting app_notes...')
        t = time.time()
        all_app_notes = count_substrings(app_notes.get_app_notes(), 'app_notes')
        print('[DONE ' + str(time.time() - t) + ']\n')


    # Count graphics critical errors.
    if all_gfx_critical_errors is None:
        print('Counting graphics_critical_errors...')
        t = time.time()
        all_gfx_critical_errors = count_substrings(gfx_critical_errors.get_critical_errors(), 'graphics_critical_error')
        print('[DONE ' + str(time.time() - t) + ']\n')


    # Count addons.
    if all_addons is None:
        print('Counting addons...')
        t = time.time()
        if signatures is not None:
            found_addons = reference.select(['signature'] + [functions.explode(reference['addons']).alias('addon')]).rdd.zipWithIndex().filter(lambda (v, i): i % 2 == 0).flatMap(lambda (v, i): [(v, 1), (v['addon'], 1)] if v['signature'] in broadcastSignatures.value else [(v['addon'], 1)]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

            addons_ref = [addon for addon in found_addons if not isinstance(addon[0], Row)]
            addons_signatures = [addon for addon in found_addons if isinstance(addon[0], Row)]
            addons_groups = dict([(signature, [(addon, count) for (s, addon), count in addons_signatures if s == signature]) for signature in signatures])
        else:
            addons_ref = reference.select(functions.explode(reference['addons']).alias('addon')).rdd.zipWithIndex().filter(lambda (v, i): i % 2 == 0).map(lambda (v, i): (v['addon'], 1)).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

            addons_groups = dict([(group[0], group[1].select(functions.explode(reference['addons']).alias('addon')).rdd.zipWithIndex().filter(lambda (v, i): i % 2 == 0).map(lambda (v, i): (v['addon'], 1)).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()) for group in groups])

        all_addons_ref = set([addon for addon, count in addons_ref if float(count) / total_reference > min_support_diff])
        all_addons_groups = dict([(group_name, set([addon for addon, count in addons_groups[group_name] if float(count) / total_groups[group_name] > min_support_diff])) for group_name in group_names])
        all_addons = all_addons_ref.union(*all_addons_groups.values())

        addons_ref = [(addon, count) for addon, count in addons_ref if addon in all_addons]
        for group_name in group_names:
            addons_groups[group_name] = [(addon, count) for addon, count in addons_groups[group_name] if addon in all_addons_ref.union(all_addons_groups[group_name])]

        save_results(addons_ref, addons_groups)

        print('[DONE ' + str(time.time() - t) + ']\n')


    # TODO: Once https://bugzilla.mozilla.org/show_bug.cgi?id=1311647 is fixed, stop using SuperSearch and use the Telemetry data for everything.
    # Count modules.
    all_modules = set()
    total_reference_telemetry = 0
    total_groups_telemetry = dict()
    if telemetry_dataset is not None:
        print('Counting modules...')
        t = time.time()

        if signatures is not None:
            found_modules = telemetry_dataset.select(['signature', 'uuid'] + [functions.explode(telemetry_dataset['json_dump']['modules']['filename']).alias('module')]).dropDuplicates(['uuid', 'module']).select(['signature', 'module']).rdd.flatMap(lambda v: [(v, 1), (v['module'], 1)] if v['signature'] in signatures else [(v['module'], 1)]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

            modules_ref = [module for module in found_modules if not isinstance(module[0], Row)]
            modules_signatures = [module for module in found_modules if isinstance(module[0], Row)]
            modules_groups = dict([(signature, [(module, count) for (s, module), count in modules_signatures if s == signature]) for signature in signatures])
        else:
            modules_ref = telemetry_dataset.select(functions.explode(telemetry_dataset['json_dump']['modules']['filename']).alias('module')).rdd.map(lambda v: (v['module'], 1)).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

            modules_groups = dict([(group[0], group[1].select(functions.explode(group[1]['json_dump']['modules']['filename']).alias('module')).rdd.map(lambda v: (v['module'], 1)).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()) for group in groups])

        total_reference_telemetry = telemetry_dataset.count()
        total_groups_telemetry = dict(telemetry_dataset.select('signature').filter(telemetry_dataset['signature'].isin(signatures)).groupBy('signature').count().rdd.map(lambda p: (p['signature'], p['count'])).collect())

        modules_ref = [(module, count * total_reference / total_reference_telemetry) for module, count in modules_ref]
        for group_name in group_names:
            modules_groups[group_name] = [(module, count * total_groups[group_name] / total_groups_telemetry[group_name]) for module, count in modules_groups[group_name]]

        all_modules_groups = dict([(group_name, set([module for module, count in modules_groups[group_name] if float(count) / total_groups[group_name] > min_support_diff * 2])) for group_name in group_names])
        all_modules = set.union(*all_modules_groups.values())

        modules_ref = [(module, count) for module, count in modules_ref if module in all_modules]
        for group_name in group_names:
            modules_groups[group_name] = [(module, count) for module, count in modules_groups[group_name] if module in set.union(all_modules_groups[group_name])]

        save_results(modules_ref, modules_groups)

        print('[DONE ' + str(time.time() - t) + ']\n')

    # TODO: Remove once we only use data from Telemetry.
    all_modules_as_columns = [module.replace('.', '__DOT__') for module in all_modules]


    priors_graph = {
        'platform': ['platform_pretty_version'],
        'platform_pretty_version': ['platform_version'],
        'platform_version': list(all_modules),
        'adapter_vendor_id': ['adapter_device_id'],
        'adapter_device_id': ['adapter_driver_version', 'adapter_driver_version_clean'],
        'adapter_driver_version': list(all_app_notes) + list(all_gfx_critical_errors),
        'CPU Info': ['cpu_microcode_version'],
    }

    for addon in all_addons:
        priors_graph[addon] = [addon + '-version']

    def find_path(start, end, path=[]):
        start = start.replace('.', '__DOT__')
        end = end.replace('.', '__DOT__')

        path = path + [start]

        if start == end:
            return path

        if start not in priors_graph:
            return None

        for node in priors_graph[start]:
            if node.replace('.', '__DOT__') in path:
                continue

            newpath = find_path(node, end, path)
            if newpath:
                return newpath

        return None


    def augment(df):
        if 'addons' in df.columns:
            '''def get_version(addons, addon):
                if addons is None:
                    return None

                for i in range(0, len(addons), 2):
                    if addons[i] == addon:
                        return addons[i + 1]

                return 'Not installed'

            def create_get_version_udf(addon):
                return functions.udf(lambda addons: get_version(addons, addon), StringType())

            df = df.select(['*'] + [functions.array_contains(df['addons'], addon).alias(addon.replace('.', '__DOT__')) for addon in all_addons] + [create_get_version_udf(addon)(df['addons']).alias(addon.replace('.', '__DOT__') + '-version') for addon in all_addons])'''
            df = df.select(['*'] + [functions.array_contains(df['addons'], addon).alias(addon.replace('.', '__DOT__')) for addon in all_addons])

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

        if 'cpu_info' in df.columns:
            df = df.withColumn('CPU Info', functions.substring_index(df['cpu_info'], ' | ', 1))
            df = df.withColumn('Is Multicore', functions.substring_index(df['cpu_info'], ' | ', -1) != '1')

        return df


    def drop_unneeded(df):
        return df.select([c for c in df.columns if c not in [
            'total_virtual_memory', 'total_physical_memory', 'available_virtual_memory', 'available_physical_memory', 'oom_allocation_size',
            'app_notes',
            'graphics_critical_error',
            'addons',
            'date',
            'cpu_info',
            'user_comments',
            'uuid',
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
        given_parent1_support_reference = count_reference / parent1_count_reference
        given_parent1_support_group = count_group / parent1_count_group
        given_parent2_support_reference = count_reference / parent2_count_reference
        given_parent2_support_group = count_group / parent2_count_group

        # TODO: Add fixed relations pruning.

        # If there's no large change in the support of a set when extending the set, prune the node.
        # TODO: Consider using a ratio instead of a threshold.
        threshold = min(0.05, min_support_diff / 2)

        if (abs(parent1_support_reference - given_parent1_support_reference) > threshold or abs(parent1_support_group - given_parent1_support_group) > threshold) and (abs(parent2_support_reference - given_parent2_support_reference) > threshold or abs(parent2_support_group - given_parent2_support_group) > threshold):
            return False

        if (abs(parent1_support_reference - support_reference) < threshold and abs(parent1_support_group - support_group) < threshold) or (abs(parent2_support_reference - support_reference) < threshold and abs(parent2_support_group - support_group) < threshold):
            return True

        # If there's no significative change, prune the node.
        chi2, p1, dof, expected = scipy.stats.chi2_contingency([[parent1_count_group, count_group], [parent1_count_reference, count_reference]])
        chi2, p2, dof, expected = scipy.stats.chi2_contingency([[parent2_count_group, count_group], [parent2_count_reference, count_reference]])
        if p1 > 0.5 and p2 > 0.5:
            return True

        return False

    def count_candidates(candidates, telemetry_candidates):
        broadcastAllCandidates = sc.broadcast(set.union(*candidates.values()))
        broadcastAllTelemetryCandidates = sc.broadcast(set.union(*telemetry_candidates.values()))
        if signatures is not None:
            broadcastCandidatesMap = sc.broadcast(candidates)
            results = dfReference.rdd.map(lambda p: (p['signature'], set(p.asDict().iteritems()))).flatMap(lambda p: [(fset, 1) for fset in broadcastAllCandidates.value if fset <= p[1]] + ([] if p[0] not in broadcastSignatures.value else [((p[0], fset), 1) for fset in broadcastCandidatesMap.value[p[0]] if fset <= p[1]])).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

            results_ref = [r for r in results if isinstance(r[0], frozenset)]
            results_groups = dict([(signature, [(r[0][1], r[1]) for r in results if not isinstance(r[0], frozenset) and r[0][0] == signature]) for signature in signatures])

            if telemetry_dataset:
                broadcastTelemetryCandidatesMap = sc.broadcast(telemetry_candidates)
                results = telemetry_dataset.select(['signature', 'platform', 'platform_pretty_version', 'platform_version'] + [functions.array_contains(telemetry_dataset['json_dump']['modules']['filename'], module).alias(module.replace('.', '__DOT__')) for module in all_modules]).rdd.map(lambda p: (p['signature'], set(p.asDict().iteritems()))).flatMap(lambda p: [(fset, 1) for fset in broadcastAllTelemetryCandidates.value if fset <= p[1]] + ([] if p[0] not in broadcastSignatures.value else [((p[0], fset), 1) for fset in broadcastTelemetryCandidatesMap.value[p[0]] if fset <= p[1]])).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

                results_telemetry_ref = [r for r in results if isinstance(r[0], frozenset)]
                results_telemetry_groups = dict([(signature, [(r[0][1], r[1]) for r in results if not isinstance(r[0], frozenset) and r[0][0] == signature]) for signature in signatures])

                results_telemetry_ref = [(r, count * total_reference / total_reference_telemetry) for r, count in results_telemetry_ref]
                for group_name in group_names:
                    results_telemetry_groups[group_name] = [(r, count * total_groups[group_name] / total_groups_telemetry[group_name]) for r, count in results_telemetry_groups[group_name]]

                results_ref += results_telemetry_ref
                results_groups.update(results_telemetry_groups)
        else:
            results_ref = dfReference.rdd.map(lambda p: (p['signature'], set(p.asDict().iteritems()))).flatMap(lambda p: [(fset, 1) for fset in broadcastAllCandidates.value if fset <= p[1]]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()
            results_groups = []
            for group in groups:
                broadcastCandidates = sc.broadcast(candidates[group[0]])
                results_groups.append((group[0], group[1].rdd.map(lambda p: (p['signature'], set(p.asDict().iteritems()))).flatMap(lambda p: [(fset, 1) for fset in broadcastCandidates.value if fset <= p[1]]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()))
            results_groups = dict(results_groups)

        save_results(results_ref, results_groups)

        return results_groups


    def generate_candidates(previous_candidates, level):
        candidates = {}
        telemetry_candidates = {}
        parents = {}

        print('Generating level-' + str(level) + ' candidates...')
        t = time.time()

        for group_name in group_names:
            candidates[group_name] = set()
            telemetry_candidates[group_name] = set()
            parents[group_name] = {}

            for i in range(0, len(previous_candidates[group_name])):
                for j in range(i + 1, len(previous_candidates[group_name])):
                    props = union(previous_candidates[group_name][i], previous_candidates[group_name][j])
                    if len(props) == level and props not in candidates[group_name]:
                        # TODO: Remove once we only use data from Telemetry.
                        if list(previous_candidates[group_name][i])[0][0] in all_modules_as_columns or list(previous_candidates[group_name][j])[0][0] in all_modules_as_columns:
                            if list(previous_candidates[group_name][i])[0][0] in ['platform', 'platform_pretty_version', 'platform_version'] or list(previous_candidates[group_name][j])[0][0] in ['platform', 'platform_pretty_version', 'platform_version']:
                                telemetry_candidates[group_name].add(props)
                            else:
                                continue
                        else:
                            candidates[group_name].add(props)
                        parents[group_name][props] = (previous_candidates[group_name][i], previous_candidates[group_name][j])

        print('[DONE ' + str(time.time() - t) + ']\n')
        print(str(level) + ' CANDIDATES: ' + str(sum(len(candidates[group_name]) for group_name in group_names)))

        print('Counting level-' + str(level) + ' candidates...')
        t = time.time()
        results_groups = count_candidates(candidates, telemetry_candidates)
        print('[DONE ' + str(time.time() - t) + ']\n')

        print('Filtering level-' + str(level) + ' candidates...')
        t = time.time()
        filtered_candidates = dict([(group_name, list(set([result[0] for result in results_groups[group_name] if not should_prune(group_name, parents[group_name][result[0]][0], parents[group_name][result[0]][1], result[0])]))) for group_name in group_names])
        print('[DONE ' + str(time.time() - t) + ']\n')

        return filtered_candidates

    candidates = {
      1: dict([(group_name, []) for group_name in group_names])
    }

    # Generate first level candidates.
    print('Counting first level candidates...')
    t = time.time()
    results_ref = get_first_level_results('reference', dfReference.columns + all_modules_as_columns)
    results_groups = dict([(group_name, get_first_level_results(group_name, dfReference.columns + all_modules_as_columns)) for group_name in group_names])
    columns = [c for c in dfReference.columns if c not in get_columns('reference', dfReference.columns) and c != 'signature']
    broadcastColumns = sc.broadcast(columns)
    if signatures is not None:
        results = dfReference.rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastColumns.value] + ([] if p['signature'] not in broadcastSignatures.value else [((p['signature'], frozenset([(key,p[key])])), 1) for key in broadcastColumns.value])).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

        results_ref += [r for r in results if isinstance(r[0], frozenset)]
        for group_name in group_names:
            results_groups[group_name] += [(r[0][1], r[1]) for r in results if not isinstance(r[0], frozenset) and r[0][0] == group_name]
    else:
        results_ref += dfReference.rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastColumns.value]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()
        for group in groups:
            results_groups[group[0]] += group[1].rdd.flatMap(lambda p: [(frozenset([(key,p[key])]), 1) for key in broadcastColumns.value]).reduceByKey(lambda x, y: x + y).filter(lambda (k, v): v >= MIN_COUNT).collect()

    save_results(results_ref, results_groups)
    print('[DONE ' + str(time.time() - t) + ']\n')


    # Filter first level candidates.
    print('Filtering first level candidates...')
    t = time.time()
    for group_name in group_names:
        candidates[1][group_name] = set([element for element, count in results_groups[group_name] if not should_prune(group_name, None, None, element)])


    # Remove useless rules (e.g. addon_X=True and addon_X=False or is_garbage_collecting=1 and is_garbage_collecting=None).
    # TODO: Remove "app_note+" when we have "app_note-"?
    def ignore_rule(candidate, candidates, group_name):
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

        # Ignore addon version...
        if elem_key.endswith('-version') and elem_key.replace('__DOT__', '.')[:-8] in all_addons:
            # ... when unavailable or...
            if elem_val == None or elem_val == 'Not installed':
                return True
            # ... when it's not adding new information compared to addon presence.
            if frozenset([(elem_key[:-8], True)]) in candidates and abs(get_count(candidate, group_name) / total_groups[group_name] - get_count(frozenset([(elem_key[:-8], True)]), group_name) / total_groups[group_name]) <= 0.01:
                return True

        return False

    for group_name in group_names:
        candidates[1][group_name] = [c for c in candidates[1][group_name] if not ignore_rule(c, candidates[1][group_name], group_name)]

    print('[DONE ' + str(time.time() - t) + ']\n')
    print('1 RULES: ' + str(sum(len(candidates[1][group_name]) for group_name in group_names)))


    l = 1
    while sum(len(candidates[l][group_name]) for group_name in group_names) > 0 and l < 2:
        l += 1
        candidates[l] = generate_candidates(candidates[l - 1], l)
        print(str(l) + ' RULES: ' + str(sum(len(candidates[l][group_name]) for group_name in group_names)))


    all_candidates = dict([(group_name, sum([candidates[i][group_name] for i in range(1,l+1)], [])) for group_name in group_names])


    def clean_candidate(candidate):
        transformed_candidate = dict(candidate)
        dict_candidate = transformed_candidate.copy()
        for key, val in candidate:
            clean_key = key.replace('__DOT__', '.')
            if clean_key in all_addons:
                dict_candidate['Addon "' + (addons.get_addon_name(clean_key) or clean_key) + '"'] = val
                del dict_candidate[key]
            elif clean_key.endswith('-version') and clean_key[:-8] in all_addons:
                dict_candidate['Addon "' + (addons.get_addon_name(clean_key[:-8]) or clean_key[:-8]) + '" Version'] = val
                del dict_candidate[key]
            elif clean_key in all_modules:
                dict_candidate['Module "' + clean_key + '"'] = val
                del dict_candidate[key]
            elif key in all_gfx_critical_errors:
                dict_candidate['GFX_ERROR "' + clean_key + '"'] = val
                del dict_candidate[key]
            elif key in all_app_notes:
                dict_candidate['"' + clean_key + '" in app_notes'] = val
                del dict_candidate[key]
            elif key == 'adapter_vendor_id':
                dict_candidate[key] = pciids.get_vendor_name(val)
            elif key == 'adapter_device_id' and 'adapter_vendor_id' in transformed_candidate:
                dict_candidate[key] = pciids.get_device_name(transformed_candidate['adapter_vendor_id'], val)
        return dict_candidate

    print('Final rules filtering...')
    t = time.time()
    alpha = 0.05
    alpha_k = alpha
    results = {}
    for group_name in group_names:
        results[group_name] = {}

        total_group = total_groups[group_name]

        for candidate in all_candidates[group_name]:
            count_reference = get_count(candidate, 'reference')
            count_group = get_count(candidate, group_name)
            support_reference = count_reference / total_reference
            support_group = count_group / total_group

            if len(candidate) > 1:
                elems = [frozenset([item]) for item in candidate]

                found_priors = []
                for prior in elems:
                    can_be_prior = True
                    for elem in elems:
                        if prior == elem:
                            continue

                        can_be_prior &= find_path(list(prior)[0][0], list(elem)[0][0]) is not None

                    if can_be_prior:
                        found_priors.append(prior)

                got_prior = False
                for found_prior in found_priors:
                    others = frozenset.union(*[elem for elem in elems if elem != found_prior])
                    if others in results[group_name]:
                        # Check if with this prior the support difference is different than without the prior.
                        count_prior_group = get_count(found_prior, group_name)
                        count_prior_reference = get_count(found_prior, 'reference')
                        others_support_group = get_count(others, group_name) / total_group
                        others_support_reference = get_count(others, 'reference') / total_reference
                        support_group_given_prior = count_group / count_prior_group
                        support_reference_given_prior = count_reference / count_prior_reference

                        threshold = min(0.05, min_support_diff / 2)
                        if abs(others_support_group - support_group_given_prior) > threshold or abs(others_support_reference - support_reference_given_prior) > threshold:
                            if results[group_name][others]['prior'] is None or results[group_name][others]['prior']['total_reference'] > count_prior_reference:
                                if list(others)[0][0] in all_modules_as_columns:
                                    if count_group > count_prior_group:
                                        count_prior_group = count_group
                                    if count_reference > count_prior_reference:
                                        count_prior_reference = count_reference

                                results[group_name][others]['prior'] = {
                                    'item': clean_candidate(found_prior),
                                    'count_reference': count_reference,
                                    'count_group': count_group,
                                    'total_reference': count_prior_reference,
                                    'total_group': count_prior_group,
                                }
                            got_prior = True

                if got_prior:
                    continue

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

            # XXX: Debugging.
            if total_group < count_group or total_reference < count_reference:
                print(candidate)
                print(group_name)
                print(count_group)
                print(total_group)
                print(count_reference)
                print(total_reference)

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

            results[group_name][candidate] = {
                'item': clean_candidate(candidate),
                'count_reference': count_reference,
                'count_group': count_group,
                'prior': None,
            }

    results = dict([(group_name, list(results[group_name].values())) for group_name in group_names])

    print('[DONE ' + str(time.time() - t) + ']\n')

    return results, total_reference, total_groups


def print_results(results, total_reference, total_groups, reference_name='overall'):
    def to_percentage(num):
        result = "%.2f" % (num * 100)

        if result == '100.00':
            return '100.0'

        if len(result[0:result.index('.')]) == 1:
            return '0' + result

        return result

    def item_to_label(rule):
        return u' ∧ '.join([key + '="' + str(value) + '"' for key, value in rule.items()]).encode('utf-8')

    def print_all(results, group_name):
        for result in results:
            print('(' + to_percentage(result['count_group'] / total_groups[group]) + '% in ' + group_name + ' vs ' + to_percentage(result['count_reference'] / total_reference) + '% ' + reference_name + ') ' + item_to_label(result['item']) + ('' if result['prior'] is None else ' [' + to_percentage(result['prior']['count_group'] / result['prior']['total_group']) + '% vs ' + to_percentage(result['prior']['count_reference'] / result['prior']['total_reference']) + '% given ' + item_to_label(result['prior']['item']) + ']'))

    for group in results.keys():
        print(group)

        len1 = [result for result in results[group] if len(result['item']) == 1]
        others = [result for result in results[group] if len(result['item']) > 1]

        print_all(sorted(len1, key=lambda v: (-abs(v['count_reference'] / total_reference - v['count_group'] / total_groups[group]))), group)

        print('\n\n')

        print_all(sorted(others, key=lambda v: (-round(abs(v['count_reference'] / total_reference - v['count_group'] / total_groups[group]), 2), len(v['item']))), group)

        print('\n\n\n')
