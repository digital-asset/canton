# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from collections import defaultdict
import sys
import re
from pathlib import Path
import storage
import textwrap


class JsonToRstConverter:
    """
    Class that contains the code which takes the reference JSON and then generates the RST to be displayed.
    """

    def __init__(self):
        assembly_website_unidoc_root = '../scaladoc/'
        website_unidoc_root = assembly_website_unidoc_root
        ci_unidoc_root = '/tmp/workspace/docs/scaladoc/'
        local_unidoc_root = f'{os.path.dirname(os.path.realpath(__file__))}/../../../../target/scala-2.13/unidoc/'
        self.unidoc_root_to_test = ci_unidoc_root if os.environ.get('CI') \
            else local_unidoc_root
        self.unidoc_root = \
            website_unidoc_root if os.environ.get('CI') \
            else local_unidoc_root

    def process_error_code_data(self, data):
        def format_single_error(item):

            if item['className'].startswith('com.daml.error.definitions'):
                url = ''
            else:
                url = build_and_test_scaladoc_url(item['className'])

            link = Formats.create_cross_referencing_label(item['code'], '_canton_error_codes')
            title_line = f"{item['code']}"
            res = ['', link, '', title_line, '-' * len(title_line)]

            def as_bullet_point(a_res, a_name):
                if item[a_name]:
                    text = Formats.properly_space(item[a_name])
                    a_res.append(f'* **{a_name.title()}**: {text}')

            for name in ['explanation', 'resolution', 'category', 'conveyance']:
                as_bullet_point(res, name)
            if url != '':
                res.append(f"* **Scaladocs**: `{item['code']} <{url}>`_")
            # appending empty string here to have line break between next error
            res.append('')
            return res

        def build_and_test_scaladoc_url(class_name):
            possible_type_paths = generate_possible_type_paths(class_name)
            type_path = [type_path for type_path in possible_type_paths if
                         os.path.exists(self.unidoc_root_to_test + type_path + '.html')]
            # comment out to also check for valid scaladocs links locally
            if os.environ.get('CI'):
                if len(type_path) != 1:
                    raise Exception(
                        f"Couldn't generate the URL to link to the scaladoc of error code '{class_name}'. "
                        "If you're seeing this error locally, ensure the environment variable 'CI' is unset.")
                else:
                    type_path = type_path[0]
                url = f'{self.unidoc_root + type_path}.html'
                return url
            else:
                return ''

        def generate_possible_type_paths(type_string):
            type_path = type_string.replace('.', '/')

            # example input: a$b$
            # example output: ['a$b$', 'a$$b$', 'a$b$$', 'a$$b$$']
            # Generates all 2^n combinations of using in $ or $$ for all occurrences of $ in a string
            # Needed to 'brute-force' the correct scaladoc url for error codes
            # because e.g. the type_string that we get through scala reflection is:
            # TransactionRoutingError$$TopologyErrors$$NotConnectedToAllContractSynchronizers$
            # but the name of the corresponding scaladoc `.html`-file that we need is:
            # TransactionRoutingError$$TopologyErrors$$NotConnectedToAllContractSynchronizers$
            # I found no way to deterministically construct the correct html-file name we need -> brute-force approach
            # An exception will be thrown if this brute-force approach ever fails
            def generate_possible_urls(a_type_string):
                prefixes = [[]]
                for c in a_type_string:
                    if c == '$':
                        prefixes_with_dollar = [prefix + ['$'] for prefix in prefixes]
                        prefixes.extend(prefixes_with_dollar)
                    [prefix.append(c) for prefix in prefixes]
                return [''.join(pre) for pre in prefixes]

            return generate_possible_urls(type_path)

        # A node of this tree is a dict that can contain
        #   1. further nodes and/or
        #   2. 'leaves' in the form of a list of error (code) data
        # Thus, the resulting tree is very similar to a trie

        def create_node():
            return defaultdict(create_node)

        def build_hierarchical_tree_of_error_data(some_data):
            a_root: dict[str, dict | list[str]] = defaultdict(create_node)
            for error_data in some_data:
                current = a_root
                for group in error_data['hierarchicalGrouping']:
                    current = current[group]
                if 'error-codes' in current:
                    current['error-codes'].append(error_data)
                else:
                    current['error-codes'] = [error_data]
            return a_root

        # DFS to traverse the error code data tree from `build_hierarchical_tree_of_error_data`
        # While traversing the tree, the presentation of the error codes on the documentation is built
        def dfs(a_tree, numbering, an_error_representation):
            if 'error-codes' in a_tree:
                for code in a_tree['error-codes']:
                    an_error_representation.extend(format_single_error(code))
            i: int = 1
            for (subtopic, subtree) in a_tree.items():
                if subtopic == 'error-codes':
                    continue

                sub_numbering = f'{numbering}{i}.'
                i += 1
                heading = f'{sub_numbering} {subtopic}'

                an_error_representation.append('')
                an_error_representation.append(Formats.create_cross_referencing_label(heading, '_canton_error_codes'))
                an_error_representation.append('')
                an_error_representation.append(heading)
                an_error_representation.append('=' * len(heading))
                an_error_representation.append('')

                dfs(subtree, sub_numbering, an_error_representation)

        tree = build_hierarchical_tree_of_error_data(data.values())
        error_representation = []
        dfs(tree, '', error_representation)

        return '\n'.join(error_representation)

    def process_console_command_data(self, data):
        # map from console command name -> # of times we have seen it
        # We need this because some commands are documented multiple times in different sections and for our anchor
        # links we need a way to differentiate them
        # First anchor link of a command will be e.g. `https://.../console.html#certs-generate`
        # Second anchor link of a command will be e.g. `https://.../console.html#certs-generate-2`
        seen = dict()

        def process_command(a_command_data):
            # If return type is a Canton type, create hyperlink to its scaladocs-page
            # This function could be made arbitrarily complex/clever -> but this already covers 95% of cases
            # A type_string is for example `com.digitalasset.canton.SynchronizerId`
            def attempt_to_link_to_scaladocs(type_string):
                # don't try to link to scaladocs when building locally
                if not os.environ.get('CI'):
                    return type_string
                # A 'nested' type is e.g. Option[Boolean]
                nested_type = re.search(r'\[(.*)]', type_string)
                # grab type inside the '[...]' if it is nested
                type_string_pre = type_string if not nested_type else nested_type.group(1)

                # don't link return type if not a canton-type or nested more than once
                if not type_string_pre.startswith('com.digitalasset.canton') \
                        or '[' in type_string_pre \
                        or ',' in type_string_pre:
                    return type_string

                type_path = build_type_path(type_string_pre)
                if not os.path.exists(f'{self.unidoc_root_to_test}{type_path}.html'):
                    # Add the manual special cases further below to `is_special_case_type`
                    # and `generate_special_case_type_url`
                    print(type_path + ' does not exist in the build unidoc. fix me by adding a manual special case',
                          file=sys.stderr)
                    return type_string
                rst_link = f'`{type_string} <{self.unidoc_root}{type_path}.html>`_'
                return rst_link

            def build_type_path(type_string):
                type_path = type_string.replace('.', '/').replace('*', '')
                if is_special_case_type(type_path):
                    return generate_special_case_type_url(type_path)
                else:
                    return type_path

            def is_special_case_type(type_path):
                special_cases = ['ListShareRequestsResponse', 'ListShareOffersResponse', 'ContractData', 'VersionInfo',
                                 'LfContractId', '=>', 'WrappedCreatedEvent', 'SequencerConnectionConfigs',
                                 'SequencerCounter',
                                 'PossiblyIgnoredProtocolEvent', 'IdentityProviderConfig', 'TopologyChangeOp/Positive',
                                 'LfPartyId', 'PositiveInt', 'NonNegativeInt']
                return any([s in type_path for s in special_cases])

            # Add manual special cases here
            def generate_special_case_type_url(type_path):
                if 'ListShareRequestsResponse' in type_path or 'ListShareOffersResponse' in type_path:
                    # actual: canton/participant/admin/v0/ListShareRequestsResponse/Item.html
                    # should: canton/participant/admin/v0/ListShareRequestsResponse.html
                    return type_path.replace('/Item', '')
                elif 'ContractData' in type_path:
                    # actual: canton/admin/api/client/commands/LedgerApiTypeWrappers/ContractData.html
                    # should: canton/admin/api/client/commands/LedgerApiTypeWrappers$$ContractData.html
                    return type_path.replace('/ContractData', '$$ContractData')
                elif 'VersionInfo' in type_path:
                    # actual: canton/buildinfo/Info/VersionInfo.html
                    # should: canton/buildinfo/Info$$VersionInfo
                    return type_path.replace('/VersionInfo', '$$VersionInfo')
                elif 'LfContractId' in type_path:
                    # special cases of LfContractId, LfContractId* and Map[...LfContractId,String]
                    # should: canton/protocol/index.html
                    idx = type_path.find('LfContractId')
                    return type_path[:idx] + 'index'
                elif 'SequencerConnectionConfigs' in type_path:
                    # actual: canton/sequencing/SequencerConnectionConfigs.html
                    # should: canton/buildinfo/index.html
                    return type_path.replace('SequencerConnectionConfigs', 'index')
                elif '=>' in type_path:
                    if 'WrappedCreatedEvent' in type_path:  # this is a 'double' special case
                        # actual: canton/admin/api/client/commands/LedgerApiTypeWrappers/WrappedCreatedEvent
                        # => Boolean.html
                        # should: canton/admin/api/client/commands/LedgerApiTypeWrappers$$WrappedCreatedEvent.html
                        return type_path.replace('/WrappedCreatedEvent => Boolean', '$$WrappedCreatedEvent')
                    else:
                        # case of 'normal' function arrow
                        return type_path[:type_path.find(' => ')]
                elif 'WrappedCreatedEvent' in type_path:
                    # actual: canton/admin/api/client/commands/LedgerApiTypeWrappers/WrappedCreatedEvent.html
                    # should: canton/admin/api/client/commands/LedgerApiTypeWrappers$$WrappedCreatedEvent.html
                    return type_path.replace('/WrappedCreatedEvent', '$$WrappedCreatedEvent')
                elif 'SequencerCounter' in type_path:
                    # actual: canton/SequencerCounter.html
                    # should: canton/index.html
                    idx = type_path.find('SequencerCounter')
                    return type_path[:idx] + 'index'
                elif 'PossiblyIgnoredProtocolEvent' in type_path:
                    # actual: com/digitalasset/canton/sequencing/PossiblyIgnoredProtocolEvent
                    # should: com/digitalasset/canton/sequencing/index.html
                    idx = type_path.find('PossiblyIgnoredProtocolEvent')
                    return type_path[:idx] + 'index'
                elif 'IdentityProviderConfig' in type_path:
                    # actual: com/digitalasset/canton/ledger/api/IdentityProviderConfig
                    # should: canton/index.html
                    return type_path.replace('IdentityProviderConfig', 'index')
                elif 'TopologyChangeOp/Positive' in type_path:
                    # actual: com/digitalasset/canton/topology/transaction/TopologyChangeOp/Positive.html
                    # should: com/digitalasset/canton/topology/transaction/TopologyChangeOp$$Positive.html
                    return type_path.replace('/Positive', '$$Positive')
                elif 'LfPartyId' in type_path:
                    # actual: com/digitalasset/canton/LfPartyId.html
                    # should: com/digitalasset/canton/index.html
                    return type_path.replace('/canton/LfPartyId', '/canton/index')
                elif 'PositiveInt' in type_path:
                    # actual: com/digitalasset/canton/config/RequireTypes/PositiveInt.html
                    # should: com/digitalasset/canton/config/RequireTypes$$PositiveInt$.html
                    return type_path.replace('/RequireTypes/PositiveInt', '/RequireTypes$$PositiveInt$')
                elif 'PositiveDouble' in type_path:
                    # actual: com/digitalasset/canton/config/RequireTypes/PositiveDouble.html
                    # should: com/digitalasset/canton/config/RequireTypes$$PositiveDouble$.html
                    return type_path.replace('/RequireTypes/PositiveDouble', '/RequireTypes$$PositiveDouble$')
                elif 'NonNegativeInt' in type_path:
                    # actual: com/digitalasset/canton/config/RequireTypes/NonNegativeInt.html
                    # should: com/digitalasset/canton/config/RequireTypes$$NonNegativeInt$.html
                    return type_path.replace('/RequireTypes/NonNegativeInt', '/RequireTypes$$NonNegativeInt$')
                else:
                    raise Exception('Unable to match type in special case handling for type_path ', type_path)

            name = a_command_data['name']
            if name in seen:
                seen[name] += 1
                section_name = f'{name}_{seen[name]}'
            else:
                seen[name] = 0
                section_name = name
            a_res = [Formats.create_cross_referencing_label(section_name), '', '']

            title_line = \
                f":ref:`{a_command_data['name']}" \
                f"{' (' + a_command_data['scope'] + ')' if a_command_data['scope'] != 'Stable' else ''} " \
                f"<{section_name}>`"
            a_res.append(title_line)
            a_res.append(Formats.as_bullet_point('Summary', a_command_data['summary']))
            if a_command_data['arguments']:
                a_res.append(Formats.as_bullet_point('Arguments', ''))
                for (name, type_) in a_command_data['arguments']:
                    a_res.append(f'\t\t* ``{name}``: {attempt_to_link_to_scaladocs(type_)}')
            if a_command_data['return_type']:
                a_res.append(Formats.as_bullet_point('Return type', ''))
                a_res.append(f"\t\t* {attempt_to_link_to_scaladocs(a_command_data['return_type'])}")
            if a_command_data['description']:
                a_res.append(
                    Formats.as_desc('Description', a_command_data['description']))
            a_res.append('')

            return a_res

        # map: topics -> command representation
        res = defaultdict(list)
        # sort by name to list commands in alphabetical order
        sorted_commands = sorted(data, key=lambda a_command_data: a_command_data['name'])
        for command_data in sorted_commands:
            key = ', '.join(command_data['topic'])
            res[key].append('\n'.join(process_command(command_data)))
        res = {k: '\n'.join(v) for (k, v) in res.items()}
        return res


class Formats:

    @staticmethod
    def desc_block(content):
        dedent_text = textwrap.dedent(content)
        block = textwrap.indent(dedent_text, "            ")
        return f'\n        .. code-block:: none\n\n{block}\n'

    @staticmethod
    def as_desc(name, desc):
        content = Formats.desc_block(desc)
        return f'\t* **{name}**:\n{content}'

    @staticmethod
    def properly_space(text):
        """
        We want to remove all line breaks in descriptions etc. because they confuse sphinx, but we want to have exactly
        one space after each instance where we previously had a line break.
        """
        return text.replace('\n', ' ').replace('  ', ' ')

    @staticmethod
    def as_bullet_point(name, desc):
        return f'\t* **{name}**: {desc}'

    @staticmethod
    def create_cross_referencing_label(label: str, label_suffix: str = '') -> str:
        return f'.. _{label.lower()}{label_suffix.lower()}:'


VERSIONING_TABLE_INCLUDE_FILE_NAME = 'versioning.rst.inc'
TOPOLOGY_VERSIONING_TABLE_INCLUDE_FILE_NAME = 'topology_versioning.rst.inc'
MONITORING_SEQ_METRICS_INCLUDE_FILE_NAME = 'monitoring.rst.sequencer_metrics.inc'
MONITORING_MED_METRICS_INCLUDE_FILE_NAME = 'monitoring.rst.mediator_metrics.inc'
MONITORING_PAR_METRICS_INCLUDE_FILE_NAME = 'monitoring.rst.participant_metrics.inc'
ERROR_CODES_INCLUDE_FILE_NAME = 'error_codes.rst.inc'
CONSOLE_COMMANDS_INCLUDE_FILE_NAME = 'console.rst.inc'

INC_TO_RST = {
    VERSIONING_TABLE_INCLUDE_FILE_NAME: 'participant/reference/versioning.rst',
    TOPOLOGY_VERSIONING_TABLE_INCLUDE_FILE_NAME: 'sdk/tutorials/app-dev/external_signing_topology_transaction.rst',
    MONITORING_SEQ_METRICS_INCLUDE_FILE_NAME: 'participant/howtos/observe/monitoring.rst',
    MONITORING_MED_METRICS_INCLUDE_FILE_NAME: 'participant/howtos/observe/monitoring.rst',
    MONITORING_PAR_METRICS_INCLUDE_FILE_NAME: 'participant/howtos/observe/monitoring.rst',
    ERROR_CODES_INCLUDE_FILE_NAME: 'participant/reference/error_codes.rst',
    CONSOLE_COMMANDS_INCLUDE_FILE_NAME: 'participant/reference/console.rst',
}


def generate(converter: JsonToRstConverter, reference_data_json: Path, target_out: Path) -> None:
    generated_dir = target_out / 'generated'

    reference_data = storage.load_json(reference_data_json)

    generate_versioning_table(generated_dir, reference_data)
    generate_topology_transaction_versioning_table(generated_dir, reference_data)
    generate_metrics(generated_dir, reference_data)
    generate_error_codes(generated_dir, reference_data, converter)
    generate_console(generated_dir, reference_data, converter)


def generate_versioning_table(a_generated_dir: Path, loaded_json) -> None:
    rv_to_pv_data = {release: pvs for (release, pvs) in loaded_json['releaseVersionsToProtocolVersions']}
    rv_to_pv = _build_versioning_table(rv_to_pv_data)
    storage.write(a_generated_dir / VERSIONING_TABLE_INCLUDE_FILE_NAME, rv_to_pv)


def generate_topology_transaction_versioning_table(a_generated_dir: Path, loaded_json) -> None:
    pv_to_protobuf_data = {pv: protobuf_version for (pv, protobuf_version) in loaded_json['topologyTransactionProtocolVersionToProtobufVersions']}
    pv_to_protobuf = _build_versioning_table(pv_to_protobuf_data, "Protocol Version", "Topology Transaction Protobuf Version")
    storage.write(a_generated_dir / TOPOLOGY_VERSIONING_TABLE_INCLUDE_FILE_NAME, pv_to_protobuf)


def _build_versioning_table(versioning_data, release_name='Release', version_name='Protocol versions'):
    lines = ['.. list-table::', '   :widths: 10 15',
             '   :header-rows: 1', '   :align: center', '', f'   * - {release_name}',
             f'     - {version_name}']
    for (release, versions) in versioning_data.items():
        lines.append(f'   * - {release}')
        lines.append(f"     - {', '.join(versions)}")
    return '\n'.join(lines)


def generate_metrics(a_generated_dir: Path, loaded_json) -> None:
    metrics_data = loaded_json['metrics']
    metrics = _process_metric_data(metrics_data)
    storage.write(a_generated_dir / MONITORING_SEQ_METRICS_INCLUDE_FILE_NAME, metrics['sequencer'])
    storage.write(a_generated_dir / MONITORING_MED_METRICS_INCLUDE_FILE_NAME, metrics['mediator'])
    storage.write(a_generated_dir / MONITORING_PAR_METRICS_INCLUDE_FILE_NAME, metrics['participant'])


def _process_metric_data(data):
    def process_metric(metric_data):
        labels = metric_data['labels']
        title_suffix = ''
        if labels:
            title_suffix = '*'  # if the metric is labeled then it's a prometheus only metric
        title_line = f"{metric_data['name']}{title_suffix}"
        res = [title_line, '^' * len(title_line),
               Formats.as_bullet_point('Summary', metric_data['summary']),
               Formats.as_bullet_point('Description', Formats.properly_space(metric_data['description'])),
               Formats.as_bullet_point('Type', metric_data['type']),
               Formats.as_bullet_point('Qualification', metric_data['qualification'])]
        if labels:
            res.append(Formats.as_bullet_point('Labels', ''))
            for metric_label in labels:
                label_description = labels[metric_label]
                res.append(f'\t{Formats.as_bullet_point(metric_label, label_description)}')  # append as nested list
        res.append('')
        return res

    def build(some_data):
        res = []
        for metric in some_data:
            # remove '\n', e.g., in descriptions
            metric_representation = [a_reprs for a_reprs in process_metric(metric)]
            res.extend(metric_representation)
        return '\n'.join(res)

    def sanitize_data(some_data: list):
        # Subset of keys to consider for deduplication (must have hashable values)
        hashable_keys = ['name', 'type']

        # Deduplication process
        seen = set()
        unique_data = []
        for item in some_data:
            # Create a hashable representation using only the subset of keys
            hashable_subset = tuple((k, item[k]) for k in hashable_keys if k in item)
            if hashable_subset not in seen:
                seen.add(hashable_subset)
                unique_data.append(item)

        # Sort the unique data by the key 'name'
        sorted_unique_data = sorted(unique_data, key=lambda x: x['name'])
        return sorted_unique_data

    reprs = dict()
    reprs['sequencer'] = build(sanitize_data(data['sequencer']))
    reprs['mediator'] = build(sanitize_data(data['mediator']))
    reprs['participant'] = build(sanitize_data(data['participant']))
    return reprs


def generate_error_codes(a_generated_dir: Path, loaded_json, converter) -> None:
    # building a map [error_code -> error_information]
    error_codes_data = {error['code']: error for error in loaded_json['errorCodes']}
    errors = converter.process_error_code_data(error_codes_data)
    storage.write(a_generated_dir / ERROR_CODES_INCLUDE_FILE_NAME, errors)


def generate_console(a_generated_dir: Path, loaded_json, converter) -> None:
    console_data = loaded_json['console']
    console = converter.process_console_command_data(console_data)
    template = storage.read(Path(__file__).with_name('console.rst.template'))
    storage.write(a_generated_dir / CONSOLE_COMMANDS_INCLUDE_FILE_NAME, _json_processing(console, template))


def _json_processing(console_data, template: str) -> str:

    console_topic_marker = 'console-topic-marker'

    marker_and_data_pair = [(f'<{console_topic_marker}: {topic}>', console_data[topic]) for
                            topic in console_data.keys()]

    for (marker, new_data) in marker_and_data_pair:
        template = template.replace(marker, new_data)

    if console_topic_marker in template:
        ii = template.find(console_topic_marker)
        raise Exception(
            f'Failure, marker falsely not replaced in lines {template[ii:ii + 200]}')

    return template


if __name__ == '__main__':
    arguments = sys.argv[1:3]
    reference_data_json_file, target = arguments

    reference_data_json_file_path: Path = Path(reference_data_json_file)
    target_out_dir: Path = Path(target)

    assembly_mode = JsonToRstConverter()

    generate(assembly_mode, reference_data_json_file_path, target_out_dir)
