..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

################################
Console commands migration guide
################################

*****************************************
Migrating from version 3.3 to version 3.4
*****************************************

Deprecated Canton console commands
----------------------------------

Universal stream changes have deprecated Canton console commands related to TransactionTrees.

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   -

      - Deprecated command
      - Migrating to
      - Migration instruction
   -

      - ledger_api.updates.trees
      - ledger_api.updates.transactions or ledger_api.updates.updates
      - To retain the original behavior,

        - if interested only in transactions, ledger_api.updates.transactions should be used with ``transactionShape`` set to ``LEDGER_EFFECTS``.
        - if interested in both transactions and reassignments ledger_api.updates.updates should be used where the ``updateFormat`` should be defined with:

          - ``include_transactions`` should be defined with:

            - ``transaction_shape`` set to ``LEDGER_EFFECTS``
            - ``event_format`` should be defined with:

              - ``filters_by_party`` containing wildcard-template filter for all original parties
              - ``verbose`` flag set as original verbose argument
          - ``include_reassignments`` should be defined with the event format described above
   -

      - ledger_api.updates.flat
      - ledger_api.updates.transactions or ledger_api.updates.updates
      - To retain the original behavior,

        - if interested only in transactions, ledger_api.updates.transactions should be used with ``transactionShape`` set to ``ACS_DELTA``.
        - if interested in both transactions and reassignments ledger_api.updates.updates should be used where the ``updateFormat`` should be defined with:

          - ``include_transactions`` should be defined with:

            - ``transaction_shape`` set to ``ACS_DELTA``
            - ``event_format`` should be defined with:

              - ``filters_by_party`` containing wildcard-template filter for all original parties
              - ``verbose`` flag set as original verbose argument
          - ``include_reassignments`` should be defined with the event format described above
   -

      - ledger_api.updates.trees_with_tx_filter
      - ledger_api.updates.transactions_with_tx_format or ledger_api.updates.updates
      - To retain the original behavior:

        - if interested only in transactions, ledger_api.updates.transactions_with_tx_format should be used with ``transactionFormat`` defined with:

          - ``transaction_shape`` set to ``LEDGER_EFFECTS``
          - ``event_format`` should be defined with:

            - ``filters_by_party`` and ``filters_for_any_party`` should be the same as in the original ``filter`` field
            - ``verbose`` should be the same as the original ``verbose`` field

        - if interested in both transactions and reassignments ledger_api.updates.updates should be used where the ``updateFormat`` should be defined with:

          - ``include_transactions`` as the ``transactionFormat`` described above
          - ``include_reassignments`` should be defined with the event format same as in ``include_transactions``
   -

      - ledger_api.updates.flat_with_tx_filter
      - ledger_api.updates.transactions_with_tx_format or ledger_api.updates.updates
      - To retain the original behavior:

        - if interested only in transactions, ledger_api.updates.transactions_with_tx_format should be used with ``transactionFormat`` defined with:

          - ``transaction_shape`` set to ``ACS_DELTA``
          - ``event_format`` should be defined with:

            - ``filters_by_party`` and ``filters_for_any_party`` should be the same as in the original ``filter`` field
            - ``verbose`` should be the same as the original ``verbose`` field

        - if interested in both transactions and reassignments ledger_api.updates.updates should be used where the ``updateFormat`` should be defined with:

          - ``include_transactions`` as the ``transactionFormat`` described above
          - ``include_reassignments`` should be defined with the event format same as in ``include_transactions``
   -

      - ledger_api.updates.{subscribe_flat, subscribe_trees}
      - ledger_api.updates.subscribe_updates
      - To retain the original behavior ledger_api.updates.subscribe_updates should be used with ``transactionFormat`` defined with:

        - ``transaction_shape`` set to ``ACS_DELTA`` for old flat or ``LEDGER_EFFECTS`` for old trees
        - ``event_format`` should be defined with:

          - ``filters_by_party`` and ``filters_for_any_party`` should be the same as in the original ``filter`` field
          - ``verbose`` should be the same as the original ``verbose`` field
   -

      - ledger_api.updates.{by_id, by_offset}
      - ledger_api.updates.{update_by_id, update_by_offset}
      - To retain the original behavior update_by_id and update_by_offset should be used with the include_transactions field:

        - ``transaction_shape`` set to ``ACS_DELTA`` for old flat or ``LEDGER_EFFECTS`` for old trees
        - ``event_format`` should be defined with:

          - ``filters_by_party`` and ``filters_for_any_party`` should be the same as in the original ``filter`` field
          - ``verbose`` should be set
   -

      - ledger_api.commands.submit_flat
      - ledger_api.commands.submit
      - To retain the original behavior of ledger_api.updates.submit_flat, the ledger_api.updates.submit command should be used with ``transactionShape`` set to ``ACS_DELTA``.
