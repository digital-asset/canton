..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

###############################
gRPC Ledger API Migration Guide
###############################

***********************************************
Migrating from Version 3.2 to Version 3.3
***********************************************

Application ID to User ID rename
--------------------------------

Application ID was already synonymous with the User ID for all Canton deployements supporting authorization. This is pure rename without changing the semantics, except the slightly changed list of accepted characters for the User ID. (Please see both in value.proto)

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - GetMeteringReportRequest, PrepareSubmissionRequest, ExecuteSubmissionRequest, CompletionStreamRequest, Commands, Completion, ReassignmentCommand
      - Field ``application_id`` is consistently renamed to ``user_id``.

'Domain' is Renamed to 'Synchronizer'
-------------------------------------

A simple renaming without changing the semantics.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Changed endpoint
      - Migration instruction
   -

      - GetConnectedDomains
      - Endpoint renamed to ``GetConnectedSynchronizers``.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Changed message
      - Migration instruction
   -

      - DomainTime, GetConnectedDomainsRequest, GetConnectedDomainsResponse, ConnectedDomain, Transaction, Metadata, PrepareSubmissionRequest
      - Messages renamed: ``Domain*`` to ``Synchronizer*``.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - DisclosedContract, Commands, Created, Archived, ActiveContract, TransactionTree
      - Field ``domain_id`` is consistently renamed to ``synchronizer_id``.
   -

      - Completion
      - Field ``domain_time`` is renamed to ``synchronizer_time``.
   -

      - OffsetCheckpoint
      - Field ``domain_times`` renamed to ``synchronizer_times``.
   -

      - ConnectedSynchronizer
      - Fields renamed: ``domain`` to ``synchronizer``.

``event_id`` Field Changes to ``offset`` and ``node_id``
--------------------------------------------------------

Field ``event_id`` is converted into two fields: ``offset`` and ``node_id``. These two properties identify the events precisely.

The structural representation of daml transaction trees changed: instead of exposing the root event IDs and the children for exercised events, only the last descendant node ID is exposed.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Changed endpoint
      - Migration instruction
   -

      - GetTransactionTreeByEventId, GetTransactionByEventId
      - The ``*ByEventId`` pointwise-lookup endpoints changed, they are renamed to ``*ByOffset`` respectively. The lookups should be based on the newly introduced ``offset`` field, instead of the ``event_id`` from the respective ``Event``.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - CreatedEvent, ArchivedEvent, ExercisedEvent
      - Field ``event_id`` is removed in favor of new field ``offset`` (referencing the offset of origin) and new field ``node_id``. These two fields identify the event.
   -

      - ExercisedEvent
      - Field ``child_event_ids`` is removed. Instead the new field ``last_descendant_node_id`` can be used, which captures structural information of the transaction trees.
   -

      - TransactionTree
      - Field ``events_by_id`` changed its type: the key of the map was previously a ``string`` referring to ``event_id``\ ’s, now it is an ``int32`` referring to the ``node_id``\ ’s of the ``Event’``\ s.

        Field ``root_event_ids`` is removed. The root nodes can be now computed with the help of the ordered list of events and the ``last_descendant_id``. If Java-bindings are used there is a helper function ``getRootNodeIds()``.


Miscellaneous
-------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - Completion
      - Field ``deduplication_offset`` can have value zero, with meaning: participant begin.

Interactive submission
----------------------

The interactive submission related proto definitions moved to the interactive folder.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - ExecuteSubmissionRequest
      - The ``min_ledger_time`` field has been removed as it was unused. ``min_ledger_time`` can be set in the PrepareSubmissionRequest message instead.

Universal Streams
-----------------

Universal Streams is a new feature introduced in 3.3.  It has a broad set of changes that consolidate the flat and tree style formats together. The old API is now deprecated and is replaced by a new API in this release.   There are two sets of changes needed:

- Required changes to work with 3.3.
- Calls to the old API need to use the new API before the next major release where the old API is removed.

Both changes can be done in this release if that is most efficient.

Required Changes in 3.3
^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Changed endpoint
      - Migration instruction
   -

      - SubmitAndWaitForTransaction
      - Instead of ``SubmitAndWaitRequest`` a new message ``SubmitAndWaitForTransactionRequest`` needs to be used. Here you can specify the ``transaction_format`` field.

        If no ``transaction_format`` field is provided, a default will be used where:

        - ``transaction_shape`` set to ``ACS_DELTA``
        - ``event_format`` defined with:

            - ``filters_by_party`` containing wildcard-template filter for all original ``Commands.act_as`` and ``Commands.read_as`` parties
            - ``verbose`` flag set
   -

      - GetEventsByContractId
      - In case of no events found, previously a ``GetEventsByContractIdResponse`` is returned with empty ``created`` and empty ``archived`` fields, now a ``CONTRACT_NOT_FOUND`` error is raised.

        Additionally: if ``EventFormat`` would filter out all results, a ``CONTRACT_NOT_FOUND`` error would be raised as well.
   -

      - GetTransactionByOffset, GetTransactionById
      - The behavior of these endpoints changed: instead of returning an empty transaction, a ``TRANSACTION_NOT_FOUND`` error will be raised, if the transaction cannot be found, or filtering based on the query parameters would lead to an empty transaction.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - Event
      - The ``oneof event`` is extended with ``exercised`` to support universal streams. The values of the events in case of ``transaction_shape``:

        - ``LEDGER_EFFECTS`` can be ``created`` or ``exercised``
        - ``ACS_DELTA`` can be ``created`` or ``archived``
   -

      - GetUpdatesResponse
      - The update ``oneof`` is extended to accommodate new ``TopologyTransaction`` updates. These types of messages will be only populated if the experimental flag is turned on, and the respective field of the ``update_filter`` is set.


Changes Required before the Next Major Release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The changes below must be made prior to the next major release because the old APIs will be removed.  This includes any related Java language bindings.


A helpful note is included in the source code for those items that will be removed.  The protobuf definitions contain documentation markers for the places where an endpoint, a message or a field will be removed in 3.4. Look for the following formulation:

.. code-block:: scala

    // Provided for backwards compatibility, it will be removed in the Canton version 3.4.0.

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   -

      - Deprecated endpoint
      - Migrating to
      - Migration instruction
   -

      - SubmitAndWaitForTransactionTree
      - SubmitAndWaitForTransaction
      - To retain the original behavior, the transaction_format field of the SubmitAndWaitForTransactionRequest should be defined with:

        - ``transaction_shape`` set to ``LEDGER_EFFECTS``
        - ``event_format`` defined with:

            - ``filters_by_party`` containing wildcard-template filter for all original ``Commands.act_as`` parties
            - ``verbose`` flag set
   -

      - GetUpdateTrees
      - GetUpdates
      - To retain the original behavior, the ``update_format`` field of the GetUpdatesRequest should be used instead of the deprecated ``filter`` and ``verbose`` fields:

        - ``include_transactions`` should be defined with

            - ``transaction_shape`` set to ``LEDGER_EFFECTS``
            - ``event_format`` should be defined by the original ``filter`` and ``verbose`` fields
            - If you are interested in seeing full trees for all querying parties and rendered views/blobs for selected interfaces or templates: extend the ``filters_by_party`` map and ``filters_for_any_party`` with a `WildcardFilter`` in the ``cumulative`` field, where the ``include_created_event_blob`` is set to false.

        - ``include_reassignments`` should be defined by the original ``filter`` and ``verbose`` fields, and additionally for each ``Filters`` (in filters_by_party map and potentially in filters_for_any_party). The list of ``cumulative`` should be extended with a WildcardFilter where the include_created_event_blob is set to false. This step is needed because update streams are filtered by the cumulative filters, whereas for GetUpdateTrees for all querying parties all reassignments returned and the filter only influenced event rendering.
        - ``include_topology_events`` should be unset
   -

      - GetTransactionTreeByOffset
      - GetUpdateByOffset
      - To retain the original behavior, the include_transactions field of the GetUpdateByOffsetRequest should be defined with:

        - ``transaction_shape`` set to ``LEDGER_EFFECTS``
        - ``event_format`` defined with:

            - ``filters_by_party`` containing wildcard-template filter for all original requesting_parties parties
            - ``verbose`` flag set
   -

      - GetTransactionTreeById
      - GetUpdateById
      - To retain the original behavior, the include_transactions field of the GetUpdateByIdRequest should be defined with:

        - ``transaction_shape`` set to ``LEDGER_EFFECTS``
        - ``event_format`` defined with:

            - ``filters_by_party`` containing wildcard-template filter for all original requesting_parties parties
            - ``verbose`` flag set
   -

      - GetTransactionByOffset
      - GetUpdateByOffset
      - To retain the original behavior, the include_transactions field of the GetUpdateByOffsetRequest should be defined with:

        - ``transaction_shape`` set to ``ACS_DELTA``
        - ``event_format`` should be defined with:

            - ``filters_by_party`` should be wildcard filter for all original ``requesting_parties``
            - ``verbose`` should be set
   -

      - GetTransactionById
      - GetUpdateById
      - To retain the original behavior, the include_transactions field of the GetUpdateByIdRequest should be defined with:

        - ``transaction_shape`` set to ``ACS_DELTA``
        - ``event_format`` should be defined with:

            - ``filters_by_party`` should be wildcard filter for all original ``requesting_parties``
            - ``verbose`` should be set

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Deprecated message
      - Migration instruction
   -

      - SubmitAndWaitForTransactionTreeResponse
      - ``SubmitAndWaitForTransaction`` should be used with setting ``transaction_format`` with ``transaction_shape`` ``LEDGER_EFFECTS`` in the ``SubmitAndWaitForTransactionRequest``
   -

      - TreeEvent, TransactionTree, GetUpdateTreesResponse, GetTransactionTreeResponse
      - These were used in tree\* query/streaming scenarios, which is deprecated. The respective non tree\* endpoint should be used with setting transaction_shape to ``LEDGER_EFFECTS`` in the request, and therefore resulting ``Event``-s will be either ``CreatedEvent`` or ``ExercisedEvent``.
   -

      - TransactionFilter
      - ``EventFormat`` should be used, which contains the original fields from ``TransactionFilter`` plus the ``verbose`` flag.
   -

      - GetTransactionByOffsetRequest, GetTransactionByIdRequest
      - The GetUpdateByOffsetRequest and GetUpdateByIdRequest respectively should be used with setting the ``include_transactions`` of the ``update_format`` field.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with deprecated field(s)
      - Migration instruction
   -

      - GetEventsByContractIdRequest
      - The new ``event_format`` field should be used instead of the deprecated ``requesting_parties``.

        To retain the original behavior, event format field should be defined with:

        - ``filters_by_party`` should be wildcard filter for all ``requesting_parties``
        - ``verbose`` should be set
   -

      - GetActiveContractsRequest
      - The new ``event_format`` field should be used instead of the deprecated filter and verbose fields.

        To retain the original behavior, the ``event_format`` field should be defined with:

        - ``filters_by_party`` and ``filters_for_any_party`` should be the same as in the original ``filter`` field
        - ``verbose`` should be the same as the original ``verbose`` field
   -

      - GetUpdatesRequest
      - The new ``update_format`` field should be used instead of the deprecated ``filter`` and ``verbose`` fields.

        To retain the original behavior:

        - ``include_transactions`` should be defined with

            - ``transaction_shape`` set to ``ACS_DELTA``
            - ``event_format`` should be defined by the original ``filter`` and ``verbose`` fields

        - ``include_reassignments`` should be defined by the original ``filter`` and ``verbose`` fields
        - ``include_topology_events`` should be unset



Deprecated Java Bindings
^^^^^^^^^^^^^^^^^^^^^^^^

All java-binding classes that wrap the gRPC Ledger API messages slated for removal in 3.4, will be removed themselves. It will affect:

- GetTransactionByIdRequest
- GetTransactionByOffsetRequest
- GetTransactionResponse
- GetUpdateTreesResponse
- SubmitAndWaitForTransactionTreeResponse
- TransactionFilter
- TransactionTree
- TransactionTreeUtils
- TreeEvent


JSON Ledger API v1 is Deprecated by JSON Ledger API v2
------------------------------------------------------

There are two JSON Ledger API versions in this release:

- *JSON Ledger API v1*:  exists in Daml 3.2. This version is deprecated and will be removed in the next major release.
- *JSON Ledger API v2*:  is introduced in this release and will be the supported JSON Ledger API version going forward.

Please refer to :ref:`JSON Ledger API v2 migration guide <json-api_migration>` for the specifics to migrate from JSON Ledger API v1 to v2.  This must be done prior to the next major release.Œ

Identifier Addressing by-package-name
-------------------------------------

As described in ``value.proto``, the following interface and template identifiers formats are supported in Canton 3.3 for requests to the gRPC Ledger API:

- **package-name reference format** (``#<package-name>:<module>:<entity>``) is supported and is the recommended format to use.

- **package-id reference format** (``<package-id>:<module>:<entity>``) is still supported but deprecated. Support is discontinued in Canton 3.4.

Prior to the next major release, all clients using the *package-id reference format* must switch to using the package-name reference format for all requests.

*****************************************
Migrating from Version 3.3 to Version 3.4
*****************************************

Preferred package version endpoints
-----------------------------------

A new set of endpoints supersedes and extends the existing functionality provided by ``InteractiveSubmissionService.GetPreferredPackageVersion`` (gRPC) and ``interactive-submission/preferred-packages`` (JSON). The new endpoints are:

- ``InteractiveSubmissionService.GetPreferredPackages`` (gRPC)

- ``interactive-submission/preferred-packages`` (JSON)

Users must switch to using the new endpoints in Canton 3.4, as the old endpoints will be removed.

Existing users can switch to the new endpoints by adapting the request payload.
For example, in the gRPC version, construct the ``GetPreferredPackagesRequest`` by populating one ``PackageVettingRequirement``
with homonymous fields from the old ``GetPreferredPackageVersionRequest``.

For more details, refer to the protobuf definitions in ``interactive_submission_service.proto``.

Package-name based queries
--------------------------

For the ledger API queries using filters, the interface or template identifiers in the event format can be specified
using either the package-name or the package-id format (via the ``package_id`` field in the ``Identifier`` message of the
``InterfaceFilter`` or the ``TemplateFilter`` of ``CumulativeFilter``).

The package-id format is deprecated and will not be supported in future releases. If the package-id format is used, a
warning message will be logged and the package-id will be internally translated to the corresponding package-name
format.

The affected endpoints are:

- ``StateService.GetActiveContracts``
- ``UpdateService.GetUpdates``
- ``UpdateService.GetUpdateTrees``
- ``UpdateService.GetTransactionTreeByOffset``
- ``UpdateService.GetTransactionTreeById``
- ``UpdateService.GetTransactionByOffset``
- ``UpdateService.GetTransactionById``
- ``UpdateService.GetUpdateByOffset``
- ``UpdateService.GetUpdateById``

Transient events in ACSDelta
----------------------------

Transactions with transient events for which there was an intersection between submitters and querying parties were
previously exposed as transactions with empty events in AcsDelta shape. In 3.4, those transactions will not be exposed at
all in the AcsDelta shape. As before, they will still be exposed in the LedgerEffects shape.

Participant divulgence
----------------------

Participant divulgence emerges if there is a party divulgence, and none of the stakeholders of the divulged contracts
are hosted on the participant. From Canton 3.4, participant divulgence is no longer performed in the Active Contracts
service and the AcsDelta shaped transaction endpoints.
