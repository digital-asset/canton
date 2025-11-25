..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

Canton documentation
====================

.. toctree::
  :maxdepth: 2
  :caption: Overview

  overview/overview/canton
  overview/explanations/index
  overview/explanations/canton/overview
  overview/explanations/canton/protocol
  overview/explanations/canton/synchronizers
  overview/explanations/canton/requirements
  overview/explanations/canton/topology
  overview/explanations/canton/participant
  overview/explanations/canton/decentralization
  overview/explanations/canton/external-party
  overview/explanations/canton/multi-synchronizer
  overview/explanations/canton/reliability
  overview/explanations/canton/pruning
  overview/explanations/canton/security
  overview/explanations/canton/traffic-management
  overview/explanations/comparison-blockchain
  overview/reference/index
  overview/reference/support/support_checklist
  overview/reference/research

.. toctree::
  :maxdepth: 2
  :caption: SDK

  sdk/tutorials/app-dev/external_signing_overview
  sdk/tutorials/app-dev/external_signing_onboarding_lapi
  sdk/tutorials/app-dev/external_signing_onboarding_multihosted
  sdk/tutorials/app-dev/external_signing_onboarding
  sdk/tutorials/app-dev/external_signing_topology_transaction
  sdk/tutorials/app-dev/external_signing_submission
  sdk/tutorials/app-dev/external_signing_submission_part_2
  sdk/tutorials/json-api/canton_and_the_json_ledger_api
  sdk/tutorials/json-api/canton_and_the_json_ledger_api_ts
  sdk/tutorials/json-api/canton_and_the_json_ledger_api_ts_websocket

  sdk/sdlc-howtos/applications/develop/manage-daml-packages
  sdk/sdlc-howtos/applications/develop/manage-daml-parties
  sdk/explanations/external-signing/external_signing_overview
  sdk/explanations/external-signing/external_signing_hashing_algorithm
  sdk/explanations/json-api/index
  sdk/explanations/json-api/migration_v2
  sdk/explanations/json-api/production-setup
  sdk/explanations/parties-users
  sdk/reference/console-commands-migration-guide
  sdk/reference/lapi-proto-docs
  sdk/reference/lapi-migration-guide
  sdk/reference/json-api/json-api
  sdk/reference/json-api/openapi
  sdk/reference/json-api/asyncapi
  sdk/reference/json-api/lf-value-specification

.. toctree::
  :maxdepth: 2
  :caption: Participant

  participant/index
  participant/overview/index

  participant/howtos/index

  participant/howtos/download/index
  participant/howtos/download/release
  participant/howtos/download/docker

  participant/howtos/install/index
  participant/howtos/install/release
  participant/howtos/install/docker

  participant/howtos/configure/index
  participant/howtos/configure/general/command_line
  participant/howtos/configure/general/conf_file
  participant/howtos/configure/general/declarative_conf
  participant/howtos/configure/identity/identity
  participant/howtos/configure/storage/storage
  participant/howtos/configure/storage/postgres
  participant/howtos/configure/apis/apis
  participant/howtos/configure/apis/admin_api
  participant/howtos/configure/apis/ledger_api
  participant/howtos/configure/apis/json_api
  participant/howtos/configure/parameters/parameters

  participant/howtos/secure/index

  participant/howtos/secure/apis/tls
  participant/howtos/secure/apis/jwt
  participant/howtos/secure/apis/limits
  participant/howtos/secure/limits/resource_limits
  participant/howtos/secure/keys/key_management
  participant/howtos/secure/keys/key_restrictions
  participant/howtos/secure/keys/namespace_key

  participant/howtos/secure/kms/kms
  participant/howtos/secure/kms/configuration/kms_configuration
  participant/howtos/secure/kms/configuration/kms_aws_config
  participant/howtos/secure/kms/configuration/kms_gcp_config
  participant/howtos/secure/kms/configuration/kms_driver_config
  participant/howtos/secure/kms/mode/kms_mode
  participant/howtos/secure/kms/mode/encrypted_key_storage
  participant/howtos/secure/kms/mode/external_key_storage
  participant/howtos/secure/kms/migration/kms_migration
  participant/howtos/secure/kms/migration/encrypted_key_storage_migration
  participant/howtos/secure/kms/migration/external_key_storage_migration
  participant/howtos/secure/kms/key_rotation/kms_key_rotation
  participant/howtos/secure/kms/key_rotation/rotate_wrapper_key
  participant/howtos/secure/kms/key_rotation/rotate_external_keys

  participant/howtos/optimize/index
  participant/howtos/optimize/performance
  participant/howtos/optimize/storage
  participant/howtos/optimize/caching
  participant/howtos/optimize/batching
  participant/howtos/optimize/session_keys

  participant/howtos/observe/index
  participant/howtos/observe/health
  participant/howtos/observe/logging
  participant/howtos/observe/metrics
  participant/howtos/observe/tracing
  participant/howtos/observe/datadog
  participant/howtos/observe/commitments
  participant/howtos/observe/monitoring

  participant/howtos/operate/index
  participant/howtos/operate/identity/manual_identity_init
  participant/howtos/operate/console/console
  participant/howtos/operate/synchronizers/connectivity
  participant/howtos/operate/ha/ha
  participant/howtos/operate/packages/packages
  participant/howtos/operate/pruning/pruning
  participant/howtos/operate/parties/parties
  participant/howtos/operate/parties/party_replication
  participant/howtos/operate/parties/decentralized_parties
  participant/howtos/operate/parties/external_parties
  participant/howtos/operate/users/users
  participant/howtos/operate/traffic/node_traffic
  participant/howtos/operate/identity_management

  participant/howtos/upgrade/index

  participant/howtos/decommission/index

  participant/howtos/recover/index
  participant/howtos/recover/repairing
  participant/howtos/recover/backup-restore

  participant/howtos/troubleshoot/index
  participant/howtos/troubleshoot/troubleshooting_guide
  participant/howtos/troubleshoot/FAQ
  participant/howtos/troubleshoot/commitments
  participant/howtos/troubleshoot/tls

  participant/explanations/index
  participant/explanations/participant-architecture
  participant/explanations/overview-ha
  participant/explanations/participant-ha
  participant/explanations/security
  participant/explanations/repairing

  participant/tutorials/index
  participant/tutorials/install
  participant/tutorials/getting_started
  participant/tutorials/monitoring/example_monitoring_setup
  participant/tutorials/kms_driver_guide

  participant/reference/index
  participant/reference/console
  participant/reference/error_codes
  participant/reference/metrics
  participant/reference/versioning
  participant/reference/crypto_schemes
  participant/reference/automatic_pruning

.. toctree::
  :maxdepth: 2
  :caption: Synchronizer

  synchronizer/index

  synchronizer/overview/index

  synchronizer/howtos/index

  synchronizer/howtos/download/index
  synchronizer/howtos/download/docker
  synchronizer/howtos/install/index

  synchronizer/howtos/configure/index
  synchronizer/howtos/configure/apis
  synchronizer/howtos/configure/storage
  synchronizer/howtos/configure/sequencer_backend

  synchronizer/howtos/secure/index
  synchronizer/howtos/secure/apis
  synchronizer/howtos/secure/limits
  synchronizer/howtos/secure/key_management

  synchronizer/howtos/optimize/index
  synchronizer/howtos/optimize/sequencer
  synchronizer/howtos/optimize/orderer
  synchronizer/howtos/optimize/mediator

  synchronizer/howtos/observe/index
  synchronizer/howtos/observe/observe
  synchronizer/howtos/observe/sequencer_health
  synchronizer/howtos/observe/mediator_health
  synchronizer/howtos/observe/mediator_inspection

  synchronizer/howtos/operate/index
  synchronizer/howtos/operate/bootstrap
  synchronizer/howtos/operate/dynamic_params
  synchronizer/howtos/operate/connectivity
  synchronizer/howtos/operate/ha
  synchronizer/howtos/operate/traffic
  synchronizer/howtos/operate/new_nodes
  synchronizer/howtos/operate/pruning

  synchronizer/howtos/upgrade/index
  synchronizer/howtos/upgrade/upgrade
  synchronizer/howtos/upgrade/logical_sync_migration

  synchronizer/howtos/decommission/index

  synchronizer/howtos/recover/index

  synchronizer/howtos/troubleshoot/index

  synchronizer/explanations/index
  synchronizer/explanations/synchronizer-ha
  synchronizer/explanations/logical_sync_migration
  synchronizer/explanations/bft_orderer_arch

  synchronizer/reference/index

  synchronizer/tutorials/index
