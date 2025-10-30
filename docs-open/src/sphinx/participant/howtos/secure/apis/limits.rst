..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Cover gRPC max message size for both Admin API and Ledger API.
    Cover rate limiting on the Ledger API.
    Discuss load balancer in front of APIs as best practice for additional protection.
    Discuss the possible performance impact if rate limits are too strict.
    Link to howto for resource limits on the participant.
    Link to "build" site for daml related size limits.

.. _limits-api-configuration:

Configure API Limits
====================

.. _max-inbound-message-size-configuration:

Max Inbound Message Size
------------------------

The APIs exposed by both the participant (gRPC Ledger API and Admin API) as well as
by the synchronizer (Public API and Admin API) have an upper limit on incoming message
size. To increase this limit to accommodate larger payloads, the flag
``max-inbound-message-size`` has to be set for the respective API to the maximum
message size in **bytes**.

For example, to configure a participant's gRPC Ledger API limit to 20MB:

.. literalinclude:: CANTON/community/integration-testing/src/main/resources/include/participant2.conf
   :start-after: architecture-handbook-entry-begin: MaxInboundMessageSizeSetting
   :end-before: architecture-handbook-entry-end: MaxInboundMessageSizeSetting

